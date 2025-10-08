import os, json, logging, base64, io
import requests
from datetime import datetime, timedelta, timezone
import azure.functions as func
from jinja2 import Environment, BaseLoader, select_autoescape

# Azure Blob SDK (only used if we need SAS fallback)
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
    BlobSasPermissions
)
from azure.storage.blob._shared.base_client import parse_connection_str


# ==============================================================
# CONFIGURATION
# ==============================================================
BASE_URL = (os.environ.get("DOWNSTREAM_BASE_URL")
            or os.environ.get("FUNCS_BASE_URL") or "").rstrip("/")

# Your downstream routes (HTTP-trigger Functions)
PPTXEXTRACT_PATH = os.environ.get("PPTXEXTRACT_PATH", "/api/pptxextract")
CVNORMALIZE_PATH = os.environ.get("CVNORMALIZE_PATH", "/api/cvnormalize")
RENDER_PATH      = os.environ.get("RENDER_PATH",      "/api/renderpdf_html")

# Optional keys for those functions (if authLevel=function)
PPTXEXTRACT_KEY = os.environ.get("PPTXEXTRACT_KEY", "")
CVNORMALIZE_KEY = os.environ.get("CVNORMALIZE_KEY", "")
RENDER_KEY      = os.environ.get("RENDER_KEY", "")

HTTP_TIMEOUT_SEC   = int(os.environ.get("HTTP_TIMEOUT_SEC", "180"))
INCOMING_CONTAINER = os.environ.get("INCOMING_CONTAINER", "incoming")
SAS_MINUTES        = int(os.environ.get("SAS_MINUTES", "120"))

# Storage (for SAS fallback only)
CONN_STR = os.environ.get("AzureWebJobsStorage")
_bsc = BlobServiceClient.from_connection_string(CONN_STR) if CONN_STR else None
ACCOUNT_NAME = None
ACCOUNT_KEY  = None
try:
    if CONN_STR:
        parsed = parse_connection_str(CONN_STR)
        ACCOUNT_NAME = parsed.get("account_name")
        ACCOUNT_KEY  = parsed.get("account_key")
    # explicit override wins (Linux is case sensitive)
    env_name = os.environ.get("STORAGE_ACCOUNT_NAME")
    env_key  = os.environ.get("STORAGE_ACCOUNT_KEY")
    if env_name and env_key:
        ACCOUNT_NAME, ACCOUNT_KEY = env_name, env_key
    if ACCOUNT_NAME and ACCOUNT_KEY:
        logging.info(f"[cvagent] Storage account available: {ACCOUNT_NAME}")
    else:
        logging.warning("[cvagent] Storage account key not available — base64 path will be used; SAS fallback only if creds present.")
except Exception as e:
    logging.error(f"[cvagent] Could not parse AzureWebJobsStorage: {e}")


# ==============================================================
# SMALL UTILITIES
# ==============================================================
def _build_url(req: func.HttpRequest, path: str, key: str = "") -> str:
    """Build absolute URL to a downstream function, appending ?code=... if needed."""
    if path.startswith("http"):
        url = path
    elif BASE_URL:
        url = f"{BASE_URL}{path}"
    else:
        root = req.url.split("/api/")[0]
        url = f"{root}{path}"
    if key:
        url += ("&" if "?" in url else "?") + "code=" + key
    return url

def _post_json(url: str, payload: dict, timeout: int = HTTP_TIMEOUT_SEC):
    """POST JSON and return (status, json_or_none, raw_text)."""
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        raw = r.text
        try:
            j = r.json()
        except Exception:
            j = None
        return r.status_code, j, raw
    except Exception as e:
        return 0, None, f"Network error calling {url}: {e}"

def _ensure_container(name: str):
    if not _bsc: return
    try:
        _bsc.create_container(name)
    except Exception:
        pass

def _upload_and_sas(pptx_bytes: bytes, blob_name: str) -> str:
    """Upload PPTX and return a signed read SAS URL (requires ACCOUNT_KEY)."""
    if not (_bsc and ACCOUNT_NAME and ACCOUNT_KEY):
        raise RuntimeError("SAS credentials not available in this environment")
    _ensure_container(INCOMING_CONTAINER)
    bc = _bsc.get_blob_client(INCOMING_CONTAINER, blob_name)
    bc.upload_blob(
        pptx_bytes,
        overwrite=True,
        content_settings=ContentSettings(
            content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation"
        ),
    )
    account_url = _bsc.url.rstrip("/")
    blob_url = f"{account_url}/{INCOMING_CONTAINER}/{blob_name}"
    sas = generate_blob_sas(
        account_name=ACCOUNT_NAME,
        container_name=INCOMING_CONTAINER,
        blob_name=blob_name,
        account_key=ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=SAS_MINUTES),
    )
    return f"{blob_url}?{sas}"


# ==============================================================
# CLIENT-SIDE TEMPLATE → HTML (EUROPASS)
# ==============================================================
_EUROPASS_HTML = """<!doctype html>
<html><head><meta charset="utf-8"/>
<title>{{ person.full_name or 'Curriculum Vitae' }}</title>
<style>
  @page { size: A4; margin: 10mm }
  body{margin:0;font-family:"DejaVu Sans",Arial,Helvetica,sans-serif;font-size:12px;color:#0f172a}
  .eu-root{display:grid;grid-template-columns:320px 1fr;min-height:100vh}
  .eu-side{background:#f8fafc;border-right:1px solid #e5e7eb;padding:22px}
  .eu-main{padding:22px 26px}
  .eu-name{font-size:26px;font-weight:800;margin:0}
  .eu-title{font-size:13px;color:#475569;margin-top:4px}
  .eu-kv{display:grid;grid-template-columns:22px 1fr;gap:10px;margin:6px 0}
  .ico{width:22px;height:22px;border-radius:6px;background:#e2e8f0;display:flex;align-items:center;justify-content:center;font-size:12px}
  .eu-sec{margin-top:16px}
  .eu-sec h2{font-size:14px;font-weight:800;margin:0 0 10px;text-transform:uppercase;letter-spacing:.06em}
  .eu-chip{display:inline-block;background:#eef2ff;color:#3730a3;border:1px solid #e0e7ff;border-radius:999px;padding:3px 10px;margin:3px 6px 0 0;font-size:11px}
  .eu-job{margin:12px 0 10px}
  .line2{color:#64748b;font-size:12px;margin-top:2px}
  .desc{margin-top:6px}
  .eu-job ul{margin:6px 0 0 18px}
  .hr{height:1px;background:linear-gradient(90deg,#e5e7eb 60%,transparent 0) repeat-x;background-size:8px 1px;margin:14px 0}
</style></head>
<body>
<div class="eu-root">
  <aside class="eu-side">
    <h1 class="eu-name">{{ person.full_name or '' }}</h1>
    {% if person.title %}<div class="eu-title">{{ person.title }}</div>{% endif %}
    <div>{% for c in contacts %}<div class="eu-kv"><div class="ico">{{ c.ico }}</div><div>{{ c.txt }}</div></div>{% endfor %}</div>
    {% if skills %}<div class="eu-sec"><h2>Skills</h2><div>{% for s in skills %}<span class="eu-chip">{{ s }}</span>{% endfor %}</div></div>{% endif %}
    {% if languages %}<div class="eu-sec"><h2>Languages</h2><div>{% for l in languages %}<span class="eu-chip">{{ l.name }}{% if l.level %} — {{ l.level }}{% endif %}</span>{% endfor %}</div></div>{% endif %}
  </aside>
  <main class="eu-main">
    {% if summary %}<section class="eu-sec"><h2>About Me</h2><div>{{ summary }}</div></section><div class="hr"></div>{% endif %}
    {% if experiences %}
      <section class="eu-sec"><h2>Work Experience</h2>{% for e in experiences %}
        <div class="eu-job"><div class="line1"><strong>{{ e.title }}</strong> — {{ e.company }}</div>
        <div class="line2">{{ e.start_date }}{% if e.end_date %} – {{ e.end_date }}{% else %} – Present{% endif %}{% if e.location %} • {{ e.location }}{% endif %}</div>
        {% if e.description %}<div class="desc">{{ e.description }}</div>{% endif %}
        {% if e.bullets %}<ul>{% for b in e.bullets %}<li>{{ b }}</li>{% endfor %}</ul>{% endif %}</div>{% endfor %}</section>
    {% endif %}
    {% if education %}
      <section class="eu-sec"><h2>Education & Training</h2>{% for ed in education %}
        <div class="eu-edu"><div class="line1"><strong>{{ ed.degree or ed.title }}</strong> — {{ ed.institution }}</div>
        <div class="line2">{{ ed.start_date }}{% if ed.end_date %} – {{ ed.end_date }}{% endif %}{% if ed.location %} • {{ ed.location }}{% endif %}</div>
        {% if ed.details %}<div class="desc">{{ ed.details }}</div>{% endif %}</div>{% endfor %}</section>
    {% endif %}
  </main>
</div></body></html>
"""

def _html_from_cv(cv: dict, template_name: str = "europass") -> str:
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    j = env.from_string(_EUROPASS_HTML)
    pi = (cv.get("personal_info") or cv.get("personal") or {})
    contacts = []
    def add(icon, val):
        if val: contacts.append({"ico": icon, "txt": val})
    add("@", pi.get("email")); add("☎", pi.get("phone")); add("in", pi.get("linkedin"))
    add("🌐", pi.get("website"))
    addr = ", ".join(filter(None, [pi.get("address"), pi.get("city"), pi.get("country")]))
    add("📍", addr); add("🎂", pi.get("date_of_birth")); add("⚧", pi.get("gender")); add("🌎", pi.get("nationality"))
    skills = [s for g in (cv.get("skills_groups") or []) for s in (g.get("items") or [])]
    model = {
        "person": {"full_name": pi.get("full_name") or cv.get("name"), "title": pi.get("headline") or cv.get("title")},
        "contacts": contacts,
        "skills": skills,
        "languages": cv.get("languages") or [],
        "summary": cv.get("summary") or pi.get("summary"),
        "experiences": cv.get("work_experience") or cv.get("experience") or [],
        "education": cv.get("education") or [],
    }
    return j.render(**model)


# ==============================================================
# EXTRACT → NORMALIZE
#  - Try base64-first (your earlier working path)
#  - If extractor complains it wants SAS, fallback to Blob+SAS
# ==============================================================
def _extract_then_normalize(req: func.HttpRequest, pptx_b64: str, pptx_name: str):
    # 1) Try base64 extractor
    extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
    base64_payload = {"pptx_base64": pptx_b64, "pptx_name": pptx_name}
    s1, d1, raw1 = _post_json(extract_url, base64_payload)
    logging.info(f"[cvagent] extract(base64) → {s1}")

    # Success → go normalize
    if s1 == 200 and isinstance(d1, dict):
        raw_cv = d1.get("raw") or d1.get("raw3") or d1
        return _normalize(req, raw_cv, pptx_name)

    # If the extractor clearly wants SAS, try SAS fallback
    need_sas = (s1 in (400, 415)) and isinstance(d1, dict) and any(
        k in (d1.get("error") or d1.get("message") or "").lower() for k in ["sas", "ppt_blob_sas", "pptx_blob_sas"]
    )
    if need_sas:
        if not (ACCOUNT_NAME and ACCOUNT_KEY and _bsc):
            # Can't do SAS here; bubble the original error with a clearer message
            msg = (d1.get("error") or raw1) if isinstance(d1, dict) else raw1
            raise RuntimeError(f"Extractor requires SAS, but storage credentials are unavailable. Downstream said: {msg}")

        # Decode base64, upload, generate SAS
        try:
            pptx_bytes = base64.b64decode(pptx_b64)
        except Exception as e:
            raise RuntimeError(f"Invalid base64: {e}")
        ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        blob_name = f"{ts}-{pptx_name}"
        sas_url = _upload_and_sas(pptx_bytes, blob_name)

        # Try extractor again with SAS
        sas_payload = {"ppt_blob_sas": sas_url, "pptx_name": pptx_name}
        s2, d2, raw2 = _post_json(extract_url, sas_payload)
        logging.info(f"[cvagent] extract(SAS) → {s2}")
        if s2 != 200 or not isinstance(d2, dict):
            msg = (d2.get("error") if isinstance(d2, dict) else raw2)
            raise RuntimeError(f"pptxextract failed ({s2}): {msg}")

        raw_cv = d2.get("raw") or d2.get("raw3") or d2
        return _normalize(req, raw_cv, pptx_name)

    # Otherwise, pass through the original error
    msg = (d1.get("error") if isinstance(d1, dict) else raw1) or f"HTTP {s1}"
    raise RuntimeError(f"pptxextract failed ({s1}): {msg}")

def _normalize(req: func.HttpRequest, raw_cv: dict, pptx_name: str):
    if not isinstance(raw_cv, dict):
        raise RuntimeError("pptxextract returned no structured data")
    normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
    s, d, raw = _post_json(normalize_url, {"raw": raw_cv, "pptx_name": pptx_name})
    logging.info(f"[cvagent] normalize → {s}")
    if s != 200 or not isinstance(d, dict):
        msg = (d.get("error") if isinstance(d, dict) else raw)
        raise RuntimeError(f"cvnormalize failed ({s}): {msg}")
    return d.get("cv") or d.get("normalized") or d


# ==============================================================
# MAIN HTTP TRIGGER
# ==============================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("cvagent triggered")
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json")

    try:
        # --------- Extract + Normalize (unchanged from your UX) ---------
        if body.get("mode") == "normalize_only":
            pptx_b64 = body.get("pptx_base64")
            pptx_name = body.get("pptx_name") or "resume.pptx"
            if not pptx_b64:
                return func.HttpResponse(json.dumps({"error": "Missing pptx_base64"}), status_code=400, mimetype="application/json")
            cv = _extract_then_normalize(req, pptx_b64, pptx_name)
            return func.HttpResponse(json.dumps({"cv": cv}), status_code=200, mimetype="application/json")

        # --------- Export (build HTML → renderpdf_html) ---------
        if "cv" in body:
            cv = body["cv"]
            out_name = body.get("file_name") or body.get("out_name") or "cv.pdf"
            template = (body.get("template") or "europass").lower()

            html = _html_from_cv(cv, template)
            render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
            payload = {
                "out_name": out_name if out_name.lower().endswith(".pdf") else out_name + ".pdf",
                "html": html,
                "css": ""  # CSS inlined
            }
            s3, rjson, rraw = _post_json(render_url, payload)
            logging.info(f"[cvagent] render → {s3}")
            if s3 != 200 or not isinstance(rjson, dict):
                raise RuntimeError(f"renderpdf_html failed ({s3}): {rjson or rraw}")
            return func.HttpResponse(json.dumps(rjson), status_code=200, mimetype="application/json")

        return func.HttpResponse(json.dumps({"error": "Unsupported request"}), status_code=400, mimetype="application/json")

    except Exception as e:
        logging.exception("cvagent error")
        return func.HttpResponse(json.dumps({"error": f"cvagent failed: {str(e)}"}), status_code=500, mimetype="application/json")
