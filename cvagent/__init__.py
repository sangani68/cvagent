import os, json, logging, base64
import requests
from datetime import datetime, timedelta, timezone
import azure.functions as func
from jinja2 import Environment, BaseLoader, select_autoescape

# Azure Blob
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
    BlobSasPermissions
)
from azure.storage.blob._shared.base_client import parse_connection_str

# ==============================================================
# CONFIG
# ==============================================================
BASE_URL = (os.environ.get("DOWNSTREAM_BASE_URL")
            or os.environ.get("FUNCS_BASE_URL") or "").rstrip("/")

PPTXEXTRACT_PATH = os.environ.get("PPTXEXTRACT_PATH", "/api/pptxextract")
CVNORMALIZE_PATH = os.environ.get("CVNORMALIZE_PATH", "/api/cvnormalize")
RENDER_PATH      = os.environ.get("RENDER_PATH",      "/api/renderpdf_html")

PPTXEXTRACT_KEY  = os.environ.get("PPTXEXTRACT_KEY", "")
CVNORMALIZE_KEY  = os.environ.get("CVNORMALIZE_KEY", "")
RENDER_KEY       = os.environ.get("RENDER_KEY", "")

HTTP_TIMEOUT_SEC   = int(os.environ.get("HTTP_TIMEOUT_SEC", "180"))
INCOMING_CONTAINER = os.environ.get("INCOMING_CONTAINER", "incoming")
SAS_MINUTES        = int(os.environ.get("SAS_MINUTES", "120"))

# ==============================================================
# STORAGE (derive creds from AzureWebJobsStorage; allow explicit override)
# ==============================================================
CONN_STR = os.environ.get("AzureWebJobsStorage")
if not CONN_STR:
    raise RuntimeError("AzureWebJobsStorage not set")

_bsc = BlobServiceClient.from_connection_string(CONN_STR)

ACCOUNT_NAME = None
ACCOUNT_KEY  = None
try:
    parsed = parse_connection_str(CONN_STR)
    ACCOUNT_NAME = parsed.get("account_name")
    ACCOUNT_KEY  = parsed.get("account_key")
except Exception as e:
    logging.error(f"[cvagent] parse_connection_str failed: {e}")

# explicit override wins (Linux env is case sensitive)
env_name = os.environ.get("STORAGE_ACCOUNT_NAME")
env_key  = os.environ.get("STORAGE_ACCOUNT_KEY")
if env_name and env_key:
    ACCOUNT_NAME, ACCOUNT_KEY = env_name, env_key

if ACCOUNT_NAME and ACCOUNT_KEY:
    logging.info(f"[cvagent] Using storage account: {ACCOUNT_NAME}")
else:
    logging.error("[cvagent] Storage account key not available — cannot generate SAS. "
                  "Set STORAGE_ACCOUNT_NAME and STORAGE_ACCOUNT_KEY or use a full connection string with AccountKey.")

# ==============================================================
# HELPERS
# ==============================================================
def _build_url(req: func.HttpRequest, path: str, key: str = "") -> str:
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
    try:
        _bsc.create_container(name)
    except Exception:
        pass

def _upload_and_sas(pptx_bytes: bytes, blob_name: str) -> str:
    if not (ACCOUNT_NAME and ACCOUNT_KEY):
        raise RuntimeError("Unable to derive storage credentials for SAS")
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
    signed = f"{blob_url}?{sas}"
    logging.info(f"[cvagent] SAS generated for {blob_name}")
    return signed

# ==============================================================
# TEMPLATE (EUROPASS → HTML)
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
    <div>
      {% for c in contacts %}
        <div class="eu-kv"><div class="ico">{{ c.ico }}</div><div>{{ c.txt }}</div></div>
      {% endfor %}
    </div>
    {% if skills %}
    <div class="eu-sec"><h2>Skills</h2><div>{% for s in skills %}<span class="eu-chip">{{ s }}</span>{% endfor %}</div></div>
    {% endif %}
    {% if languages %}
    <div class="eu-sec"><h2>Languages</h2><div>{% for l in languages %}<span class="eu-chip">{{ l.name }}{% if l.level %} — {{ l.level }}{% endif %}</span>{% endfor %}</div></div>
    {% endif %}
  </aside>
  <main class="eu-main">
    {% if summary %}
      <section class="eu-sec"><h2>About Me</h2><div>{{ summary }}</div></section><div class="hr"></div>
    {% endif %}
    {% if experiences %}
      <section class="eu-sec"><h2>Work Experience</h2>
        {% for e in experiences %}
          <div class="eu-job">
            <div class="line1"><strong>{{ e.title }}</strong> — {{ e.company }}</div>
            <div class="line2">{{ e.start_date }}{% if e.end_date %} – {{ e.end_date }}{% else %} – Present{% endif %}{% if e.location %} • {{ e.location }}{% endif %}</div>
            {% if e.description %}<div class="desc">{{ e.description }}</div>{% endif %}
            {% if e.bullets %}<ul>{% for b in e.bullets %}<li>{{ b }}</li>{% endfor %}</ul>{% endif %}
          </div>
        {% endfor %}
      </section>
    {% endif %}
    {% if education %}
      <section class="eu-sec"><h2>Education & Training</h2>
        {% for ed in education %}
          <div class="eu-edu">
            <div class="line1"><strong>{{ ed.degree or ed.title }}</strong> — {{ ed.institution }}</div>
            <div class="line2">{{ ed.start_date }}{% if ed.end_date %} – {{ ed.end_date }}{% endif %}{% if ed.location %} • {{ ed.location }}{% endif %}</div>
            {% if ed.details %}<div class="desc">{{ ed.details }}</div>{% endif %}
          </div>
        {% endfor %}
      </section>
    {% endif %}
  </main>
</div>
</body></html>
"""

# --- Kyndryl variant (same layout, correct brand red, white text) ---
_KYNDRYL_HTML = _EUROPASS_HTML \
    .replace('#f8fafc', '#c4122f') \
    .replace('border-right:1px solid #e5e7eb', 'border-right:1px solid #a60f24') \
    .replace('color:#0f172a', 'color:#fff') \
    .replace('background:#fff;', 'background:#fff;color:#0f172a;')


def _html_from_cv(cv: dict, template_name: str = "europass") -> str:
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    j = env.from_string(_KYNDRYL_HTML if (template_name or "europass").lower() == "kyndryl" else _EUROPASS_HTML)
    pi = (cv.get("personal_info") or cv.get("personal") or {}) if isinstance(cv, dict) else {}
    contacts = []
    def add(icon, val):
        if val: contacts.append({"ico": icon, "txt": val})
    add("@",  pi.get("email")); add("☎", pi.get("phone")); add("in", pi.get("linkedin"))
    add("🌐", pi.get("website"))
    addr = ", ".join([pi.get("address") or "", pi.get("city") or "", pi.get("country") or ""]).strip(", ")
    add("📍", addr); add("🎂", pi.get("date_of_birth")); add("⚧", pi.get("gender")); add("🌎", pi.get("nationality"))
    skills = [s for g in (cv.get("skills_groups") or []) for s in (g.get("items") or [])]
    model = {
        "person": {"full_name": pi.get("full_name") or cv.get("name"),
                   "title":     pi.get("headline")  or cv.get("title")},
        "contacts": contacts,
        "skills": skills,
        "languages": cv.get("languages") or [],
        "summary": cv.get("summary") or pi.get("summary"),
        "experiences": cv.get("work_experience") or cv.get("experience") or [],
        "education": cv.get("education") or [],
    }
    return j.render(**model)

# ==============================================================
# MAIN
# ==============================================================
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("cvagent triggered")
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json")

    try:
        # ---------- Extract + Normalize (ALWAYS SAS) ----------
        if body.get("mode") == "normalize_only":
            pptx_b64 = body.get("pptx_base64")
            pptx_name = body.get("pptx_name") or "resume.pptx"
            if not pptx_b64:
                return func.HttpResponse(json.dumps({"error": "Missing pptx_base64"}), status_code=400, mimetype="application/json")

            # Decode + upload + sign SAS
            try:
                pptx_bytes = base64.b64decode(pptx_b64)
            except Exception as e:
                return func.HttpResponse(json.dumps({"error": f"Invalid base64: {e}"}), status_code=400, mimetype="application/json")

            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            blob_name = f"{ts}-{pptx_name}"
            logging.info(f"[cvagent] Uploading {blob_name} ...")
            sas_url = _upload_and_sas(pptx_bytes, blob_name)
            logging.info("[cvagent] SAS ready")

            # Call extractor that requires ppt_blob_sas
            extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
            s, data, raw = _post_json(extract_url, {"ppt_blob_sas": sas_url, "pptx_name": pptx_name})
            logging.info(f"[cvagent] extract → {s}")
            if s != 200 or not isinstance(data, dict):
                msg = (data.get("error") if isinstance(data, dict) else raw)
                raise RuntimeError(f"pptxextract failed ({s}): {msg}")

            raw_cv = data.get("raw") or data.get("raw3") or data

            # Normalize
            normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
            s2, norm, raw2 = _post_json(normalize_url, {"raw": raw_cv, "pptx_name": pptx_name})
            logging.info(f"[cvagent] normalize → {s2}")
            if s2 != 200 or not isinstance(norm, dict):
                msg = (norm.get("error") if isinstance(norm, dict) else raw2)
                raise RuntimeError(f"cvnormalize failed ({s2}): {msg}")

            normalized = norm.get("cv") or norm.get("normalized") or norm
            return func.HttpResponse(json.dumps({"cv": normalized}), status_code=200, mimetype="application/json")

        # ---------- Export (HTML → renderpdf_html) ----------
        if "cv" in body:
            cv = body["cv"]
            out_name = body.get("file_name") or body.get("out_name") or "cv.pdf"
            template = (body.get("template") or "europass").lower()

            html = _html_from_cv(cv, template)
            render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
            payload = {
                "out_name": out_name if out_name.lower().endswith(".pdf") else out_name + ".pdf",
                "html": html,
                "css": ""  # inlined
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
