import os, json, logging, base64, requests
from datetime import datetime, timedelta, timezone
from azure.storage.blob import BlobServiceClient, ContentSettings, BlobSasPermissions, generate_blob_sas
from azure.storage.blob._shared.base_client import parse_connection_str
from jinja2 import Environment, BaseLoader, select_autoescape
import azure.functions as func

# ==============================================================
# CONFIGURATION
# ==============================================================

BASE_URL = (os.environ.get("DOWNSTREAM_BASE_URL") or os.environ.get("FUNCS_BASE_URL") or "").rstrip("/")
PPTXEXTRACT_PATH = os.environ.get("PPTXEXTRACT_PATH", "/api/pptxextract")
CVNORMALIZE_PATH = os.environ.get("CVNORMALIZE_PATH", "/api/cvnormalize")
RENDER_PATH = os.environ.get("RENDER_PATH", "/api/renderpdf_html")

PPTXEXTRACT_KEY = os.environ.get("PPTXEXTRACT_KEY", "")
CVNORMALIZE_KEY = os.environ.get("CVNORMALIZE_KEY", "")
RENDER_KEY = os.environ.get("RENDER_KEY", "")

HTTP_TIMEOUT_SEC = int(os.environ.get("HTTP_TIMEOUT_SEC", "180"))
INCOMING_CONTAINER = os.environ.get("INCOMING_CONTAINER", "incoming")
SAS_MINUTES = int(os.environ.get("SAS_MINUTES", "120"))

# ==============================================================
# STORAGE SETUP
# ==============================================================

CONN_STR = os.environ.get("AzureWebJobsStorage")
_bsc = BlobServiceClient.from_connection_string(CONN_STR)

ACCOUNT_NAME = ACCOUNT_KEY = None
try:
    parsed = parse_connection_str(CONN_STR)
    ACCOUNT_NAME = parsed.get("account_name")
    ACCOUNT_KEY = parsed.get("account_key")
except Exception as e:
    logging.error(f"parse_connection_str error: {e}")

# If environment overrides exist, prefer them
env_name, env_key = os.environ.get("STORAGE_ACCOUNT_NAME"), os.environ.get("STORAGE_ACCOUNT_KEY")
if env_name and env_key:
    ACCOUNT_NAME, ACCOUNT_KEY = env_name, env_key

# ==============================================================
# HELPER FUNCTIONS
# ==============================================================

def _build_url(req, path, key=""):
    """Resolve internal function URLs dynamically."""
    if path.startswith("http"):
        url = path
    elif BASE_URL:
        url = f"{BASE_URL}{path}"
    else:
        url = f"{req.url.split('/api/')[0]}{path}"
    if key:
        url += ("&" if "?" in url else "?") + "code=" + key
    return url

def _post_json(url, payload):
    r = requests.post(url, json=payload, timeout=HTTP_TIMEOUT_SEC)
    try:
        return r.status_code, r.json(), r.text
    except Exception:
        return r.status_code, None, r.text

def _upload_and_sas(pptx_bytes, blob_name):
    """Upload PPTX and generate temporary SAS URL."""
    if not (ACCOUNT_NAME and ACCOUNT_KEY):
        raise RuntimeError("Missing storage credentials")

    # ✅ Fixed version: safe container creation
    try:
        _bsc.create_container(INCOMING_CONTAINER, public_access=None, exist_ok=True)
    except Exception as e:
        logging.info(f"Container '{INCOMING_CONTAINER}' may already exist: {e}")

    bc = _bsc.get_blob_client(INCOMING_CONTAINER, blob_name)
    bc.upload_blob(
        pptx_bytes,
        overwrite=True,
        content_settings=ContentSettings(
            content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation"
        ),
    )

    sas = generate_blob_sas(
        account_name=ACCOUNT_NAME,
        container_name=INCOMING_CONTAINER,
        blob_name=blob_name,
        account_key=ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=SAS_MINUTES),
    )
    return f"{_bsc.url}/{INCOMING_CONTAINER}/{blob_name}?{sas}"

# ==============================================================
# HTML TEMPLATES
# ==============================================================

_EUROPASS_HTML = """<!doctype html>
<html><head><meta charset="utf-8"/><style>
body{font-family:'DejaVu Sans',Arial,Helvetica,sans-serif;font-size:12px;color:#0f172a;margin:0}
h1{font-size:20px;margin-bottom:4px}h2{font-size:14px;margin-top:16px}
ul{margin:0 0 10px 20px}
</style></head>
<body>
<h1>{{ person.full_name }}</h1>
<p><strong>{{ person.title }}</strong> — {{ person.city }}, {{ person.country }}</p>
<hr/>
{% if summary %}<h2>About Me</h2><p>{{ summary }}</p>{% endif %}
{% if skills %}<h2>Skills</h2><ul>{% for s in skills %}<li>{{ s }}</li>{% endfor %}</ul>{% endif %}
{% if experiences %}<h2>Experience</h2><ul>{% for e in experiences %}<li><strong>{{ e.title }}</strong> — {{ e.company }}</li>{% endfor %}</ul>{% endif %}
{% if education %}<h2>Education</h2><ul>{% for ed in education %}<li>{{ ed.degree or ed.title }} — {{ ed.institution }}</li>{% endfor %}</ul>{% endif %}
{% if languages %}<h2>Languages</h2><ul>{% for l in languages %}<li>{{ l.name }} — {{ l.level }}</li>{% endfor %}</ul>{% endif %}
</body></html>
"""

_KYNDRYL_HTML = """<!doctype html>
<html><head><meta charset="utf-8"/>
<title>{{ person.full_name or 'Curriculum Vitae' }}</title>
<style>
@page { size:A4; margin:0 }
body{margin:0;font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#111}
table{width:100%;border-spacing:0}
h3{color:#FF462D;margin:8px 0 4px;font-size:14px;text-transform:uppercase}
h3.white{color:white}
ul{margin:6px 0 10px 20px;list-style:none;padding:0}
li{margin-bottom:4px}
.left{vertical-align:top;width:30%;height:100vh;color:white;background-color:#FF462D;padding:14px;position:relative}
.right{vertical-align:top;width:70%;padding:20px 24px}
.subtitle{font-size:13px;margin-bottom:8px}
.logo{width:110px;margin-top:20px}
hr{border:0;border-top:1px solid #FF462D;margin:12px 0}
</style></head>
<body>
<table><tr>
<td class="left">
  <div style="text-align:center;margin-bottom:10px;">
    {% if photo_b64 %}<img src="data:image/png;base64,{{ photo_b64 }}" style="width:100px;border-radius:50%;"/>{% endif %}
  </div>
  <h3 class="white">{{ person.full_name or '' }}</h3>
  {% if person.title %}<div class="subtitle">{{ person.title }}</div>{% endif %}
  {% if person.city or person.country %}<div class="subtitle">{{ person.city }}{% if person.city and person.country %}, {% endif %}{{ person.country }}</div>{% endif %}
  <h3 class="white">LANGUAGES</h3>
  <ul>{% for l in languages %}<li>{{ l.name }}{% if l.level %} — {{ l.level }}{% endif %}</li>{% endfor %}</ul>
  <div style="position:absolute;bottom:20px;left:14px;text-align:center;width:26%;">
    <img class="logo" src="{{ kyndryl_logo_url }}" alt="Kyndryl Logo"/>
  </div>
</td>
<td class="right">
{% if summary %}<h3>ABOUT ME</h3><p style="font-style:italic">{{ summary }}</p><hr/>{% endif %}
{% if experiences %}<h3>PREVIOUS ROLES</h3><ul>{% for e in experiences %}<li><strong>{{ e.title }}</strong> — {{ e.company }}{% if e.start_date %} ({{ e.start_date }}{% if e.end_date %}–{{ e.end_date }}{% endif %}){% endif %}</li>{% endfor %}</ul>{% endif %}
{% if skills %}<h3>SKILLS</h3><ul>{% for s in skills %}<li>{{ s }}</li>{% endfor %}</ul>{% endif %}
{% if achievements %}<h3>ACHIEVEMENTS</h3><ul>{% for a in achievements %}<li>{{ a }}</li>{% endfor %}</ul>{% endif %}
{% if education %}<h3>EDUCATION</h3><ul>{% for ed in education %}<li>{{ ed.degree or ed.title }} — {{ ed.institution }}</li>{% endfor %}</ul>{% endif %}
{% if certifications %}<h3>CERTIFICATIONS</h3><ul>{% for c in certifications %}<li>{{ c.name or c }}</li>{% endfor %}</ul>{% endif %}
</td></tr></table></body></html>
"""

# ==============================================================
# TEMPLATE SELECTOR
# ==============================================================

def _html_from_cv(cv, template_name="europass"):
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    tname = (template_name or "europass").lower()
    tpl = _KYNDRYL_HTML if tname == "kyndryl" else _EUROPASS_HTML
    j = env.from_string(tpl)

    pi = (cv.get("personal_info") or {}) if isinstance(cv, dict) else {}
    skills = [s for g in (cv.get("skills_groups") or []) for s in (g.get("items") or [])]

    model = {
        "person": {
            "full_name": pi.get("full_name"),
            "title": pi.get("headline"),
            "city": pi.get("city"),
            "country": pi.get("country"),
        },
        "skills": skills,
        "languages": cv.get("languages") or [],
        "summary": cv.get("summary") or pi.get("summary"),
        "experiences": cv.get("work_experience") or [],
        "education": cv.get("education") or [],
        "achievements": cv.get("achievements") or [],
        "certifications": cv.get("certifications") or [],
        "photo_b64": pi.get("photo_base64") or "",
        "kyndryl_logo_url": "https://upload.wikimedia.org/wikipedia/commons/7/73/Kyndryl_logo.svg",
    }

    return j.render(**model)

# ==============================================================
# MAIN ENTRYPOINT
# ==============================================================

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("cvagent triggered")
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400)

    try:
        # --- Extract + Normalize ---
        if body.get("mode") == "normalize_only":
            pptx_b64 = body.get("pptx_base64")
            pptx_name = body.get("pptx_name") or "resume.pptx"
            if not pptx_b64:
                return func.HttpResponse(json.dumps({"error": "Missing pptx_base64"}), status_code=400)

            pptx_bytes = base64.b64decode(pptx_b64)
            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            blob_name = f"{ts}-{pptx_name}"
            sas_url = _upload_and_sas(pptx_bytes, blob_name)

            extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
            s, data, raw = _post_json(extract_url, {"ppt_blob_sas": sas_url, "pptx_name": pptx_name})
            if s != 200 or not data:
                msg = data.get("error") if isinstance(data, dict) else raw
                raise RuntimeError(f"pptxextract failed ({s}): {msg}")

            raw_cv = data.get("raw") or data.get("raw3") or data
            normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
            s2, norm, raw2 = _post_json(normalize_url, {"raw": raw_cv, "pptx_name": pptx_name})
            if s2 != 200 or not norm:
                msg = norm.get("error") if isinstance(norm, dict) else raw2
                raise RuntimeError(f"cvnormalize failed ({s2}): {msg}")

            normalized = norm.get("cv") or norm.get("normalized") or norm
            return func.HttpResponse(json.dumps({"cv": normalized}), status_code=200)

        # --- Render PDF ---
        if "cv" in body:
            cv = body["cv"]
            out_name = body.get("file_name") or "cv.pdf"
            template = (body.get("template") or "europass").lower()
            html = _html_from_cv(cv, template)
            render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
            payload = {"out_name": out_name, "html": html, "css": ""}
            s3, rjson, rraw = _post_json(render_url, payload)
            if s3 != 200 or not rjson:
                raise RuntimeError(f"renderpdf_html failed ({s3}): {rjson or rraw}")
            return func.HttpResponse(json.dumps(rjson), status_code=200)

        return func.HttpResponse(json.dumps({"error": "Unsupported request"}), status_code=400)

    except Exception as e:
        logging.exception("cvagent error")
        return func.HttpResponse(
            json.dumps({"error": f"cvagent failed: {str(e)}"}), status_code=500, mimetype="application/json"
        )
