import os, json, logging, base64
import requests
from datetime import datetime, timedelta, timezone
import azure.functions as func
from jinja2 import Environment, BaseLoader, select_autoescape
from azure.storage.blob import BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions
from azure.storage.blob._shared.base_client import parse_connection_str

# ==============================================================
# CONFIG
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
# STORAGE
# ==============================================================
CONN_STR = os.environ.get("AzureWebJobsStorage")
_bsc = BlobServiceClient.from_connection_string(CONN_STR)
ACCOUNT_NAME = ACCOUNT_KEY = None
try:
    parsed = parse_connection_str(CONN_STR)
    ACCOUNT_NAME = parsed.get("account_name")
    ACCOUNT_KEY = parsed.get("account_key")
except Exception as e:
    logging.error(f"parse_connection_str: {e}")

env_name = os.environ.get("STORAGE_ACCOUNT_NAME")
env_key = os.environ.get("STORAGE_ACCOUNT_KEY")
if env_name and env_key:
    ACCOUNT_NAME, ACCOUNT_KEY = env_name, env_key

# ==============================================================
# HELPERS
# ==============================================================
def _build_url(req, path, key=""):
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
    if not (ACCOUNT_NAME and ACCOUNT_KEY):
        raise RuntimeError("Missing storage credentials for SAS generation")
    _bsc.create_container(INCOMING_CONTAINER)
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
.left{vertical-align:top;width:30%;height:100vh;color:white;background-color:#FF462D;padding:14px}
.right{vertical-align:top;width:70%;padding:20px 24px}
.name{font-weight:700;font-size:20px;margin:10px 0 2px}
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
  <h3 class="white">LANGUAGES</h3><ul>{% for l in languages %}<li>{{ l.name }}{% if l.level %} — {{ l.level }}{% endif %}</li>{% endfor %}</ul>
  <div style="position:absolute;bottom:20px;left:14px;text-align:center;width:26%;"><img class="logo" src="{{ kyndryl_logo_url }}" alt="Kyndryl Logo"/></div>
</td>
<td class="right">
{% if summary %}<h3>ABOUT ME</h3><p style="font-style:italic">{{ summary }}</p><hr/>{% endif %}
{% if experiences %}<h3>PREVIOUS ROLES</h3><ul>{% for e in experiences %}<li><strong>{{ e.title }}</strong> — {{ e.company }}{% if e.start_date %} ({{ e.start_date }}{% if e.end_date %}–{{ e.end_date }}{% endif %}){% endif %}</li>{% endfor %}</ul>{% endif %}
{% if skills %}<h3>SKILLS</h3><ul>{% for s in skills %}<li>{{ s }}</li>{% endfor %}</ul>{% endif %}
{% if achievements %}<h3>ACHIEVEMENTS</h3><ul>{% for a in achievements %}<li>{{ a }}</li>{% endfor %}</ul>{% endif %}
{% if education %}<h3>EDUCATION</h3><ul>{% for ed in education %}<li>{{ ed.degree or ed.title }} — {{ ed.institution }}</li>{% endfor %}</ul>{% endif %}
{% if certifications %}<h3>CERTIFICATIONS</h3><ul>{% for c in certifications %}<li>{{ c.name or c }}</li>{% endfor %}</ul>{% endif %}
</td></tr></table>
</body></html>
"""

# ==============================================================
# HTML RENDERER
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
            "full_name": pi.get("full_name"), "title": pi.get("headline"),
            "city": pi.get("city"), "country": pi.get("country")
        },
        "skills": skills,
        "languages": cv.get("languages") or [],
        "summary": cv.get("summary") or pi.get("summary"),
        "experiences": cv.get("work_experience") or [],
        "education": cv.get("education") or [],
        "achievements": cv.get("achievements") or [],
        "certifications": cv.get("certifications") or [],
        "photo_b64": pi.get("photo_base64") or "",
        "kyndryl_logo_url": "https://upload.wikimedia.org/wikipedia/commons/7/73/Kyndryl_logo.svg"
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
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json")

    try:
        if body.get("mode") == "normalize_only":
            pptx_b64 = body.get("pptx_base64")
            pptx_name = body.get("pptx_name") or "resume.pptx"
            pptx_bytes = base64.b64decode(pptx_b64)
            blob_name = f"{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{pptx_name}"
            sas_url = _upload_and_sas(pptx_bytes, blob_name)
            extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
            s, data, raw = _post_json(extract_url, {"ppt_blob_sas": sas_url, "pptx_name": pptx_name})
            if s != 200 or not isinstance(data, dict):
                raise RuntimeError(f"pptxextract failed ({s}): {raw}")
            raw_cv = data.get("raw") or data.get("raw3") or data
            normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
            s2, norm, raw2 = _post_json(normalize_url, {"raw": raw_cv, "pptx_name": pptx_name})
            if s2 != 200 or not isinstance(norm, dict):
                raise RuntimeError(f"cvnormalize failed ({s2}): {raw2}")
            normalized = norm.get("cv") or norm.get("normalized") or norm
            return func.HttpResponse(json.dumps({"cv": normalized}), status_code=200, mimetype="application/json")

        if "cv" in body:
            cv = body["cv"]
            out_name = body.get("file_name") or "cv.pdf"
            template = body.get("template") or "europass"
            html = _html_from_cv(cv, template)
            render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
            payload = {"out_name": out_name, "html": html, "css": ""}
            s3, rjson, rraw = _post_json(render_url, payload)
            if s3 != 200 or not isinstance(rjson, dict):
                raise RuntimeError(f"renderpdf_html failed ({s3}): {rraw}")
            return func.HttpResponse(json.dumps(rjson), status_code=200, mimetype="application/json")

        return func.HttpResponse(json.dumps({"error": "Unsupported request"}), status_code=400, mimetype="application/json")

    except Exception as e:
        logging.exception("cvagent error")
        return func.HttpResponse(json.dumps({"error": f"cvagent failed: {str(e)}"}), status_code=500, mimetype="application/json")
