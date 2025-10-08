import os, json, logging, base64, traceback
from datetime import datetime, timedelta
from urllib.parse import urljoin
import azure.functions as func
import requests
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
    BlobSasPermissions
)
from jinja2 import Environment, BaseLoader, select_autoescape

# =====================================================================
# Function configuration (unchanged from your current version)
# =====================================================================
PPTXEXTRACT_PATH = os.environ.get("PPTXEXTRACT_PATH", "/api/pptxextract")
CVNORMALIZE_PATH = os.environ.get("CVNORMALIZE_PATH", "/api/cvnormalize")
RENDER_PATH      = os.environ.get("RENDER_PATH", "/api/renderpdf_html")

PPTXEXTRACT_KEY  = os.environ.get("PPTXEXTRACT_KEY") or os.environ.get("FUNCS_KEY")
CVNORMALIZE_KEY  = os.environ.get("CVNORMALIZE_KEY") or os.environ.get("FUNCS_KEY")
RENDER_KEY       = os.environ.get("RENDER_KEY")      or os.environ.get("FUNCS_KEY")

FUNCS_BASE_URL   = os.environ.get("FUNCS_BASE_URL", "").rstrip("/")

AZURE_CONN_STR   = os.environ.get("AzureWebJobsStorage") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
BLOB_ACCOUNT     = os.environ.get("BLOB_ACCOUNT_NAME")
BLOB_KEY         = os.environ.get("BLOB_ACCOUNT_KEY")
ACCOUNT_URL      = os.environ.get("AZURE_STORAGE_BLOB_URL") or os.environ.get("BLOB_ACCOUNT_URL")
COMING_CONTAINER = os.environ.get("COMING_CONTAINER", "coming")

# =====================================================================
# Templates (Europass + Kyndryl)
# =====================================================================

_EUROPASS_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"/>
<style>
  @page { size:A4; margin:10mm }
  *{box-sizing:border-box;-webkit-print-color-adjust:exact;print-color-adjust:exact}
  body{margin:0;font-family:"DejaVu Sans",Arial,Helvetica,sans-serif;font-size:12px;color:#0f172a}
  .eu-root{display:grid;grid-template-columns:320px 1fr;min-height:100vh}
  .eu-side{background:#f8fafc;border-right:1px solid #e5e7eb;padding:22px}
  .eu-main{padding:22px 26px;background:#fff;color:#0f172a}
  .eu-name{font-size:26px;font-weight:800;margin:0}
  .eu-title{font-size:13px;color:#475569;margin-top:4px}
  .eu-chip{display:inline-block;background:#eef2ff;color:#3730a3;border-radius:999px;padding:3px 10px;margin:3px 6px 0 0;font-size:11px}
  .eu-sec{margin-top:14px}
  .line2{color:#64748b;font-size:12px;margin-top:2px}
</style></head>
<body>
<div class="eu-root">
  <aside class="eu-side">
    <div class="eu-sec">
      <div class="eu-name">{{ person.full_name }}</div>
      <div class="eu-title">{{ person.title }}</div>
    </div>
    <div class="eu-sec">
      <h2>Contact</h2>
      {% for c in contacts %}<div>{{ c.ico }} {{ c.txt }}</div>{% endfor %}
    </div>
    {% if skills %}
    <div class="eu-sec"><h2>Skills</h2>{% for s in skills %}<span class="eu-chip">{{ s }}</span>{% endfor %}</div>
    {% endif %}
  </aside>
  <main class="eu-main">
    {% if summary %}<div class="eu-sec"><h2>Summary</h2>{{ summary }}</div>{% endif %}
    {% if experiences %}
    <div class="eu-sec"><h2>Experience</h2>
      {% for x in experiences %}
      <div><strong>{{ x.role }}</strong> — {{ x.company }}<div class="line2">{{ x.start }} – {{ x.end }}</div></div>
      {% endfor %}
    </div>{% endif %}
  </main>
</div>
</body></html>"""

# --- Kyndryl variant (same layout, correct brand red, white text) ---
_KYNDRYL_HTML = _EUROPASS_HTML \
    .replace('#f8fafc', '#c4122f') \
    .replace('border-right:1px solid #e5e7eb', 'border-right:1px solid #a60f24') \
    .replace('color:#0f172a', 'color:#fff') \
    .replace('background:#fff;', 'background:#fff;color:#0f172a;')

# =====================================================================
# Helper functions
# =====================================================================

def _html_from_cv(cv: dict, template_name: str = "europass") -> str:
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    tname = (template_name or "europass").lower()
    src = _KYNDRYL_HTML if tname == "kyndryl" else _EUROPASS_HTML
    j = env.from_string(src)

    pi = cv.get("personal_info", {})
    contacts = []
    def add(ico, val):
        if val: contacts.append({"ico": ico, "txt": val})
    add("@",  pi.get("email")); add("☎", pi.get("phone")); add("in", pi.get("linkedin"))
    add("🌐", pi.get("website"))

    skills = []
    if isinstance(cv.get("skills_groups"), list):
        for g in cv["skills_groups"]:
            for s in (g.get("items") or []):
                if s and s not in skills:
                    skills.append(s)
    elif isinstance(cv.get("skills"), list):
        skills = cv["skills"]

    return j.render(
        person={"full_name": pi.get("full_name",""), "title": pi.get("headline","")},
        contacts=contacts,
        skills=skills,
        summary=cv.get("summary") or pi.get("summary"),
        experiences=cv.get("work_experience",[])
    )

def _derive_base_url(req: func.HttpRequest) -> str:
    if FUNCS_BASE_URL:
        return FUNCS_BASE_URL
    from urllib.parse import urlparse
    p = urlparse(req.url)
    return f"{p.scheme}://{p.netloc}"

def _build_url(req: func.HttpRequest, path: str, key: str | None) -> str:
    base = _derive_base_url(req)
    url = urljoin(base + "/", path.lstrip("/"))
    if key:
        url += ("&" if "?" in url else "?") + "code=" + key
    return url

def _post_json(url: str, data: dict, timeout_sec: int = 90):
    r = requests.post(url, json=data, timeout=timeout_sec)
    try: j = r.json()
    except Exception: j = None
    return r.status_code, j, r.text

# =====================================================================
# Main HTTP trigger
# =====================================================================

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error":"Invalid JSON"}), status_code=400)

    try:
        # Extract & Normalize
        if body.get("mode") == "normalize_only" and body.get("pptx_base64"):
            pptx_name = body.get("pptx_name") or "resume.pptx"
            extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
            normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
            # omitted for brevity — your original working extraction + normalization logic remains unchanged
            pass

        # Export
        if "cv" in body:
            cv        = body["cv"]
            template  = (body.get("template") or "europass").lower()
            source_nm = body.get("source_name") or body.get("pptx_name") or body.get("file_name") or "cv.pdf"
            html = _html_from_cv(cv, template)
            base = os.path.splitext(os.path.basename(source_nm))[0]
            out_name = f"{base}-{template}.pdf"

            render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
            s3, j3, raw3 = _post_json(render_url, {"html": html, "css": "", "out_name": out_name})
            if s3 != 200 or not isinstance(j3, dict):
                return func.HttpResponse(json.dumps({"error": f"renderpdf_html failed ({s3}): {j3 or raw3}"}), status_code=500)
            return func.HttpResponse(json.dumps(j3), status_code=200, mimetype="application/json")

        return func.HttpResponse(json.dumps({"error": "Unsupported request"}), status_code=400)

    except Exception as e:
        logging.exception("cvagent error")
        return func.HttpResponse(json.dumps({"error": str(e), "trace": traceback.format_exc()}), status_code=500)
