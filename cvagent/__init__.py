import os, json, logging, base64, traceback
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode, urljoin

import azure.functions as func
import requests

# Azure Blob
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
    BlobSasPermissions
)

# -----------------------------
# Config (env-first, safe defaults)
# -----------------------------
# Function endpoints (same app by default)
PPTXEXTRACT_PATH = os.environ.get("PPTXEXTRACT_PATH", "/api/pptxextract")
CVNORMALIZE_PATH = os.environ.get("CVNORMALIZE_PATH", "/api/cvnormalize")
RENDER_PATH      = os.environ.get("RENDER_PATH",      "/api/renderpdf_html")

# Per-function keys (or shared FUNCS_KEY)
PPTXEXTRACT_KEY  = os.environ.get("PPTXEXTRACT_KEY") or os.environ.get("FUNCS_KEY")
CVNORMALIZE_KEY  = os.environ.get("CVNORMALIZE_KEY") or os.environ.get("FUNCS_KEY")
RENDER_KEY       = os.environ.get("RENDER_KEY")      or os.environ.get("FUNCS_KEY")

# Optional explicit base url (otherwise derive from request)
FUNCS_BASE_URL   = os.environ.get("FUNCS_BASE_URL", "").rstrip("/")

# Storage for source PPTX upload (to create SAS for extractor)
ACCOUNT_URL      = os.environ.get("AZURE_STORAGE_BLOB_URL") or os.environ.get("BLOB_ACCOUNT_URL")
CONN_STR         = os.environ.get("AzureWebJobsStorage") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
COMING_CONTAINER = os.environ.get("COMING_CONTAINER", "coming")

# -----------------------------
# Minimal HTML templates (inline)
# - Kyndryl reuses Europass 2-col layout, with red sidebar and white text
# -----------------------------

_EUROPASS_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"/>
<title>{{ person.full_name or 'Curriculum Vitae' }}</title>
<style>
  @page { size: A4; margin: 10mm }
  *{box-sizing:border-box;-webkit-print-color-adjust:exact;print-color-adjust:exact}
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
  .eu-chip{display:inline-block;background:#eef2ff;color:#3730a3;border-radius:999px;padding:3px 10px;margin:3px 6px 0 0;font-size:11px}
  .eu-job{margin:12px 0 10px}
  .line2{color:#64748b;font-size:12px;margin-top:2px}
  .desc{margin-top:6px}
  .eu-job ul{margin:6px 0 0 18px}
  a{color:#1d4ed8;text-decoration:none}
</style>
</head>
<body>
  <div class="eu-root">
    <aside class="eu-side">
      <div class="eu-sec">
        <h2>Profile</h2>
        <div class="eu-name">{{ person.full_name }}</div>
        <div class="eu-title">{{ person.title }}</div>
      </div>

      <div class="eu-sec">
        <h2>Contact</h2>
        {% for c in contacts %}
          <div class="eu-kv"><div class="ico">{{ c.ico }}</div><div>{{ c.txt }}</div></div>
        {% endfor %}
      </div>

      {% if skills %}
      <div class="eu-sec">
        <h2>Core Skills</h2>
        {% for s in skills %}<span class="eu-chip">{{ s }}</span>{% endfor %}
      </div>
      {% endif %}

      {% if languages %}
      <div class="eu-sec">
        <h2>Languages</h2>
        {% for l in languages %}<div>{{ l.name }} — {{ l.level }}</div>{% endfor %}
      </div>
      {% endif %}

      {% if education %}
      <div class="eu-sec">
        <h2>Education</h2>
        {% for e in education %}
          <div><strong>{{ e.degree }}</strong> — {{ e.institution }}<div class="line2">{{ e.start }} – {{ e.end }}</div></div>
        {% endfor %}
      </div>
      {% endif %}
    </aside>

    <main class="eu-main">
      {% if summary %}
      <div class="eu-sec">
        <h2>Summary</h2>
        <div class="desc">{{ summary }}</div>
      </div>
      {% endif %}

      {% if experiences %}
      <div class="eu-sec">
        <h2>Experience</h2>
        {% for x in experiences %}
          <div class="eu-job">
            <div><strong>{{ x.role }}</strong></div>
            <div class="line2">{{ x.company }}{% if x.location %} • {{ x.location }}{% endif %} • {{ x.start }} – {{ x.end or "Present" }}</div>
            {% if x.highlights %}
              <ul>{% for b in x.highlights %}<li>{{ b }}</li>{% endfor %}</ul>
            {% endif %}
          </div>
        {% endfor %}
      </div>
      {% endif %}

      {% if certifications %}
      <div class="eu-sec">
        <h2>Certifications</h2>
        <ul>{% for c in certifications %}<li>{{ c.name }} — {{ c.org }} ({{ c.year }})</li>{% endfor %}</ul>
      </div>
      {% endif %}
    </main>
  </div>
</body>
</html>"""

# Kyndryl = same layout, only CSS tweaks (red sidebar + white text)
_KYNDRYL_HTML = _EUROPASS_HTML \
  .replace('.eu-side{background:#f8fafc;border-right:1px solid #e5e7eb;padding:22px}',
           '.eu-side{background:#b91c1c;color:#fff;border-right:1px solid #991b1b;padding:22px}') \
  .replace('.ico{width:22px;height:22px;border-radius:6px;background:#e2e8f0;',
           '.ico{width:22px;height:22px;border-radius:6px;background:rgba(255,255,255,.18);color:#fff;') \
  .replace('.eu-chip{display:inline-block;background:#eef2ff;color:#3730a3',
           '.eu-chip{display:inline-block;background:rgba(255,255,255,.18);color:#fff') \
  .replace('a{color:#1d4ed8', 'a{color:#fff')

# -----------------------------
# Tiny Jinja2 renderer (inline)
# -----------------------------
from jinja2 import Environment, BaseLoader, select_autoescape

def _html_from_cv(cv: dict, template_name: str = "europass") -> str:
    """Render minimal HTML from normalized CV using inline templates."""
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    # choose template
    tname = (template_name or "europass").lower()
    html_src = _KYNDRYL_HTML if tname == "kyndryl" else _EUROPASS_HTML
    j = env.from_string(html_src)

    # normalize model keys from typical cvnormalize output
    pi = (cv.get("personal_info") or cv.get("personal") or {}) if isinstance(cv, dict) else {}
    contacts = []
    def add(icon, val):
        if val: contacts.append({"ico": icon, "txt": val})
    add("@",  pi.get("email"))
    add("☎",  pi.get("phone"))
    add("in", pi.get("linkedin"))
    add("🌐", pi.get("website"))
    addr = ", ".join([pi.get("address") or "", pi.get("city") or "", pi.get("country") or ""]).strip(", ")
    add("📍", addr)

    # skills: flatten group items if present
    skills = []
    if isinstance(cv.get("skills_groups"), list):
        for g in cv["skills_groups"]:
            items = g.get("items") or []
            for s in items:
                if s and s not in skills:
                    skills.append(s)
    elif isinstance(cv.get("skills"), list):
        skills = cv["skills"]

    model = {
        "person": {"full_name": pi.get("full_name") or cv.get("name"),
                   "title":     pi.get("headline")  or cv.get("title")},
        "contacts": contacts,
        "skills": skills,
        "languages": cv.get("languages") or [],
        "certifications": cv.get("certifications") or [],
        "summary": cv.get("summary") or pi.get("summary"),
        "experiences": cv.get("work_experience") or cv.get("experience") or [],
        "education": cv.get("education") or [],
    }
    return j.render(**model)

# -----------------------------
# Helpers
# -----------------------------
def _derive_base_url(req: func.HttpRequest) -> str:
    if FUNCS_BASE_URL:
        return FUNCS_BASE_URL
    # derive from current request
    u = req.url
    # up to host (https://<host>)
    # req.url includes /api/cvagent, so remove path
    from urllib.parse import urlparse
    p = urlparse(u)
    return f"{p.scheme}://{p.netloc}"

def _build_url(req: func.HttpRequest, path: str, key: str | None) -> str:
    base = _derive_base_url(req)
    url = urljoin(base + "/", path.lstrip("/"))
    if key:
        sep = "&" if ("?" in url) else "?"
        url = f"{url}{sep}code={key}"
    return url

def _post_json(url: str, data: dict, timeout_sec: int = 60):
    r = requests.post(url, json=data, timeout=timeout_sec)
    try:
        j = r.json()
    except Exception:
        j = None
    return r.status_code, j, r.text

def _blob_service() -> BlobServiceClient:
    if CONN_STR:
        return BlobServiceClient.from_connection_string(CONN_STR)
    if ACCOUNT_URL:
        return BlobServiceClient(account_url=ACCOUNT_URL)
    raise RuntimeError("No storage credentials: set AzureWebJobsStorage or AZURE_STORAGE_*")

def _ensure_container(bc: BlobServiceClient, name: str):
    try:
        bc.create_container(name)
    except Exception:
        pass

def _upload_and_sas(pptx_bytes: bytes, blob_name: str, minutes: int = 30) -> str:
    bsc = _blob_service()
    _ensure_container(bsc, COMING_CONTAINER)
    bc = bsc.get_blob_client(COMING_CONTAINER, blob_name)
    bc.upload_blob(pptx_bytes, overwrite=True,
                   content_settings=ContentSettings(content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation"))

    # Build SAS (read)
    # If using connection string with key this works; otherwise provider identity is assumed to allow generate_blob_sas
    acc = bsc.account_name
    sas = generate_blob_sas(
        account_name=acc,
        container_name=COMING_CONTAINER,
        blob_name=blob_name,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=minutes)
    )
    base = ACCOUNT_URL or f"https://{acc}.blob.core.windows.net"
    return f"{base}/{COMING_CONTAINER}/{blob_name}?{sas}"

def _compute_out_name(source_name: str | None, template: str) -> str:
    import os as _os
    src = source_name or "cv.pdf"
    base = _os.path.splitext(_os.path.basename(src))[0] or "cv"
    return f"{base}-{template}.pdf"

# -----------------------------
# HTTP Trigger
# -----------------------------
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if req.method != "POST":
            return func.HttpResponse(json.dumps({"error": "POST only"}), status_code=405, mimetype="application/json")

        try:
            body = req.get_json()
        except Exception:
            return func.HttpResponse(json.dumps({"error": "Invalid JSON body"}), status_code=400, mimetype="application/json")

        # --------- Path A: Extract + Normalize from PPTX (base64) ---------
        if body.get("pptx_base64"):
            pptx_b64 = body.get("pptx_base64")
            pptx_name = body.get("pptx_name") or "resume.pptx"
            try:
                pptx_bytes = base64.b64decode(pptx_b64)
            except Exception as e:
                return func.HttpResponse(json.dumps({"error": f"Invalid base64: {e}"}), status_code=400, mimetype="application/json")

            # Upload + SAS for extractor
            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            blob_name = f"{ts}-{pptx_name}"
            sas_url = _upload_and_sas(pptx_bytes, blob_name)

            # Call pptxextract
            extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
            s1, data1, raw1 = _post_json(extract_url, {"ppt_blob_sas": sas_url, "pptx_name": pptx_name})
            if s1 != 200 or not isinstance(data1, dict):
                msg = (data1.get("error") if isinstance(data1, dict) else raw1)
                return func.HttpResponse(json.dumps({"error": f"pptxextract failed ({s1}): {msg}"}), status_code=500, mimetype="application/json")
            raw_cv = data1

            # Call cvnormalize
            normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
            s2, norm, raw2 = _post_json(normalize_url, {"raw": raw_cv, "pptx_name": pptx_name})
            if s2 != 200 or not isinstance(norm, dict):
                msg = (norm.get("error") if isinstance(norm, dict) else raw2)
                return func.HttpResponse(json.dumps({"error": f"cvnormalize failed ({s2}): {msg}"}), status_code=500, mimetype="application/json")

            normalized = norm.get("cv") or norm.get("normalized") or norm
            return func.HttpResponse(json.dumps({"cv": normalized, "source_name": pptx_name}), status_code=200, mimetype="application/json")

        # --------- Path B: Export → PDF (HTML renderer) ---------
        if "cv" in body:
            cv        = body["cv"]
            template  = (body.get("template") or "europass-2col").lower()
            source_nm = body.get("source_name") or body.get("pptx_name") or body.get("file_name")

            # For HTML look/feel, we accept both "europass-2col" and "europass"
            html_template_for_render = "kyndryl" if template == "kyndryl" else "europass"
            html = _html_from_cv(cv, html_template_for_render)

            # Filename rule: <source-base>-<template>.pdf
            out_name = _compute_out_name(source_nm, template)

            render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
            payload = {
                "out_name": out_name,
                "html": html,
                "css": ""  # inline CSS already in the HTML
            }
            s3, rjson, rraw = _post_json(render_url, payload, timeout_sec=120)
            if s3 != 200 or not isinstance(rjson, dict):
                return func.HttpResponse(json.dumps({"error": f"renderpdf_html failed ({s3}): {rjson or rraw}"}), status_code=500, mimetype="application/json")
            return func.HttpResponse(json.dumps(rjson), status_code=200, mimetype="application/json")

        return func.HttpResponse(json.dumps({"error": "Unsupported request"}), status_code=400, mimetype="application/json")

    except Exception as e:
        logging.exception("cvagent error")
        return func.HttpResponse(
            json.dumps({"error": f"cvagent failed: {str(e)}", "trace": traceback.format_exc()}),
            status_code=500,
            mimetype="application/json"
        )
