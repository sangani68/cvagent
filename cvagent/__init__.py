import os, json, logging, base64, traceback
from datetime import datetime, timedelta
from urllib.parse import urljoin
import azure.functions as func
import requests

# Azure Blob SDK
from azure.storage.blob import (
    BlobServiceClient,
    ContentSettings,
    generate_blob_sas,
    BlobSasPermissions
)

# =========================
# Configuration
# =========================
# Function routes (same Function App by default)
PPTXEXTRACT_PATH = os.environ.get("PPTXEXTRACT_PATH", "/api/pptxextract")
CVNORMALIZE_PATH = os.environ.get("CVNORMALIZE_PATH", "/api/cvnormalize")
RENDER_PATH      = os.environ.get("RENDER_PATH",      "/api/renderpdf_html")

# Function keys (or shared FUNCS_KEY)
PPTXEXTRACT_KEY  = os.environ.get("PPTXEXTRACT_KEY") or os.environ.get("FUNCS_KEY")
CVNORMALIZE_KEY  = os.environ.get("CVNORMALIZE_KEY") or os.environ.get("FUNCS_KEY")
RENDER_KEY       = os.environ.get("RENDER_KEY")      or os.environ.get("FUNCS_KEY")

# Base URL (optional). If unset, derived from incoming request.
FUNCS_BASE_URL   = os.environ.get("FUNCS_BASE_URL", "").rstrip("/")

# Storage config (multiple ways supported)
AZURE_CONN_STR   = os.environ.get("AzureWebJobsStorage") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
BLOB_ACCOUNT     = os.environ.get("BLOB_ACCOUNT_NAME")
BLOB_KEY         = os.environ.get("BLOB_ACCOUNT_KEY")
ACCOUNT_URL      = os.environ.get("AZURE_STORAGE_BLOB_URL") or os.environ.get("BLOB_ACCOUNT_URL")
COMING_CONTAINER = os.environ.get("COMING_CONTAINER", "coming")

# =========================
# Inline templates
# =========================

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

# Kyndryl = same layout; red sidebar + white text
_KYNDRYL_HTML = _EUROPASS_HTML \
  .replace('.eu-side{background:#f8fafc;border-right:1px solid #e5e7eb;padding:22px}',
           '.eu-side{background:#b91c1c;color:#fff;border-right:1px solid #991b1b;padding:22px}') \
  .replace('.ico{width:22px;height:22px;border-radius:6px;background:#e2e8f0;',
           '.ico{width:22px;height:22px;border-radius:6px;background:rgba(255,255,255,.18);color:#fff;') \
  .replace('.eu-chip{display:inline-block;background:#eef2ff;color:#3730a3',
           '.eu-chip{display:inline-block;background:rgba(255,255,255,.18);color:#fff') \
  .replace('a{color:#1d4ed8', 'a{color:#fff')

# =========================
# Rendering helpers
# =========================
from jinja2 import Environment, BaseLoader, select_autoescape

def _html_from_cv(cv: dict, template_name: str = "europass") -> str:
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    t = (template_name or "europass").lower()
    src = _KYNDRYL_HTML if t == "kyndryl" else _EUROPASS_HTML

    j = env.from_string(src)

    pi = (cv.get("personal_info") or cv.get("personal") or {}) if isinstance(cv, dict) else {}
    contacts = []
    def add(icon, val):
        if val: contacts.append({"ico": icon, "txt": val})
    add("@",  pi.get("email")); add("☎", pi.get("phone")); add("in", pi.get("linkedin"))
    add("🌐", pi.get("website"))
    addr = ", ".join([pi.get("address") or "", pi.get("city") or "", pi.get("country") or ""]).strip(", ")
    add("📍", addr)

    # flatten skills
    skills = []
    if isinstance(cv.get("skills_groups"), list):
        for g in cv["skills_groups"]:
            for s in (g.get("items") or []):
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

# =========================
# URL helpers
# =========================
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

# =========================
# Storage helpers (robust)
# =========================
def _get_blob_service() -> tuple[BlobServiceClient, str, str]:
    """
    Returns (BlobServiceClient, account_name, account_key_or_None).
    Works with:
    - AzureWebJobsStorage / AZURE_STORAGE_CONNECTION_STRING (full connection string)
    - BLOB_ACCOUNT_NAME + BLOB_ACCOUNT_KEY (+ optional BLOB_ACCOUNT_URL)
    """
    # Connection string path
    if AZURE_CONN_STR and "AccountName=" in AZURE_CONN_STR and "AccountKey=" in AZURE_CONN_STR:
        bsc = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
        # extract pieces for SAS
        acc = _get_pair(AZURE_CONN_STR, "AccountName")
        key = _get_pair(AZURE_CONN_STR, "AccountKey")
        return bsc, acc, key

    # Explicit account/key path
    if BLOB_ACCOUNT and BLOB_KEY:
        base_url = ACCOUNT_URL or f"https://{BLOB_ACCOUNT}.blob.core.windows.net"
        bsc = BlobServiceClient(account_url=base_url, credential=BLOB_KEY)
        return bsc, BLOB_ACCOUNT, BLOB_KEY

    # If someone put a SAS URL or invalid string into AzureWebJobsStorage → don't try to parse
    # but we cannot generate SAS without a key. Fail fast with a clear message.
    raise RuntimeError("Storage not configured: set AzureWebJobsStorage (with AccountKey) "
                       "or BLOB_ACCOUNT_NAME + BLOB_ACCOUNT_KEY.")

def _get_pair(conn: str, key: str) -> str | None:
    # Parse "key=value" segments in connection string
    parts = conn.split(";")
    for p in parts:
        if not p: continue
        if p.strip().lower().startswith(key.lower() + "="):
            return p.split("=",1)[1]
    return None

def _ensure_container(bsc: BlobServiceClient, name: str):
    try: bsc.create_container(name)
    except Exception: pass

def _upload_pptx_and_get_sas(pptx_bytes: bytes, blob_name: str, minutes: int = 30) -> str:
    bsc, acc_name, acc_key = _get_blob_service()
    _ensure_container(bsc, COMING_CONTAINER)
    bc = bsc.get_blob_client(COMING_CONTAINER, blob_name)
    bc.upload_blob(
        pptx_bytes, overwrite=True,
        content_settings=ContentSettings(
            content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation"
        )
    )
    if not acc_key:
        raise RuntimeError("Cannot generate SAS without account key. "
                           "Provide AzureWebJobsStorage with AccountKey or BLOB_ACCOUNT_KEY.")
    sas = generate_blob_sas(
        account_name=acc_name,
        container_name=COMING_CONTAINER,
        blob_name=blob_name,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=minutes),
        account_key=acc_key,
    )
    base = ACCOUNT_URL or f"https://{acc_name}.blob.core.windows.net"
    return f"{base}/{COMING_CONTAINER}/{blob_name}?{sas}"

def _compute_out_name(source_name: str | None, template: str, fallback_file_name: str | None) -> str:
    # Preferred: <source-base>-<template>.pdf if we know the original source
    if source_name:
        import os as _os
        base = _os.path.splitext(_os.path.basename(source_name))[0] or "cv"
        return f"{base}-{template}.pdf"
    # Otherwise respect UI-provided file_name to avoid breaking your page
    name = (fallback_file_name or "cv.pdf").strip()
    return name if name.lower().endswith(".pdf") else (name + ".pdf")

# =========================
# HTTP Trigger
# =========================
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if req.method != "POST":
            return func.HttpResponse(json.dumps({"error":"POST only"}), status_code=405, mimetype="application/json")

        try:
            body = req.get_json()
        except Exception:
            return func.HttpResponse(json.dumps({"error":"Invalid JSON body"}), status_code=400, mimetype="application/json")

        # 1) Extract + Normalize from PPTX (UI: mode="normalize_only")
        if body.get("mode") == "normalize_only" and body.get("pptx_base64"):
            pptx_name = body.get("pptx_name") or "resume.pptx"
            try:
                pptx_bytes = base64.b64decode(body["pptx_base64"])
            except Exception as e:
                return func.HttpResponse(json.dumps({"error": f"Invalid base64: {e}"}), status_code=400, mimetype="application/json")

            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            blob_name = f"{ts}-{pptx_name}"
            try:
                sas_url = _upload_pptx_and_get_sas(pptx_bytes, blob_name)
            except Exception as e:
                # Frequently where "The string did not match the expected pattern." occurs → clarify
                msg = f"Upload/SAS failed: {str(e)}. Ensure AzureWebJobsStorage is a full connection string with AccountKey, or set BLOB_ACCOUNT_NAME + BLOB_ACCOUNT_KEY."
                return func.HttpResponse(json.dumps({"error": msg}), status_code=500, mimetype="application/json")

            # Call extractor (expects ppt_blob_sas)
            extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
            s1, j1, raw1 = _post_json(extract_url, {"ppt_blob_sas": sas_url, "pptx_name": pptx_name})
            if s1 != 200 or not isinstance(j1, dict):
                return func.HttpResponse(json.dumps({"error": f"pptxextract failed ({s1}): {j1 or raw1}"}), status_code=500, mimetype="application/json")

            # Normalize
            normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
            s2, j2, raw2 = _post_json(normalize_url, {"raw": j1, "pptx_name": pptx_name})
            if s2 != 200 or not isinstance(j2, dict):
                return func.HttpResponse(json.dumps({"error": f"cvnormalize failed ({s2}): {j2 or raw2}"}), status_code=500, mimetype="application/json")

            normalized = j2.get("cv") or j2.get("normalized") or j2
            return func.HttpResponse(json.dumps({"cv": normalized, "source_name": pptx_name}), status_code=200, mimetype="application/json")

        # 2) Export → PDF (UI: { cv, template, file_name })
        if "cv" in body:
            cv        = body["cv"]
            template  = (body.get("template") or "europass").lower()   # UI sends "europass" by default
            file_name = body.get("file_name")                           # UI OutName field
            source_nm = body.get("source_name") or body.get("pptx_name") or body.get("file_name")

            # Render HTML (map "europass" from UI to europass HTML)
            html_tpl = "kyndryl" if template == "kyndryl" else "europass"
            html = _html_from_cv(cv, html_tpl)

            # Filename: prefer <source-base>-<template>.pdf when source is known; else keep UI filename
            out_name = _compute_out_name(source_nm, template, file_name)

            # Call renderer
            render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
            s3, j3, raw3 = _post_json(render_url, {"html": html, "css": "", "out_name": out_name})
            if s3 != 200 or not isinstance(j3, dict):
                return func.HttpResponse(json.dumps({"error": f"renderpdf_html failed ({s3}): {j3 or raw3}"}), status_code=500, mimetype="application/json")

            return func.HttpResponse(json.dumps(j3), status_code=200, mimetype="application/json")

        return func.HttpResponse(json.dumps({"error":"Unsupported request"}), status_code=400, mimetype="application/json")

    except Exception as e:
        logging.exception("cvagent error")
        return func.HttpResponse(
            json.dumps({"error": f"cvagent failed: {str(e)}", "trace": traceback.format_exc()}),
            status_code=500, mimetype="application/json"
        )
