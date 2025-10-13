import os, json, logging, re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple, List

import azure.functions as func
import requests

from jinja2 import Environment, BaseLoader, select_autoescape
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._shared.base_client import parse_connection_str

# ========== ENV HELPERS (match your normalize style) ==========
def _get(name, *aliases, default=None):
    for k in (name, *aliases):
        v = os.getenv(k)
        if v: return v
    return default

# ----- AOAI (Azure OpenAI) -----
from openai import AzureOpenAI
AOAI_ENDPOINT    = _get("AOAI_ENDPOINT", "AZURE_OPENAI_ENDPOINT")
AOAI_KEY         = _get("AOAI_KEY", "AZURE_OPENAI_API_KEY")
AOAI_DEPLOYMENT  = _get("AOAI_DEPLOYMENT", "AZURE_OPENAI_DEPLOYMENT", default="gpt-4.1")
AOAI_API_VERSION = _get("AOAI_API_VERSION", "AZURE_OPENAI_API_VERSION", default="2024-10-21")

_client: Optional[AzureOpenAI] = None
def client() -> AzureOpenAI:
    global _client
    if _client is None:
        if not (AOAI_ENDPOINT and AOAI_KEY):
            raise RuntimeError("AOAI not configured (set AOAI_ENDPOINT and AOAI_KEY)")
        _client = AzureOpenAI(azure_endpoint=AOAI_ENDPOINT, api_key=AOAI_KEY, api_version=AOAI_API_VERSION)
    return _client

# ----- Downstream (reuse your working envs) -----
BASE_URL          = (_get("DOWNSTREAM_BASE_URL", "FUNCS_BASE_URL", default="") or "").rstrip("/")
PPTXEXTRACT_PATH  = _get("PPTXEXTRACT_PATH", default="/api/pptxextract")
CVNORMALIZE_PATH  = _get("CVNORMALIZE_PATH", default="/api/cvnormalize")
RENDER_PATH       = _get("RENDER_PATH", default="/api/renderpdf_html")
PPTXEXTRACT_KEY   = _get("PPTXEXTRACT_KEY", default="")
CVNORMALIZE_KEY   = _get("CVNORMALIZE_KEY", default="")
RENDER_KEY        = _get("RENDER_KEY", default="")

HTTP_TIMEOUT_SEC   = int(_get("HTTP_TIMEOUT_SEC", default="180"))
INCOMING_CONTAINER = _get("INCOMING_CONTAINER", default="incoming")
SAS_MINUTES        = int(_get("SAS_MINUTES", default="120"))

# ========== STORAGE ==========
CONN_STR = os.environ.get("AzureWebJobsStorage")
if not CONN_STR:
    raise RuntimeError("AzureWebJobsStorage not set")

_bsc = BlobServiceClient.from_connection_string(CONN_STR)

# Parse connection string and capture SAS if present
def _kv_from_conn_str(cs: str) -> dict:
    out = {}
    for part in cs.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip()
    return out

cs_kv = _kv_from_conn_str(CONN_STR)

ACCOUNT_NAME = None
ACCOUNT_KEY  = None
CONN_SAS     = (cs_kv.get("SharedAccessSignature") or cs_kv.get("SharedAccessSig") or "").lstrip("?")
ACCOUNT_URL  = cs_kv.get("BlobEndpoint") or _bsc.url.rstrip("/")

try:
    parsed = parse_connection_str(CONN_STR)
    # parse_connection_str returns a dict-like with account parts when AccountKey is present
    ACCOUNT_NAME = parsed.get("account_name")
    ACCOUNT_KEY  = parsed.get("account_key")
except Exception as e:
    logging.warning(f"[chatcv] parse_connection_str failed (likely SAS-only connection string): {e}")

# Explicit override wins (Linux env is case-sensitive)
env_name = os.environ.get("STORAGE_ACCOUNT_NAME")
env_key  = os.environ.get("STORAGE_ACCOUNT_KEY")
if env_name and env_key:
    ACCOUNT_NAME, ACCOUNT_KEY = env_name, env_key

if ACCOUNT_KEY:
    logging.info("[chatcv] Storage auth: using AccountKey (can mint SAS).")
elif CONN_SAS:
    logging.info("[chatcv] Storage auth: using SharedAccessSignature from connection string.")
else:
    logging.error("[chatcv] No AccountKey or SAS available; blob SAS URL generation will fail.")

# ========== HTTP/PIPELINE HELPERS ==========
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

def _blob_sas_url(container: str, blob_name: str, minutes: int = SAS_MINUTES) -> str:
    """
    Return a signed URL for the blob.
    - If we have an AccountKey, mint a fresh short-lived SAS.
    - Otherwise, if the connection string already contains a SAS, reuse it.
    """
    bc = _bsc.get_blob_client(container, blob_name)
    if not bc.exists():
        raise FileNotFoundError(f"Blob not found: {blob_name}")

    base_url = f"{ACCOUNT_URL}/{container}/{blob_name}"

    if ACCOUNT_KEY:
        from azure.storage.blob import generate_blob_sas, BlobSasPermissions
        sas = generate_blob_sas(
            account_name=ACCOUNT_NAME,
            container_name=container,
            blob_name=blob_name,
            account_key=ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(minutes=minutes),
        )
        return f"{base_url}?{sas}"

    if CONN_SAS:
        return f"{base_url}?{CONN_SAS}"

    raise RuntimeError("No AccountKey or SharedAccessSignature available to build SAS")

def list_recent_cv_blobs(limit:int=60):
    cc = _bsc.get_container_client(INCOMING_CONTAINER)
    blobs = list(cc.list_blobs())
    blobs = [b for b in blobs if str(b.name).lower().endswith((".pptx",".pptm",".ppsx",".ppt",".odp",".potx",".potm"))]
    blobs.sort(key=lambda b: b.last_modified or datetime(2000,1,1,tzinfo=timezone.utc), reverse=True)
    return blobs[:limit]

# ========== TEMPLATES (same as your exporter) ==========
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

# Kyndryl variant: red sidebar; main stays black on white
_KYNDRYL_HTML = _EUROPASS_HTML \
    .replace('#f8fafc', '#F9423A') \
    .replace('border-right:1px solid #e5e7eb', 'border-right:1px solid #a60f24')

def _html_from_cv(cv: dict, template_name: str = "europass") -> str:
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    j = env.from_string(_KYNDRYL_HTML if (template_name or "europass").lower() == "kyndryl" else _EUROPASS_HTML)
    pi = (cv.get("personal_info") or cv.get("personal") or {}) if isinstance(cv, dict) else {}
    contacts = []
    def add(icon, val):
        if val: contacts.append({"ico": icon, "txt": val})
    add("@",  pi.get("email")); add("☎", pi.get("phone")); add("in", pi.get("linkedin")); add("🌐", pi.get("website"))
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

# ========== GPT 4.1 JSON Schemas (AOAI chat.completions) ==========
INTENT_SCHEMA: Dict[str, Any] = {
    "type":"object","additionalProperties":False,
    "properties":{
        "person_name":{"type":"string"},
        "template":{"type":"string","enum":["europass","kyndryl"]}
    },
    "required":["person_name"]
}
BLOB_PICK_SCHEMA: Dict[str, Any] = {
    "type":"object","additionalProperties":False,
    "properties":{"best":{"type":"string"}},
    "required":["best"]
}

SYSTEM_INTENT = (
    "Extract the person_name and the template from the user's request. "
    "Allowed template values: europass, kyndryl. "
    "If the template is missing, default to europass. "
    "Return ONLY JSON."
)

SYSTEM_BLOB_PICK = (
    "You select the single best CV filename for the given person_name from the list. "
    "Return ONLY JSON with the filename in the 'best' field. If nothing matches, return 'NONE'."
)

def parse_intent_with_llm(prompt: str) -> Tuple[str,str]:
    """Returns (person_name, template|europass). Uses AOAI chat.completions with JSON schema."""
    resp = client().chat.completions.create(
        model=AOAI_DEPLOYMENT, temperature=0,
        response_format={"type":"json_schema","json_schema":{"name":"Intent","schema":INTENT_SCHEMA}},
        messages=[
            {"role":"system","content":SYSTEM_INTENT},
            {"role":"user","content":prompt}
        ]
    )
    content = resp.choices[0].message.content
    data = json.loads(content)
    person = (data.get("person_name") or "").strip()
    template = (data.get("template") or "europass").strip().lower()
    if template not in ("europass","kyndryl"):
        template = "europass"
    return person, template

def choose_best_blob_with_llm(person_name: str, candidates: List[str]) -> str:
    """Returns a single filename or 'NONE'."""
    payload = {"person_name": person_name, "candidates": candidates}
    resp = client().chat.completions.create(
        model=AOAI_DEPLOYMENT, temperature=0,
        response_format={"type":"json_schema","json_schema":{"name":"BlobPick","schema":BLOB_PICK_SCHEMA}},
        messages=[
            {"role":"system","content":SYSTEM_BLOB_PICK},
            {"role":"user","content":json.dumps(payload, ensure_ascii=False)}
        ]
    )
    content = resp.choices[0].message.content
    data = json.loads(content)
    best = (data.get("best") or "").strip()
    return best or "NONE"

# ========== HTTP TRIGGER ==========
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("chatcv triggered")
    if req.method != "POST":
        return func.HttpResponse("POST only", status_code=405)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(json.dumps({"error":"Invalid JSON"}), status_code=400, mimetype="application/json")

    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return func.HttpResponse(json.dumps({"error":"Missing 'prompt'"}), status_code=400, mimetype="application/json")

    # 1) Intent via AOAI
    try:
        person, template = parse_intent_with_llm(prompt)
    except Exception as e:
        logging.exception("intent parse failed")
        return func.HttpResponse(json.dumps({
            "message":"I couldn’t understand the request. Try: “Give CV of Ada Lovelace in kyndryl template”.",
            "details": f"intent-error: {e}"
        }), status_code=200, mimetype="application/json")

    if not person:
        return func.HttpResponse(json.dumps({
            "message":"I couldn’t figure out the person’s name. Try: “Give CV of Ada Lovelace in europass template”."
        }), status_code=200, mimetype="application/json")

    # 2) Find best blob from 'incoming' via AOAI
    try:
        recent = list_recent_cv_blobs(limit=60)
        names = [b.name for b in recent]
    except Exception as e:
        logging.exception("blob list failed")
        return func.HttpResponse(json.dumps({
            "person_name": person, "template": template,
            "message": "I couldn’t access the 'incoming' container. Please check storage permissions/connection string.",
            "details": str(e)
        }), status_code=200, mimetype="application/json")

    if not names:
        return func.HttpResponse(json.dumps({
            "person_name": person, "template": template,
            "message": f"No PPTX files found in '{INCOMING_CONTAINER}'. Please upload the CV PPTX and try again."
        }), status_code=200, mimetype="application/json")

    try:
        best = choose_best_blob_with_llm(person, names)
    except Exception as e:
        logging.exception("blob pick failed")
        best = "NONE"

    if not best or best == "NONE":
        return func.HttpResponse(json.dumps({
            "person_name": person, "template": template,
            "message": f"I looked in '{INCOMING_CONTAINER}' but couldn’t find a matching PPTX for “{person}”. "
                       f"Please upload their PPTX to the '{INCOMING_CONTAINER}' container and try again."
        }), status_code=200, mimetype="application/json")

    # 3) Pipeline: SAS → extract → normalize → render
    try:
        sas = _blob_sas_url(INCOMING_CONTAINER, best)

        # Extract
        extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
        s1, d1, r1 = _post_json(extract_url, {"ppt_blob_sas": sas, "pptx_name": best})
        if s1 != 200 or not isinstance(d1, dict):
            msg = (d1.get("error") if isinstance(d1, dict) else r1)
            return func.HttpResponse(json.dumps({
                "person_name": person, "template": template, "blob_name": best,
                "message": f"Extraction failed ({s1}). {msg}"
            }), status_code=200, mimetype="application/json")

        raw_cv = d1.get("raw") or d1.get("raw3") or d1

        # Normalize (your existing normalizer service)
        normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
        s2, d2, r2 = _post_json(normalize_url, {"raw": raw_cv, "pptx_name": best})
        if s2 != 200 or not isinstance(d2, dict):
            msg = (d2.get("error") if isinstance(d2, dict) else r2)
            return func.HttpResponse(json.dumps({
                "person_name": person, "template": template, "blob_name": best,
                "message": f"Normalize failed ({s2}). {msg}"
            }), status_code=200, mimetype="application/json")

        cv = d2.get("cv") or d2.get("normalized") or d2

        # Render
        html = _html_from_cv(cv, template)
        out_name = f"{os.path.splitext(os.path.basename(best))[0]}-{template}.pdf"
        render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
        s3, d3, r3 = _post_json(render_url, {"out_name": out_name, "html": html, "css": ""})
        if s3 != 200 or not isinstance(d3, dict):
            msg = (d3.get("error") if isinstance(d3, dict) else r3)
            return func.HttpResponse(json.dumps({
                "person_name": person, "template": template, "blob_name": best,
                "message": f"Render failed ({s3}). {msg}"
            }), status_code=200, mimetype="application/json")

        pdf_url = d3.get("pdf_url") or d3.get("url") or d3.get("sas_url") or d3.get("link")
        return func.HttpResponse(json.dumps({
            "person_name": person, "template": template, "blob_name": best,
            "pdf_url": pdf_url,
            "message": "Generated from the best-matching PPTX in 'incoming'."
        }), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.exception("chatcv fatal")
        return func.HttpResponse(json.dumps({
            "person_name": person, "template": template,
            "message": f"Something went wrong while generating the PDF. Details: {str(e)}"
        }), status_code=200, mimetype="application/json")
