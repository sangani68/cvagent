import os, json, logging, re
from datetime import datetime, timedelta, timezone
import azure.functions as func
import requests

from jinja2 import Environment, BaseLoader, select_autoescape
from azure.storage.blob import BlobServiceClient, BlobSasPermissions, generate_blob_sas
from azure.storage.blob._shared.base_client import parse_connection_str

# ----------------------------------------------------
# Config (reuses your working envs)
# ----------------------------------------------------
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

# ----------------------------------------------------
# Storage
# ----------------------------------------------------
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
    logging.error(f"[chatcv] parse_connection_str failed: {e}")

# ----------------------------------------------------
# OpenAI optional (safe fallback if missing)
# ----------------------------------------------------
USE_AZURE_OPENAI = bool(os.environ.get("AZURE_OPENAI_ENDPOINT") and os.environ.get("AZURE_OPENAI_API_KEY"))
MODEL_NAME = os.environ.get("CHATCV_MODEL", "gpt-4.1")
OAI_AVAILABLE = False
oai = None
try:
    if USE_AZURE_OPENAI:
        from openai import AzureOpenAI
        oai = AzureOpenAI(
            api_key=os.environ["AZURE_OPENAI_API_KEY"],
            api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-06-01"),
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        )
        OAI_AVAILABLE = True
    else:
        # Standard OpenAI
        from openai import OpenAI
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key:
            oai = OpenAI(api_key=api_key)
            OAI_AVAILABLE = True
except Exception as e:
    logging.warning(f"[chatcv] OpenAI not available: {e}")
    OAI_AVAILABLE = False

# ----------------------------------------------------
# Helpers
# ----------------------------------------------------
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

def _blob_sas_url(container:str, blob_name:str, minutes:int=SAS_MINUTES)->str:
    if not (ACCOUNT_NAME and ACCOUNT_KEY):
        raise RuntimeError("Missing storage key for SAS")
    bc = _bsc.get_blob_client(container, blob_name)
    if not bc.exists():
        raise FileNotFoundError(f"Blob not found: {blob_name}")
    account_url = _bsc.url.rstrip("/")
    blob_url = f"{account_url}/{container}/{blob_name}"
    sas = generate_blob_sas(
        account_name=ACCOUNT_NAME,
        container_name=container,
        blob_name=blob_name,
        account_key=ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(minutes=minutes),
    )
    return f"{blob_url}?{sas}"

# ----------------------------------------------------
# Templates (same visual as your cvagent)
# ----------------------------------------------------
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

# ----------------------------------------------------
# GPT helpers (all try/except; graceful degradation)
# ----------------------------------------------------
INTENT_SCHEMA = {
  "type":"object",
  "properties":{
    "person_name":{"type":"string"},
    "template":{"type":"string","enum":["europass","kyndryl"]}
  },
  "required":["person_name"],
  "additionalProperties":False
}

STOP = {"cv","resume","give","show","for","of","in","the","a","an","template","please","generate","make","create"}

def _fallback_parse(prompt:str):
    p = re.sub(r"[^a-zA-Z0-9\s._-]+"," ",prompt or "").strip()
    toks = [w for w in p.split() if w.lower() not in STOP]
    name = " ".join(toks[:3]).strip() if toks else ""
    template = "kyndryl" if "kyndryl" in (prompt or "").lower() else ("europass" if "europass" in (prompt or "").lower() else "europass")
    return name, template

def parse_intent_with_gpt41(prompt:str):
    if not (OAI_AVAILABLE and oai):
        return _fallback_parse(prompt)
    try:
        resp = oai.responses.create(
            model=MODEL_NAME,
            response_format={"type":"json_schema","json_schema":{"name":"intent","schema":INTENT_SCHEMA}},
            input=f'Extract person_name and template (europass/kyndryl). Default template=europass.\nUser: {prompt}'
        )
        # Robust: prefer output_text; fallback to content JSON if SDK differs
        text = getattr(resp, "output_text", None)
        if not text:
            # Try to navigate content if output_text missing
            text = resp.output[0].content[0].text  # may raise; caught below
        data = json.loads(text)
        person = (data.get("person_name") or "").strip()
        template = (data.get("template") or "europass").strip().lower()
        if template not in ("europass","kyndryl"):
            template = "europass"
        if not person:
            raise ValueError("empty name from model")
        return person, template
    except Exception as e:
        logging.warning(f"[chatcv] intent parse via GPT failed; using fallback. err={e}")
        return _fallback_parse(prompt)

def list_recent_cv_blobs(limit:int=50):
    cc = _bsc.get_container_client(INCOMING_CONTAINER)
    blobs = list(cc.list_blobs())
    blobs = [b for b in blobs if str(b.name).lower().endswith((".pptx",".pptm",".ppsx",".ppt",".odp",".potx",".potm"))]
    blobs.sort(key=lambda b: b.last_modified or datetime(2000,1,1,tzinfo=timezone.utc), reverse=True)
    return blobs[:limit]

def choose_best_blob_fallback(person_name:str, blob_names:list[str])->str:
    tokens = [t for t in re.split(r"[\s._-]+", (person_name or "").lower()) if t]
    best, score = None, -1
    for b in blob_names:
        nm = b.lower()
        sc = sum(1 for t in tokens if t in nm)
        if sc > score:
            best, score = b, sc
    return best or "NONE"

def choose_best_blob_gpt(person_name:str, blob_names:list[str])->str:
    if not (OAI_AVAILABLE and oai):
        return choose_best_blob_fallback(person_name, blob_names)
    try:
        names = "\n".join(f"- {b}" for b in blob_names)
        prompt = f"""Select the best CV file name for person "{person_name}".
Choose exactly one filename from the list, or return "NONE" if nothing matches.
List:
{names}
Return ONLY JSON: {{ "best": "<filename or NONE>" }}"""
        resp = oai.responses.create(
            model=MODEL_NAME,
            response_format={"type":"json_object"},
            input=prompt
        )
        text = getattr(resp, "output_text", None) or "{}"
        data = json.loads(text)
        best = (data.get("best") or "NONE").strip()
        if best == "NONE":
            return choose_best_blob_fallback(person_name, blob_names)
        return best
    except Exception as e:
        logging.warning(f"[chatcv] blob choose via GPT failed; using fallback. err={e}")
        return choose_best_blob_fallback(person_name, blob_names)

# ----------------------------------------------------
# HTTP function
# ----------------------------------------------------
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("chatcv triggered")
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json")

    prompt = (body.get("prompt") or "").trim() if hasattr(str, "trim") else (body.get("prompt") or "").strip()
    if not prompt:
        return func.HttpResponse(json.dumps({"error":"Missing 'prompt'"}), status_code=400, mimetype="application/json")

    # 1) Intent (person/template)
    person, template = parse_intent_with_gpt41(prompt)
    if not person:
        return func.HttpResponse(json.dumps({
            "message":"I couldn’t figure out the person’s name. Try: “Give CV of Ada Lovelace in europass template”."
        }), status_code=200, mimetype="application/json")

    # 2) Find a candidate in 'incoming'
    try:
        recent = list_recent_cv_blobs(limit=60)
    except Exception as e:
        logging.exception("Failed to list blobs")
        return func.HttpResponse(json.dumps({
            "message": "I couldn’t access the 'incoming' container. Please check storage permissions/connection string.",
            "details": str(e)
        }), status_code=200, mimetype="application/json")

    names = [b.name for b in recent]
    best = choose_best_blob_gpt(person, names)
    if not best or best == "NONE":
        return func.HttpResponse(json.dumps({
            "person_name": person,
            "template": template,
            "message": f"I looked in '{INCOMING_CONTAINER}' but couldn’t find a matching PPTX for “{person}”. "
                       f"Please upload their PPTX to the '{INCOMING_CONTAINER}' container and try again."
        }), status_code=200, mimetype="application/json")

    # 3) Pipeline: SAS → extract → normalize → render
    try:
        sas = _blob_sas_url(INCOMING_CONTAINER, best)

        # extract
        extract_url = _build_url(req, PPTXEXTRACT_PATH, PPTXEXTRACT_KEY)
        s1, d1, r1 = _post_json(extract_url, {"ppt_blob_sas": sas, "pptx_name": best})
        if s1 != 200 or not isinstance(d1, dict):
            msg = (d1.get("error") if isinstance(d1, dict) else r1)
            return func.HttpResponse(json.dumps({
                "person_name": person, "template": template, "blob_name": best,
                "message": f"Extraction failed ({s1}). {msg}"
            }), status_code=200, mimetype="application/json")

        raw_cv = d1.get("raw") or d1.get("raw3") or d1

        # normalize
        normalize_url = _build_url(req, CVNORMALIZE_PATH, CVNORMALIZE_KEY)
        s2, d2, r2 = _post_json(normalize_url, {"raw": raw_cv, "pptx_name": best})
        if s2 != 200 or not isinstance(d2, dict):
            msg = (d2.get("error") if isinstance(d2, dict) else r2)
            return func.HttpResponse(json.dumps({
                "person_name": person, "template": template, "blob_name": best,
                "message": f"Normalize failed ({s2}). {msg}"
            }), status_code=200, mimetype="application/json")

        cv = d2.get("cv") or d2.get("normalized") or d2

        # render
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
        # Return friendly 200 with message instead of a 500 to the UI
        return func.HttpResponse(json.dumps({
            "person_name": person, "template": template,
            "message": f"Something went wrong while generating the PDF. Details: {str(e)}"
        }), status_code=200, mimetype="application/json")
