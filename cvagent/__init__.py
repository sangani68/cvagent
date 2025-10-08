import os, json, logging, base64, requests
from datetime import datetime, timedelta, timezone
import azure.functions as func
from jinja2 import Environment, BaseLoader, select_autoescape
from azure.storage.blob import BlobServiceClient, ContentSettings, BlobSasPermissions, generate_blob_sas
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

# Optional override for a built-in data-URL logo (can be huge). If set, it wins.
KYNDRYL_LOGO_DATA_ENV = os.environ.get("KYNDRYL_LOGO_DATA", "").strip()

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
    logging.error(f"parse_connection_str error: {e}")

env_name, env_key = os.environ.get("STORAGE_ACCOUNT_NAME"), os.environ.get("STORAGE_ACCOUNT_KEY")
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
        raise RuntimeError("Missing storage credentials")

    # Safe container creation (no error if exists)
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
# HTML TEMPLATES (table/flex fallback; PDF-safe)
# ==============================================================

# NOTE: Avoid CSS Grid. Many HTML→PDF engines don’t support it consistently.
# Use a simple 2-column table or flex + fixed widths.

_EURO_BASE = """
<!doctype html>
<html><head><meta charset="utf-8"/>
<title>{{ person.full_name or 'Curriculum Vitae' }}</title>
<style>
@page { size: A4; margin: 0 }
body { margin:0; font-family: Arial, Helvetica, sans-serif; font-size:12px; color:#0f172a }
table.layout { width:100%; border-spacing:0; table-layout:fixed }
td.side { width: 32%; vertical-align: top; background: {{ side_bg }}; color: {{ side_fg }}; padding: 18px 16px; }
td.main { width: 68%; vertical-align: top; padding: 20px 22px; }
h1 { font-size: 24px; margin: 0 0 2px 0; }
.sub { color:#475569; font-size: 13px; margin: 4px 0 12px 0; }
.sec { margin: 14px 0 0 0; }
.sec h2 { font-size: 14px; margin: 0 0 8px 0; text-transform: uppercase; letter-spacing: .06em; }
.kv { margin: 5px 0; }
.chips { margin-top: 6px; }
.chip { display: inline-block; border-radius: 999px; padding: 3px 10px; margin: 3px 6px 0 0; font-size: 11px; border:1px solid {{ chip_border }}; background: {{ chip_bg }}; color: {{ chip_fg }}; }
.line2 { color:#64748b; font-size:12px; margin: 2px 0; }
ul { margin: 6px 0 6px 18px; padding: 0; }
.logo-wrap { text-align:center; margin-top: 16px; }
.logo { width: 110px; }
.hr { border:0; border-top:1px solid #e5e7eb; margin: 12px 0; }
.photo { width: 100px; height: 100px; border-radius: 50%; object-fit: cover; }
</style>
</head>
<body>
<table class="layout"><tr>
  <td class="side">
    {% if photo_b64 %}<div style="text-align:center;margin-bottom:10px;">
      <img class="photo" src="data:image/png;base64,{{ photo_b64 }}" alt="Profile"/>
    </div>{% endif %}
    <h1 style="color: {{ name_color }}">{{ person.full_name or '' }}</h1>
    {% if person.title %}<div class="sub" style="color: {{ title_color }}">{{ person.title }}</div>{% endif %}
    {% if location %}<div class="kv">{{ location }}</div>{% endif %}
    {% if side_languages %}
      <div class="sec"><h2 style="color: {{ side_h_color }}">Languages</h2>
        <ul style="list-style:none;margin-left:0">
          {% for l in side_languages %}<li>{{ l }}</li>{% endfor %}
        </ul>
      </div>
    {% endif %}
    {% if show_logo %}
    <div class="logo-wrap">
      <img class="logo" src="{{ logo_src }}" alt="Kyndryl Logo"/>
    </div>
    {% endif %}
  </td>
  <td class="main">
    {% if summary %}<div class="sec"><h2>About Me</h2><div>{{ summary }}</div></div><hr class="hr"/>{% endif %}
    {% if experiences %}
      <div class="sec"><h2>{{ exp_title }}</h2>
        {% for e in experiences %}
          <div style="margin: 10px 0 8px 0;">
            <div><strong>{{ e.title }}</strong> — {{ e.company }}</div>
            <div class="line2">{{ e.start_date }}{% if e.end_date %} – {{ e.end_date }}{% else %} – Present{% endif %}{% if e.location %} • {{ e.location }}{% endif %}</div>
            {% if e.description %}<div style="margin-top:6px">{{ e.description }}</div>{% endif %}
            {% if e.bullets %}<ul>{% for b in e.bullets %}<li>{{ b }}</li>{% endfor %}</ul>{% endif %}
          </div>
        {% endfor %}
      </div>
    {% endif %}
    {% if skills %}
      <div class="sec"><h2>Skills</h2>
        <div class="chips">{% for s in skills %}<span class="chip">{{ s }}</span>{% endfor %}</div>
      </div>
    {% endif %}
    {% if achievements %}
      <div class="sec"><h2>Achievements</h2>
        <ul>{% for a in achievements %}<li>{{ a }}</li>{% endfor %}</ul>
      </div>
    {% endif %}
    {% if education %}
      <div class="sec"><h2>Education & Training</h2>
        {% for ed in education %}
          <div style="margin: 10px 0 8px 0;">
            <div><strong>{{ ed.degree or ed.title }}</strong> — {{ ed.institution }}</div>
            <div class="line2">{{ ed.start_date }}{% if ed.end_date %} – {{ ed.end_date }}{% endif %}{% if ed.location %} • {{ ed.location }}{% endif %}</div>
            {% if ed.details %}<div style="margin-top:6px">{{ ed.details }}</div>{% endif %}
          </div>
        {% endfor %}
      </div>
    {% endif %}
    {% if main_languages %}
      <div class="sec"><h2>Languages</h2>
        <div class="chips">{% for l in main_languages %}<span class="chip">{{ l }}</span>{% endfor %}</div>
      </div>
    {% endif %}
  </td>
</tr></table>
</body></html>
"""

# Europass: pale sidebar, blue chips, dark text (2-column table)
_EUROPASS_HTML = _EURO_BASE

# Kyndryl: same layout but with red left sidebar and white text; logo on sidebar bottom
# We’ll pass colors via the model in _html_from_cv.
# (Template string is the same; only variables differ.)

# ==============================================================
# RENDERER
# ==============================================================
def _as_lang_chips(languages):
    chips = []
    for l in languages or []:
        name = l.get("name") or l.get("language") or l.get("lang") or ""
        level = l.get("level") or ""
        chips.append(f"{name}" + (f" — {level}" if level else ""))
    return chips

def _html_from_cv(cv, template_name="europass", logo_override_dataurl=""):
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html"]))
    tname = (template_name or "europass").lower()
    tpl = env.from_string(_EUROPASS_HTML)  # we use same skeleton; only colors flip

    pi = (cv.get("personal_info") or {}) if isinstance(cv, dict) else {}
    skills = [s for g in (cv.get("skills_groups") or []) for s in (g.get("items") or [])]
    languages = _as_lang_chips(cv.get("languages"))

    location = ", ".join([x for x in [pi.get("city"), pi.get("country")] if x])

    # decide palette based on template
    if tname == "kyndryl":
        side_bg = "#FF462D"
        side_fg = "#FFFFFF"
        name_color = "#FFFFFF"
        title_color = "#FFFFFF"
        side_h_color = "#FFFFFF"
        chip_bg, chip_fg, chip_border = "#FFFFFF", "#FF462D", "#FFFFFF"
        show_logo = True
        exp_title = "Previous Roles"
    else:
        side_bg = "#F8FAFC"
        side_fg = "#0F172A"
        name_color = "#0F172A"
        title_color = "#475569"
        side_h_color = "#0F172A"
        chip_bg, chip_fg, chip_border = "#EEF2FF", "#3730A3", "#E0E7FF"
        show_logo = False
        exp_title = "Work Experience"

    # logo source resolution (prefer explicit data URL)
    data_url_from_body = logo_override_dataurl.strip() if logo_override_dataurl else ""
    data_url_from_env = KYNDRYL_LOGO_DATA_ENV
    logo_src = data_url_from_body or data_url_from_env or "https://upload.wikimedia.org/wikipedia/commons/7/73/Kyndryl_logo.svg"

    model = {
        "person": {
            "full_name": pi.get("full_name"),
            "title": pi.get("headline"),
            "city": pi.get("city"),
            "country": pi.get("country"),
        },
        "location": location,
        "skills": skills,
        "languages": cv.get("languages") or [],
        "summary": cv.get("summary") or pi.get("summary"),
        "experiences": cv.get("work_experience") or [],
        "education": cv.get("education") or [],
        "achievements": cv.get("achievements") or [],
        "main_languages": [],                 # Europass uses chips on main
        "side_languages": languages,          # show languages on sidebar for both
        "photo_b64": pi.get("photo_base64") or "",
        "exp_title": exp_title,
        "side_bg": side_bg,
        "side_fg": side_fg,
        "name_color": name_color,
        "title_color": title_color,
        "side_h_color": side_h_color,
        "chip_bg": chip_bg,
        "chip_fg": chip_fg,
        "chip_border": chip_border,
        "show_logo": show_logo if tname == "kyndryl" else False,
        "logo_src": logo_src,
    }
    return tpl.render(**model)

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
        # 1) Extract + Normalize
        if body.get("mode") == "normalize_only":
            pptx_b64 = body.get("pptx_base64")
            pptx_name = body.get("pptx_name") or "resume.pptx"
            if not pptx_b64:
                return func.HttpResponse(json.dumps({"error": "Missing pptx_base64"}), status_code=400, mimetype="application/json")

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
            return func.HttpResponse(json.dumps({"cv": normalized}), status_code=200, mimetype="application/json")

        # 2) Render
        if "cv" in body:
            cv = body["cv"]
            out_name = body.get("file_name") or "cv.pdf"
            template = (body.get("template") or "europass").lower()
            # Allow user to pass a data URL for the Kyndryl logo
            kyndryl_logo_data = body.get("kyndryl_logo_data", "")  # e.g. "data:image/png;base64,AAA..."
            html = _html_from_cv(cv, template, kyndryl_logo_data)

            render_url = _build_url(req, RENDER_PATH, RENDER_KEY)
            payload = {"out_name": out_name, "html": html, "css": ""}  # keep inline CSS only
            s3, rjson, rraw = _post_json(render_url, payload)
            if s3 != 200 or not isinstance(rjson, dict):
                raise RuntimeError(f"renderpdf_html failed ({s3}): {rraw}")

            return func.HttpResponse(json.dumps(rjson), status_code=200, mimetype="application/json")

        return func.HttpResponse(json.dumps({"error": "Unsupported request"}), status_code=400, mimetype="application/json")

    except Exception as e:
        logging.exception("cvagent error")
        return func.HttpResponse(json.dumps({"error": f"cvagent failed: {str(e)}"}), status_code=500, mimetype="application/json")
