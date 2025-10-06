import os
import json
import base64
from datetime import datetime
from typing import Tuple, Optional

import azure.functions as func
from jinja2 import Environment, FileSystemLoader, select_autoescape, TemplateError

# Optional: upload to blob
try:
    from azure.storage.blob import BlobServiceClient
except Exception:  # pragma: no cover
    BlobServiceClient = None

# ---- Helpers -----------------------------------------------------------------

def _parse_primary_color() -> Tuple[str, Tuple[int, int, int]]:
    """
    Reads PDF_PRIMARY_RGB as 'r,g,b' (e.g. '15,98,254').
    Returns (hex '#RRGGBB', (r,g,b)). Default: 15,98,254 (#0F62FE).
    """
    raw = os.getenv("PDF_PRIMARY_RGB", "15,98,254")
    try:
        r, g, b = [int(x.strip()) for x in raw.split(",")]
        r = max(0, min(255, r))
        g = max(0, min(255, g))
        b = max(0, min(255, b))
    except Exception:
        r, g, b = (15, 98, 254)  # safe default
    hexcolor = f"#{r:02X}{g:02X}{b:02X}"
    return hexcolor, (r, g, b)


def _normalize_list(val):
    return val if isinstance(val, list) else []


def _normalize_cv(cv: dict) -> dict:
    """
    Make the template resilient: always pass lists / list-of-dicts
    and support either list-of-dicts or dict forms for skills.
    """
    cv = cv or {}

    # Basic sections as lists
    for k in ("work_experience", "education"):
        cv[k] = _normalize_list(cv.get(k))

    # Skills: accept dict {"Tech":[...]} or list-of-dicts [{"group":"Tech","items":[...]}]
    sg = cv.get("skills_groups")
    if isinstance(sg, dict):
        cv["skills_groups"] = [{"group": k, "items": (v or [])} for k, v in sg.items()]
    elif isinstance(sg, list):
        norm = []
        for x in sg:
            if isinstance(x, dict):
                norm.append({"group": x.get("group", ""), "items": x.get("items") or []})
        cv["skills_groups"] = norm
    else:
        cv["skills_groups"] = []

    # Languages (optional): accept list-of-dicts or dict of CEFR
    langs = cv.get("languages") or cv.get("language_skills")
    if isinstance(langs, list):
        cv["languages"] = langs
    elif isinstance(langs, dict):
        # try to convert {"English": {"reading":"C1",...}, ...} to list-of-dicts
        norm = []
        for name, levels in langs.items():
            if isinstance(levels, dict):
                norm.append({"name": name, **levels})
            else:
                norm.append({"name": name, "level": str(levels)})
        cv["languages"] = norm
    else:
        cv["languages"] = []

    # Personal info scaffold
    cv.setdefault("personal_info", {})
    pi = cv["personal_info"]
    pi.setdefault("full_name", "")
    pi.setdefault("headline", "")
    pi.setdefault("city", "")
    pi.setdefault("country", "")
    # optional
    for opt in ("email", "phone", "address", "nationality", "date_of_birth", "gender", "website", "linkedin"):
        pi.setdefault(opt, "")

    return cv


def _render_html(cv: dict, template_name: str, templates_dir: str) -> str:
    primary_hex, primary_rgb = _parse_primary_color()
    env = Environment(
        loader=FileSystemLoader(templates_dir),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template(template_name)
    return template.render(
        cv=cv,
        theme={
            "primary_hex": primary_hex,
            "primary_rgb": primary_rgb,
        },
        now=datetime.utcnow(),
    )


def _html_to_pdf_bytes(html: str) -> bytes:
    # Playwright must be in requirements.txt; Chromium will be fetched on first runs
    from playwright.sync_api import sync_playwright

    # Some environments require --no-sandbox
    launch_args = ["--no-sandbox", "--disable-setuid-sandbox"]
    pdf = b""
    with sync_playwright() as p:
        browser = p.chromium.launch(args=launch_args)
        context = browser.new_context()
        page = context.new_page()
        page.set_content(html, wait_until="load")
        pdf = page.pdf(
            format="A4",
            print_background=True,
            margin={"top": "14mm", "right": "14mm", "bottom": "14mm", "left": "14mm"},
        )
        browser.close()
    return pdf


def _upload_pdf(file_name: str, data: bytes) -> Tuple[Optional[str], Optional[str]]:
    """
    Uploads to blob if AzureWebJobsStorage + azure-storage-blob available.
    Uses:
      - PDF_OUT_CONTAINER (default 'pdf-out')
      - PDF_OUT_BASE (optional SAS container URL like 'https://acct.blob.core.windows.net/pdf-out?sv=...')
    Returns (blob_name, url_or_None)
    """
    conn_str = os.getenv("AzureWebJobsStorage")
    container = os.getenv("PDF_OUT_CONTAINER", "pdf-out")
    base = os.getenv("PDF_OUT_BASE")  # SAS container URL (optional)

    if not (conn_str and BlobServiceClient):
        return None, None

    bsc = BlobServiceClient.from_connection_string(conn_str)
    cc = bsc.get_container_client(container)
    try:
        cc.create_container()  # idempotent
    except Exception:
        pass
    cc.upload_blob(name=file_name, data=data, overwrite=True, content_type="application/pdf")

    url = None
    if base and "?" in base:
        # Build {container}/{blob}?SAS
        container_url, sas = base.split("?", 1)
        url = f"{container_url.rstrip('/')}/{file_name}?{sas}"
    return file_name, url


# ---- Azure Function entry -----------------------------------------------------

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    POST body:
    {
      "file_name": "mycv.pdf",
      "cv": { ...normalized/un-normalized CV data... }
    }
    Returns JSON with either blob/url or base64 content.
    """
    if req.method != "POST":
        return func.HttpResponse("POST only", status_code=405)

    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    file_name = (payload.get("file_name") or "cv.pdf").strip()
    if not file_name.lower().endswith(".pdf"):
        file_name += ".pdf"

    cv = _normalize_cv(payload.get("cv") or {})

    # Locate the template
    here = os.path.dirname(__file__)
    templates_dir = os.path.join(here, "templates")
    template_name = os.getenv("CV_TEMPLATE_NAME", "cv_europass.html")

    try:
        html = _render_html(cv=cv, template_name=template_name, templates_dir=templates_dir)
    except TemplateError as e:
        line = getattr(e, "lineno", "?")
        return func.HttpResponse(f"TemplateError (line {line}): {e}", status_code=500)

    # Render to PDF
    try:
        pdf_bytes = _html_to_pdf_bytes(html)
    except Exception as e:
        # On first cold run chromium might download; ask caller to retry once.
        return func.HttpResponse(f"PDF render error: {e}", status_code=500)

    # Try to upload to blob (if configured)
    blob_name, url = _upload_pdf(file_name, pdf_bytes)

    # Prefer returning URL; if not available, return base64 for convenience
    if url:
        body = {"ok": True, "pdf_blob": blob_name, "url": url}
        return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json")
    else:
        body = {
            "ok": True,
            "pdf_blob": file_name,
            "content_base64": base64.b64encode(pdf_bytes).decode("utf-8"),
        }
        return func.HttpResponse(json.dumps(body), status_code=200, mimetype="application/json")
