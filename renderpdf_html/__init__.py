import os
import sys
import json
import base64
import subprocess
from datetime import datetime
from typing import Tuple, Optional, List

import azure.functions as func
from jinja2 import Environment, select_autoescape, TemplateError

# Optional blob upload
try:
    from azure.storage.blob import BlobServiceClient
except Exception:  # pragma: no cover
    BlobServiceClient = None


# =============================================================================
# Helpers
# =============================================================================

def _parse_primary_color() -> Tuple[str, Tuple[int, int, int]]:
    """Read PDF_PRIMARY_RGB 'r,g,b' -> ('#RRGGBB',(r,g,b)); default 15,98,254."""
    raw = os.getenv("PDF_PRIMARY_RGB", "15,98,254")
    try:
        r, g, b = [int(x.strip()) for x in raw.split(",")]
        r = max(0, min(255, r)); g = max(0, min(255, g)); b = max(0, min(255, b))
    except Exception:
        r, g, b = (15, 98, 254)
    return f"#{r:02X}{g:02X}{b:02X}", (r, g, b)


def _normalize_list(val):
    return val if isinstance(val, list) else []


def _normalize_cv(cv: dict) -> dict:
    """Give the template predictable shapes (lists, list-of-dicts, defaults)."""
    cv = cv or {}

    # Lists
    for k in ("work_experience", "education"):
        cv[k] = _normalize_list(cv.get(k))

    # Skills groups: dict -> list-of-dicts; list -> cleaned list
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

    # Languages (accept dict or list)
    langs = cv.get("languages") or cv.get("language_skills")
    if isinstance(langs, list):
        cv["languages"] = langs
    elif isinstance(langs, dict):
        norm = []
        for name, levels in langs.items():
            if isinstance(levels, dict):
                entry = {"name": name}; entry.update(levels); norm.append(entry)
            else:
                norm.append({"name": name, "level": str(levels)})
        cv["languages"] = norm
    else:
        cv["languages"] = []

    # Personal info defaults
    pi = cv.setdefault("personal_info", {})
    for key in (
        "full_name", "headline", "address", "city", "country",
        "email", "phone", "website", "linkedin",
        "nationality", "date_of_birth", "gender", "summary"
    ):
        pi.setdefault(key, "")

    return cv


# ---------- Template discovery (file-based, not loader-path dependent) --------

def _template_search_dirs() -> List[str]:
    """
    Directory order:
      1) CV_TEMPLATE_DIR (abs or relative to /site/wwwroot)
      2) /site/wwwroot/renderpdf_html/templates
      3) /site/wwwroot/templates
      4) /site/wwwroot/renderpdf_html
      5) /site/wwwroot
    Only existing dirs are kept, duplicates removed.
    """
    here = os.path.dirname(__file__)                      # /site/wwwroot/renderpdf_html
    app_root = os.path.abspath(os.path.join(here, ".."))  # /site/wwwroot

    dirs: List[str] = []
    override = os.getenv("CV_TEMPLATE_DIR")
    if override:
        dirs.append(override if os.path.isabs(override) else os.path.join(app_root, override))

    dirs += [
        os.path.join(here, "templates"),
        os.path.join(app_root, "templates"),
        here,
        app_root,
    ]

    seen, ordered = set(), []
    for d in dirs:
        d = os.path.normpath(d)
        if d not in seen and os.path.isdir(d):
            seen.add(d); ordered.append(d)
    return ordered


def _find_template_file(wanted: str) -> Optional[str]:
    """Return absolute path to template if found (exact or case-insensitive)."""
    # exact first
    for d in _template_search_dirs():
        p = os.path.join(d, wanted)
        if os.path.isfile(p):
            return p
    # case-insensitive fallback
    lower = wanted.lower()
    for d in _template_search_dirs():
        try:
            for fname in os.listdir(d):
                if fname.lower() == lower and os.path.isfile(os.path.join(d, fname)):
                    return os.path.join(d, fname)
        except Exception:
            continue
    return None


def _render_html_from_file(cv: dict, filepath: str) -> str:
    """Read a template file and render it using env.from_string()."""
    primary_hex, primary_rgb = _parse_primary_color()
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()
    env = Environment(
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True, lstrip_blocks=True,
    )
    tpl = env.from_string(content)
    return tpl.render(cv=cv, theme={"primary_hex": primary_hex, "primary_rgb": primary_rgb}, now=datetime.utcnow())


# ---------- Playwright helpers -------------------------------------------------

def _has_chromium(path: str) -> bool:
    """Detect if a Chromium 'headless_shell' exists somewhere under path."""
    try:
        for root, _, files in os.walk(path):
            if "headless_shell" in files:
                return True
    except Exception:
        pass
    return False


def _ensure_playwright_browsers() -> str:
    """
    Ensure Chromium exists. If PLAYWRIGHT_BROWSERS_PATH points to a packaged
    directory (read-only), we just use it. If missing, install Chromium into a
    writable path (/home/site/pw-browsers) and return it.
    """
    app_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    target = os.getenv("PLAYWRIGHT_BROWSERS_PATH")  # may be 'pw-browsers' (packaged) or abs path
    if target:
        # Convert relative to absolute (relative -> under app root)
        if not os.path.isabs(target):
            target = os.path.join(app_root, target)
    else:
        # Writable location outside run-from-package
        target = "/home/site/pw-browsers"

    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = target
    os.makedirs(target, exist_ok=True)

    if _has_chromium(target):
        return target

    # Try installing Chromium into 'target'
    cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
    # Ensure our vendored packages are importable when running the CLI
    env = os.environ.copy()
    vendored = os.path.join(app_root, ".python_packages", "lib", "site-packages")
    env["PYTHONPATH"] = f"{vendored}:{env.get('PYTHONPATH','')}"
    proc = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to install Playwright browsers: {proc.stderr or proc.stdout}")

    return target


def _html_to_pdf_bytes(html: str) -> bytes:
    """
    HTML → PDF using Playwright Chromium.
    - First try render directly (works if browsers are already present).
    - On 'Executable doesn't exist' error, install browsers to a writable path
      and retry once.
    """
    from playwright.sync_api import sync_playwright

    def render_once() -> bytes:
        args = ["--no-sandbox", "--disable-setuid-sandbox"]
        with sync_playwright() as p:
            browser = p.chromium.launch(args=args)
            context = browser.new_context()
            page = context.new_page()
            page.set_content(html, wait_until="load")
            pdf_bytes = page.pdf(
                format="A4",
                print_background=True,
                margin={"top": "14mm", "right": "14mm", "bottom": "14mm", "left": "14mm"},
            )
            browser.close()
        return pdf_bytes

    try:
        return render_once()
    except Exception as e:
        msg = str(e)
        if "Executable doesn't exist" in msg or "playwright install" in msg.lower():
            _ensure_playwright_browsers()
            return render_once()
        raise


# ---------- Blob upload --------------------------------------------------------

def _upload_pdf(file_name: str, data: bytes) -> Tuple[Optional[str], Optional[str]]:
    """
    Uploads to blob if AzureWebJobsStorage is set and azure-storage-blob is available.
    PDF_OUT_CONTAINER (default 'pdf-out')
    PDF_OUT_BASE (SAS container URL) → returns a direct URL if provided.
    """
    conn_str = os.getenv("AzureWebJobsStorage")
    container = os.getenv("PDF_OUT_CONTAINER", "pdf-out")
    base = os.getenv("PDF_OUT_BASE")

    if not (conn_str and BlobServiceClient):
        return None, None

    bsc = BlobServiceClient.from_connection_string(conn_str)
    cc = bsc.get_container_client(container)
    try:
        cc.create_container()
    except Exception:
        pass
    cc.upload_blob(name=file_name, data=data, overwrite=True, content_type="application/pdf")

    url = None
    if base and "?" in base:
        container_url, sas = base.split("?", 1)
        url = f"{container_url.rstrip('/')}/{file_name}?{sas}"
    return file_name, url


# =============================================================================
# Azure Function
# =============================================================================

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    POST body:
    {
      "file_name": "mycv.pdf",
      "cv": { ... }
    }

    Env:
      CV_TEMPLATE_NAME (default: cv_europass.html)
      CV_TEMPLATE_DIR  (optional search dir; abs or relative to /site/wwwroot)
      PDF_PRIMARY_RGB  (e.g., "15,98,254")
      PLAYWRIGHT_BROWSERS_PATH (recommended: 'pw-browsers')
      AzureWebJobsStorage, PDF_OUT_CONTAINER, PDF_OUT_BASE (for blob upload)
    """
    if req.method != "POST":
        return func.HttpResponse("POST only", status_code=405)

    # Parse body
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    file_name = (payload.get("file_name") or "cv.pdf").strip()
    if not file_name.lower().endswith(".pdf"):
        file_name += ".pdf"

    cv = _normalize_cv(payload.get("cv") or {})

    # Find template file
    wanted = os.getenv("CV_TEMPLATE_NAME", "cv_europass.html")
    path = _find_template_file(wanted)
    if not path:
        # Helpful listing to debug
        listings = []
        for d in _template_search_dirs():
            try:
                entries = ", ".join(sorted(os.listdir(d)))
            except Exception as e:
                entries = f"(unreadable: {e})"
            listings.append(f"{d}: {entries}")
        return func.HttpResponse(
            "Template not found. "
            f"Tried '{wanted}' (case-insensitive) in:\n" + "\n".join(listings),
            status_code=500,
        )

    # Render HTML
    try:
        html = _render_html_from_file(cv, path)
    except TemplateError as e:
        ln = getattr(e, "lineno", "?")
        return func.HttpResponse(f"TemplateError (line {ln}): {e}", status_code=500)

    # HTML → PDF
    try:
        pdf_bytes = _html_to_pdf_bytes(html)
    except Exception as e:
        return func.HttpResponse(f"PDF render error: {e}", status_code=500)

    # Upload if configured; else return base64
    blob_name, url = _upload_pdf(file_name, pdf_bytes)
    resp = {"ok": True, "pdf_blob": blob_name or file_name}
    if url:
        resp["url"] = url
    else:
        resp["content_base64"] = base64.b64encode(pdf_bytes).decode("utf-8")

    return func.HttpResponse(json.dumps(resp), status_code=200, mimetype="application/json")
