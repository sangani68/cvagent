import json, os, io, base64, tempfile
import azure.functions as func
from jinja2 import Environment, FileSystemLoader, select_autoescape
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from playwright.sync_api import sync_playwright

# ---- Config from env ----
STORAGE_CONN = os.getenv("AzureWebJobsStorage")  # or a dedicated connection string
PDF_OUT_CONTAINER = os.getenv("PDF_OUT_CONTAINER", "pdf-out")
TEMPLATES_CONTAINER = os.getenv("TEMPLATES_CONTAINER", "templates")
LOCAL_TEMPLATE_DIR = os.getenv("LOCAL_TEMPLATE_DIR", "/site/wwwroot/templates")
TEMPLATE_NAME = os.getenv("CV_TEMPLATE_NAME", "cv_template.html")
PDF_PRIMARY_RGB = os.getenv("PDF_PRIMARY_RGB", "")  # e.g. "15,98,254"
PDF_FILE_PREFIX = os.getenv("PDF_FILE_PREFIX", "CV_")

# Optional: use Blob template first, fallback to local
PREFER_BLOB_TEMPLATE = os.getenv("PREFER_BLOB_TEMPLATE", "false").lower() == "true"

def _accent_to_css(rgb_str: str) -> str:
    try:
        if not rgb_str: return None
        r,g,b = [int(x.strip()) for x in rgb_str.split(",")]
        return f"rgb({r},{g},{b})"
    except Exception:
        return None

def _load_template_from_blob(bsc) -> str | None:
    try:
        blob = bsc.get_blob_client(container=TEMPLATES_CONTAINER, blob=TEMPLATE_NAME)
        data = blob.download_blob().readall()
        return data.decode("utf-8")
    except Exception:
        return None

def _ensure_playwright_installed():
    # On first cold start, ensure chromium is present. Safe no-op if already installed.
    try:
        import shutil, subprocess
        if shutil.which("playwright") is None:
            return
        # check if chromium is installed by looking for .local-browsers marker
        browsers_dir = os.path.expanduser("~/.cache/ms-playwright")
        if not os.path.exists(browsers_dir) or not any("chromium" in d for d in os.listdir(browsers_dir)):
            subprocess.run(["playwright", "install", "chromium"], check=True)
    except Exception:
        # don't crash if install check fails; Playwright may still work
        pass

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except Exception:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Expected: normalized CV JSON from cvnormalize
    cv = payload.get("cv") or payload
    if not isinstance(cv, dict):
        return func.HttpResponse("Missing 'cv' object with normalized data", status_code=400)

    file_name = payload.get("file_name") or f"{PDF_FILE_PREFIX}{cv.get('personal_info', {}).get('full_name','CV')}.pdf"
    file_name = file_name.replace("/", "_").replace("\\", "_")

    # Accent color
    accent_css = _accent_to_css(PDF_PRIMARY_RGB) or None

    # Prepare template
    bsc = BlobServiceClient.from_connection_string(STORAGE_CONN)
    html_string = None
    if PREFER_BLOB_TEMPLATE:
        html_string = _load_template_from_blob(bsc)

    env = Environment(
        loader=FileSystemLoader(LOCAL_TEMPLATE_DIR),
        autoescape=select_autoescape(["html", "xml"])
    )

    if html_string:
        template = env.from_string(html_string)
    else:
        try:
            template = env.get_template(TEMPLATE_NAME)
        except Exception as e:
            return func.HttpResponse(f"Template not found: {TEMPLATE_NAME} ({e})", status_code=500)

    # Render HTML with Jinja2
    try:
        html = template.render(accent_css=accent_css, **cv)
    except Exception as e:
        return func.HttpResponse(f"Template render error: {e}", status_code=500)

    # Ensure Playwright browser
    _ensure_playwright_installed()

    # Generate PDF with Playwright
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=[
                "--no-sandbox",
                "--font-render-hinting=none",
                "--disable-gpu",
                "--disable-dev-shm-usage"
            ])
            page = browser.new_page()
            # Use set_content so we don't rely on network access
            page.set_content(html, wait_until="load")
            pdf_bytes = page.pdf(
                format="A4",
                print_background=True,
                margin={"top":"0.5in","right":"0.45in","bottom":"0.5in","left":"0.45in"}
            )
            browser.close()
    except Exception as e:
        return func.HttpResponse(f"Playwright PDF error: {e}", status_code=500)

    # Upload to Blob
    try:
        blob = bsc.get_blob_client(container=PDF_OUT_CONTAINER, blob=file_name)
        blob.upload_blob(pdf_bytes, overwrite=True, content_type="application/pdf")

        # If you already have a SAS base (PDF_OUT_BASE) you can return a composed URL
        sas_base = os.getenv("PDF_OUT_BASE")  # e.g., "https://<acct>.blob.core.windows.net/pdf-out?<SAS>"
        if sas_base:
            # For containers SAS, just append /{blob}
            if "?" in sas_base:
                base, qs = sas_base.split("?", 1)
                url = f"{base.rstrip('/')}/{file_name}?{qs}"
            else:
                url = f"{sas_base.rstrip('/')}/{file_name}"
        else:
            # fallback: no SAS; return blob path only
            url = f"{PDF_OUT_CONTAINER}/{file_name}"

        return func.HttpResponse(
            json.dumps({"pdf_blob": file_name, "url": url}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Blob upload error: {e}", status_code=500)
