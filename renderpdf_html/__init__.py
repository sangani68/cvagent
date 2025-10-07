# renderpdf_html/__init__.py
import json, os, io, tempfile, traceback
from datetime import datetime, timedelta
import azure.functions as func

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from playwright.sync_api import sync_playwright

# ---- Config (keeps your conventions) ----
ACCOUNT_URL     = os.environ.get("AZURE_STORAGE_BLOB_URL") or os.environ.get("BLOB_ACCOUNT_URL")
CONN_STR        = os.environ.get("AzureWebJobsStorage") or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
PDF_CONTAINER   = os.environ.get("PDF_CONTAINER", "pdf-out")
# optional: control filename color/brand via your existing setting
PRIMARY_RGB     = os.environ.get("PDF_PRIMARY_RGB", "0,102,204")

# SAS expiry in minutes for returned link
SAS_MINUTES     = int(os.environ.get("SAS_MINUTES", "120"))

def _blob_client(container: str, blob_name: str):
    bsc = BlobServiceClient.from_connection_string(CONN_STR)
    return bsc.get_blob_client(container=container, blob=blob_name)

def _ensure_container(container: str):
    try:
        bsc = BlobServiceClient.from_connection_string(CONN_STR)
        bsc.create_container(container)
    except Exception:
        pass

def _make_sas(container: str, blob_name: str, minutes: int = SAS_MINUTES) -> str:
    # Build SAS with read perms
    from azure.storage.blob import generate_blob_sas, BlobSasPermissions
    from datetime import datetime, timezone
    account_name = os.environ.get("STORAGE_ACCOUNT_NAME")
    account_key  = os.environ.get("STORAGE_ACCOUNT_KEY")
    # If you don't expose name/key as app settings, SAS is optional; the raw URL still works if container is public.
    if not (account_name and account_key):
        # Fall back to regular URL (no SAS)
        base = ACCOUNT_URL or ""
        return f"{base}/{container}/{blob_name}"

    sas = generate_blob_sas(
        account_name=account_name,
        container_name=container,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=minutes),
    )
    base = ACCOUNT_URL or f"https://{account_name}.blob.core.windows.net"
    return f"{base}/{container}/{blob_name}?{sas}"

def _render_pdf_bytes(html: str, css: str = "") -> bytes:
    # Write temp HTML; keep CSS inline to avoid file path shenanigans on Functions.
    with tempfile.TemporaryDirectory() as td:
        html_path = os.path.join(td, "cv.html")
        with open(html_path, "w", encoding="utf-8") as f:
            if css and css.strip():
                f.write(f"<style>{css}</style>\n")
            # Optionally expose theme color to CSS if you like
            f.write(f'<!-- primary={PRIMARY_RGB} -->\n')
            f.write(html)

        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-gpu",
                        "--single-process",
                        "--no-zygote",
                    ],
                )
            except Exception as e:
                raise RuntimeError(f"Playwright launch failed: {e}")

            page = browser.new_page()
            page.goto(f"file://{html_path}")
            # Wait for fonts/images/custom elements to settle
            page.wait_for_load_state("networkidle")

            pdf_bytes = page.pdf(
                format="A4",
                print_background=True,
                margin={"top": "10mm", "bottom": "12mm", "left": "10mm", "right": "10mm"},
            )
            browser.close()
            return pdf_bytes

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except Exception:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON body"}), mimetype="application/json", status_code=400
        )

    html       = payload.get("html")
    css        = payload.get("css", "")
    out_name   = payload.get("out_name")  # prefer UI to send the source-pptx-matching name
    container  = payload.get("container") or PDF_CONTAINER

    if not html:
        return func.HttpResponse(
            json.dumps({"error": "Missing 'html' in request body"}), mimetype="application/json", status_code=400
        )

    # Default filename if UI didn't send one
    if not out_name:
        out_name = f"cv-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.pdf"
    if not out_name.lower().endswith(".pdf"):
        out_name += ".pdf"

    try:
        pdf_bytes = _render_pdf_bytes(html, css)
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"HTML->PDF failed: {str(e)}", "trace": traceback.format_exc()}),
            mimetype="application/json",
            status_code=500,
        )

    try:
        _ensure_container(container)
        bc = _blob_client(container, out_name)
        bc.upload_blob(
            io.BytesIO(pdf_bytes),
            overwrite=True,
            content_settings=ContentSettings(content_type="application/pdf"),
        )
        sas_url = _make_sas(container, out_name)
        return func.HttpResponse(
            json.dumps({"ok": True, "pdf_url": sas_url, "blob": {"container": container, "name": out_name}}),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": f"Upload failed: {str(e)}", "trace": traceback.format_exc()}),
            mimetype="application/json",
            status_code=500,
        )
