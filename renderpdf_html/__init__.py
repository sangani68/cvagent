import os, json, logging, base64
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import azure.functions as func
from jinja2 import Environment, FileSystemLoader, select_autoescape
from playwright.sync_api import sync_playwright
from azure.storage.blob import BlobServiceClient, ContentSettings, generate_blob_sas, BlobSasPermissions

TEMPLATES_DIRS = [
    os.path.abspath(os.path.join(os.path.dirname(__file__), "templates")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "templates")),
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
]
DEFAULT_TEMPLATE = os.getenv("CV_TEMPLATE_NAME", "cv_europass.html")
PDF_OUT_CONTAINER = os.getenv("PDF_OUT_CONTAINER", "pdf-out")
PRIMARY_HEX = os.getenv("PRIMARY_HEX", "#0F62FE")

def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(TEMPLATES_DIRS),
        autoescape=select_autoescape(["html","xml"]),
        trim_blocks=True, lstrip_blocks=True
    )

def _theme():
    h = PRIMARY_HEX.lstrip("#")
    if len(h)==3: h = "".join([c*2 for c in h])
    r,g,b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
    return {"primary_hex": PRIMARY_HEX, "primary_rgb": f"{r},{g},{b}"}

def _render_html(cv: Dict[str, Any], template_name: Optional[str]) -> str:
    tpl = _env().get_template(template_name or DEFAULT_TEMPLATE)
    return tpl.render(cv=cv, theme=_theme(), now=datetime.utcnow())

def _html_to_pdf(html: str) -> bytes:
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"], headless=True)
        page = browser.new_page()
        page.set_content(html, wait_until="load")
        pdf = page.pdf(format="A4", print_background=True,
                       margin={"top":"12mm","bottom":"12mm","left":"10mm","right":"10mm"})
        browser.close()
        return pdf

def _sas_url(blob_name: str, minutes: int = 1440) -> str:
    conn = os.getenv("AzureWebJobsStorage")
    bsc = BlobServiceClient.from_connection_string(conn)
    cc = bsc.get_container_client(PDF_OUT_CONTAINER)
    try: cc.create_container()
    except Exception: pass

    cs = ContentSettings(content_type="application/pdf",
                         content_disposition=f'inline; filename="{os.path.basename(blob_name)}"')
    cc.upload_blob(name=blob_name, data=b"", overwrite=True, content_settings=cs)  # ensure exists before overwrite
    cc.upload_blob(name=blob_name, data=None, overwrite=True)  # noop to keep metadata if already uploaded

    # upload final bytes happens outside; this just ensures container exists

    # build SAS
    from azure.storage.blob import generate_blob_sas, BlobSasPermissions
    parts = {}
    for seg in os.getenv("AzureWebJobsStorage","").split(";"):
        if "=" in seg:
            k,v = seg.split("=",1); parts[k]=v
    account = parts.get("AccountName"); key = parts.get("AccountKey")
    suffix = parts.get("EndpointSuffix","core.windows.net")
    base = f"https://{account}.blob.{suffix}"
    sas = generate_blob_sas(account_name=account, container_name=PDF_OUT_CONTAINER,
                            blob_name=blob_name, account_key=key,
                            permission=BlobSasPermissions(read=True),
                            expiry=datetime.utcnow()+timedelta(minutes=minutes))
    return f"{base}/{PDF_OUT_CONTAINER}/{blob_name}?{sas}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try: body = req.get_json()
    except ValueError: return func.HttpResponse("Invalid JSON", status_code=400)

    cv = body.get("cv")
    if not isinstance(cv, dict): return func.HttpResponse("Missing cv", status_code=400)
    file_name = (body.get("file_name") or "cv.pdf").strip() or "cv.pdf"
    template = body.get("template") or DEFAULT_TEMPLATE
    want = (body.get("return") or "url").lower()

    try:
        html = _render_html(cv, template)
        pdf_bytes = _html_to_pdf(html)
    except Exception as e:
        logging.exception("render failed")
        return func.HttpResponse(f"PDF render error: {e}", status_code=500)

    conn = os.getenv("AzureWebJobsStorage")
    bsc = BlobServiceClient.from_connection_string(conn)
    cc = bsc.get_container_client(PDF_OUT_CONTAINER)
    try: cc.create_container()
    except Exception: pass
    cs = ContentSettings(content_type="application/pdf",
                         content_disposition=f'inline; filename="{file_name}"')
    cc.upload_blob(file_name, pdf_bytes, overwrite=True, content_settings=cs)

    url = _sas_url(file_name, minutes=int(os.getenv("PDF_SAS_TTL_MIN", "1440")))

    if want == "base64":
        b64 = base64.b64encode(pdf_bytes).decode("ascii")
        return func.HttpResponse(json.dumps({"ok": True, "pdf_blob": file_name, "content_base64": b64}),
                                 mimetype="application/json")

    return func.HttpResponse(json.dumps({"ok": True, "pdf_blob": file_name, "url": url}),
                             mimetype="application/json")
