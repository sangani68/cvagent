import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import azure.functions as func
from jinja2 import Environment, FileSystemLoader, select_autoescape
from azure.storage.blob import BlobServiceClient, ContentSettings
from playwright.sync_api import sync_playwright

TEMPLATES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "templates"))
DEFAULT_TEMPLATE = os.getenv("CV_TEMPLATE_NAME", "cv_europass.html")
PDF_OUT_CONT = os.getenv("PDF_OUT_CONTAINER", "pdf-out")
PRIMARY_HEX = os.getenv("PRIMARY_HEX", "#0F62FE")

def _render_html(cv: Dict[str, Any], template_name: Optional[str]) -> str:
    env = Environment(
        loader=FileSystemLoader([TEMPLATES_DIR, os.path.dirname(TEMPLATES_DIR)]),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True, lstrip_blocks=True
    )
    tpl = env.get_template(template_name or DEFAULT_TEMPLATE)
    # rgb for theming
    h = PRIMARY_HEX.lstrip("#")
    if len(h) == 3: h = "".join([c*2 for c in h])
    r = int(h[0:2],16); g=int(h[2:4],16); b=int(h[4:6],16)
    return tpl.render(cv=cv, theme={"primary_hex": PRIMARY_HEX, "primary_rgb": f"{r},{g},{b}"}, now=datetime.utcnow())

def _html_to_pdf(html: str) -> bytes:
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"], headless=True)
        page = browser.new_page()
        page.set_content(html, wait_until="load")
        pdf_bytes = page.pdf(format="A4", print_background=True, margin={"top":"12mm","bottom":"12mm","left":"10mm","right":"10mm"})
        browser.close()
        return pdf_bytes

def _upload_pdf(file_name: str, data: bytes) -> str:
    conn = os.getenv("AzureWebJobsStorage")
    bsc = BlobServiceClient.from_connection_string(conn)
    cc = bsc.get_container_client(PDF_OUT_CONT)
    try: cc.create_container()
    except Exception: pass
    cs = ContentSettings(content_type="application/pdf",
                         content_disposition=f'inline; filename="{file_name}"')
    cc.upload_blob(file_name, data, overwrite=True, content_settings=cs)
    # return short-lived blob SAS via website behind; or rely on cvagent to return url
    return f"blob://{PDF_OUT_CONT}/{file_name}"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    cv = body.get("cv")
    if not isinstance(cv, dict):
        return func.HttpResponse("Missing cv", status_code=400)

    file_name = (body.get("file_name") or "cv.pdf").strip() or "cv.pdf"
    template = body.get("template") or DEFAULT_TEMPLATE
    want = (body.get("return") or "").lower()  # "url" or "base64"

    try:
        html = _render_html(cv, template)
        pdf_bytes = _html_to_pdf(html)
    except Exception as e:
        logging.exception("render failed")
        return func.HttpResponse(f"PDF render error: {e}", status_code=500)

    if want == "url":
        # let cvagent produce SAS; here we just upload 
        _upload_pdf(file_name, pdf_bytes)
        return func.HttpResponse(json.dumps({"ok": True, "pdf_blob": file_name}), mimetype="application/json")

    # default: base64 inline
    import base64
    b64 = base64.b64encode(pdf_bytes).decode("ascii")
    return func.HttpResponse(json.dumps({"ok": True, "content_base64": b64}), mimetype="application/json")
