import os
import re
import json
import base64
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from urllib.parse import quote

import azure.functions as func
import requests

# Blob SDK (staging + SAS)
try:
    from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
except Exception:  # pragma: no cover
    BlobServiceClient = None
    generate_blob_sas = None
    BlobSasPermissions = None


# =============================================================================
# Configuration
# =============================================================================

def _base_url() -> str:
    override = os.getenv("DOWNSTREAM_BASE_URL")
    if override:
        return override.rstrip("/")
    host = os.getenv("WEBSITE_HOSTNAME")
    if not host:
        raise RuntimeError("WEBSITE_HOSTNAME not set; set DOWNSTREAM_BASE_URL=https://<app>.azurewebsites.net")
    return f"https://{host}"

PPTXEXTRACT_PATH = os.getenv("PPTXEXTRACT_PATH", "/api/pptxextract")  # note the 'x'
CVNORMALIZE_PATH = os.getenv("CVNORMALIZE_PATH", "/api/cvnormalize")
RENDER_PATH     = os.getenv("RENDER_PATH",     "/api/renderpdf_html")

HOST_KEY        = os.getenv("HOST_KEY") or os.getenv("DOWNSTREAM_KEY")
PPTXEXTRACT_KEY = os.getenv("PPTXEXTRACT_KEY", HOST_KEY)
CVNORMALIZE_KEY = os.getenv("CVNORMALIZE_KEY", HOST_KEY)
RENDER_KEY      = os.getenv("RENDER_KEY", HOST_KEY)

COMING_CONTAINER  = os.getenv("COMING_CONTAINER", "incoming")
TIMEOUT_EXTRACT   = int(os.getenv("TIMEOUT_EXTRACT", "180"))
TIMEOUT_NORMALIZE = int(os.getenv("TIMEOUT_NORMALIZE", "180"))
TIMEOUT_RENDER    = int(os.getenv("TIMEOUT_RENDER", "300"))


# =============================================================================
# HTTP helper
# =============================================================================

def _call(path: str, key: Optional[str], payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    url = f"{_base_url()}{path}"
    headers = {"Content-Type": "application/json"}
    if key:
        headers["x-functions-key"] = key
    logging.info("POST %s", url)
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"Downstream error {r.status_code} calling {path}: {r.text[:2000]}")
    try:
        return r.json()
    except Exception:
        return {"raw": r.text}


# =============================================================================
# Blob helpers (stage + SAS)
# =============================================================================

def _parse_conn_str(conn: str) -> Dict[str, str]:
    parts: Dict[str, str] = {}
    for seg in conn.split(";"):
        if "=" in seg:
            k, v = seg.split("=", 1)
            parts[k.strip()] = v.strip()
    return parts

def _blob_base_url_from_conn(conn: str, account_name: str) -> str:
    info = _parse_conn_str(conn)
    if "BlobEndpoint" in info and info["BlobEndpoint"]:
        return info["BlobEndpoint"].rstrip("/")
    suffix = info.get("EndpointSuffix", "core.windows.net")
    return f"https://{account_name}.blob.{suffix}"

def _require_blob_libs():
    if not (BlobServiceClient and generate_blob_sas and BlobSasPermissions):
        raise RuntimeError("azure-storage-blob not available; cannot stage PPTX or generate SAS.")

def _stage_to_incoming(pptx_url: Optional[str], pptx_b64: Optional[str], name_hint: Optional[str]) -> str:
    _require_blob_libs()
    conn = os.getenv("AzureWebJobsStorage")
    if not conn:
        raise RuntimeError("AzureWebJobsStorage not set.")
    bsc = BlobServiceClient.from_connection_string(conn)
    cc = bsc.get_container_client(COMING_CONTAINER)
    try:
        cc.create_container()
    except Exception:
        pass

    if pptx_url:
        resp = requests.get(pptx_url, timeout=120)
        resp.raise_for_status()
        content = resp.content
        filename = name_hint or os.path.basename(pptx_url.split("?", 1)[0]) or "input.pptx"
    elif pptx_b64:
        content = base64.b64decode(pptx_b64)
        filename = name_hint or "input.pptx"
    else:
        raise RuntimeError("No pptx_url or pptx_base64 provided to stage.")

    cc.upload_blob(
        name=filename,
        data=content,
        overwrite=True,
        content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
    )
    logging.info("Staged PPTX to '%s/%s'", COMING_CONTAINER, filename)
    return filename

def _sas_url_for_blob(container: str, blob_name: str, ttl_minutes: int = 120) -> str:
    _require_blob_libs()
    conn = os.getenv("AzureWebJobsStorage")
    if not conn:
        raise RuntimeError("AzureWebJobsStorage not set.")
    info = _parse_conn_str(conn)
    account = info.get("AccountName")
    key = info.get("AccountKey")
    if not (account and key):
        raise RuntimeError("AccountName/AccountKey missing in AzureWebJobsStorage; cannot build SAS.")
    base_url = _blob_base_url_from_conn(conn, account)

    sas = generate_blob_sas(
        account_name=account,
        container_name=container,
        blob_name=blob_name,
        account_key=key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=ttl_minutes),
    )
    return f"{base_url}/{container}/{quote(blob_name)}?{sas}"


# =============================================================================
# Azure Function (orchestrator)
# =============================================================================

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Orchestrates: pptxextract (needs 'ppt_blob_sas') -> cvnormalize -> renderpdf_html

    Body:
      - pptx_blob | pptx_url | pptx_base64 (+ pptx_name)
      - file_name (optional; defaults to <source>.pdf)
      - template (optional), return (optional)
      - mode: 'normalize_only' to return {cv} without rendering
    """
    if req.method != "POST":
        return func.HttpResponse("POST only", status_code=405)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # ---- inputs
    file_name     = (body.get("file_name") or "").strip()
    template_name = body.get("template")
    want          = (body.get("return") or "").lower()
    mode          = (body.get("mode") or "").lower()

    pptx_blob = body.get("pptx_blob")
    pptx_url  = body.get("pptx_url")
    pptx_b64  = body.get("pptx_base64")
    pptx_name = body.get("pptx_name")

    # ---- derive default PDF name from source if client didn't provide one
    def to_pdf_name(n: str) -> str:
        return re.sub(r"\.(pptx|pptm|ppt|ppsx|potx|potm|odp)$", "", n, flags=re.I).rstrip(".") + ".pdf"

    src_name = pptx_name or pptx_blob
    if not src_name and pptx_url:
        src_name = os.path.basename(pptx_url.split("?", 1)[0]) or "cv.pptx"
    if not file_name:
        file_name = to_pdf_name(src_name or "cv.pptx")

    # ---- ensure blob exists (stage if needed)
    if not pptx_blob:
        try:
            pptx_blob = _stage_to_incoming(pptx_url, pptx_b64, pptx_name)
        except Exception as e:
            return func.HttpResponse(f"Unable to stage PPTX: {e}", status_code=400)

    # ---- SAS for extractor
    try:
        sas_url = _sas_url_for_blob(COMING_CONTAINER, pptx_blob, ttl_minutes=120)
    except Exception as e:
        return func.HttpResponse(f"Could not create SAS for PPTX: {e}", status_code=400)

    # 1) Extract
    extract_payload = {
        "ppt_blob_sas": sas_url,
        "pptx_blob": pptx_blob,
        "container": COMING_CONTAINER,
        "coming_container": COMING_CONTAINER,
    }
    try:
        extract_res = _call(PPTXEXTRACT_PATH, PPTXEXTRACT_KEY, extract_payload, TIMEOUT_EXTRACT)
    except Exception as e:
        logging.exception("pptxextract failed")
        return func.HttpResponse(f"pptxextract error: {e}", status_code=502)

    # 2) Normalize (or accept CV)
    if isinstance(extract_res.get("cv"), dict):
        cv = extract_res["cv"]
    else:
        text = None
        for k in ("slides_text", "raw", "text", "content"):
            if extract_res.get(k):
                text = extract_res[k]
                break
        if text is None:
            return func.HttpResponse(
                f"pptxextract returned no text/cv. Keys: {list(extract_res.keys())}",
                status_code=502,
            )
        try:
            norm_res = _call(CVNORMALIZE_PATH, CVNORMALIZE_KEY, {"text": text}, TIMEOUT_NORMALIZE)
        except Exception as e:
            logging.exception("cvnormalize failed")
            return func.HttpResponse(f"cvnormalize error: {e}", status_code=502)
        cv = norm_res.get("cv") or norm_res.get("normalized") or norm_res

    # --- normalize-only short-circuit
    if mode in ("normalize_only", "extract_only", "cv_only"):
        return func.HttpResponse(json.dumps({"cv": cv}), status_code=200, mimetype="application/json")

    # 3) Render
    render_payload: Dict[str, Any] = {"file_name": file_name, "cv": cv}
    if template_name:
        render_payload["template"] = template_name
    if want:
        render_payload["return"] = want

    try:
        render_res = _call(RENDER_PATH, RENDER_KEY, render_payload, TIMEOUT_RENDER)
    except Exception as e:
        logging.exception("renderpdf_html failed")
        return func.HttpResponse(f"renderpdf_html error: {e}", status_code=502)

    return func.HttpResponse(json.dumps(render_res), status_code=200, mimetype="application/json")
