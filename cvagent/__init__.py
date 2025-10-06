import os
import json
import base64
import logging
from typing import Any, Dict, Optional

import azure.functions as func
import requests

# Optional: to stage PPTX into blob when using URL/base64
try:
    from azure.storage.blob import BlobServiceClient
except Exception:  # pragma: no cover
    BlobServiceClient = None


# =========================
# Env / config
# =========================

def _get_base_url() -> str:
    # Prefer explicit override for cross-app orchestration; else same app
    explicit = os.getenv("DOWNSTREAM_BASE_URL")
    if explicit:
        return explicit.rstrip("/")
    host = os.getenv("WEBSITE_HOSTNAME")  # e.g. cvfa-...azurewebsites.net
    if not host:
        raise RuntimeError("WEBSITE_HOSTNAME not set; set DOWNSTREAM_BASE_URL env to https://<app>.azurewebsites.net")
    return f"https://{host}"

# individual paths (can be overridden)
PPTXEXTRACT_PATH = os.getenv("PPTXEXTRACT_PATH", "/api/pptextract")
CVNORMALIZE_PATH = os.getenv("CVNORMALIZE_PATH", "/api/cvnormalize")
RENDER_PATH     = os.getenv("RENDER_PATH",     "/api/renderpdf_html")

# keys: prefer per-function keys, else fall back to HOST_KEY (host-level)
HOST_KEY          = os.getenv("HOST_KEY") or os.getenv("DOWNSTREAM_KEY")
PPTXEXTRACT_KEY   = os.getenv("PPTXEXTRACT_KEY", HOST_KEY)
CVNORMALIZE_KEY   = os.getenv("CVNORMALIZE_KEY", HOST_KEY)
RENDER_KEY        = os.getenv("RENDER_KEY", HOST_KEY)

COMING_CONTAINER  = os.getenv("COMING_CONTAINER", "incoming")  # where PPTX blob is / will be staged
TIMEOUT_EXTRACT   = int(os.getenv("TIMEOUT_EXTRACT", "180"))
TIMEOUT_NORMALIZE = int(os.getenv("TIMEOUT_NORMALIZE", "180"))
TIMEOUT_RENDER    = int(os.getenv("TIMEOUT_RENDER", "300"))


# =========================
# Utilities
# =========================

def _http_call(path: str, key: Optional[str], payload: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    """
    Calls another function in the same app (or external app) via HTTP POST.
    Adds x-functions-key if provided. Returns JSON, raises on HTTP error.
    """
    base = _get_base_url()
    url = f"{base}{path}"
    headers = {"Content-Type": "application/json"}
    if key:
        headers["x-functions-key"] = key
    logging.info("Calling %s", url)
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"Downstream error {r.status_code} calling {path}: {r.text[:800]}")
    try:
        return r.json()
    except Exception:
        # Some functions might return non-JSON (e.g. base64-only); wrap it
        return {"raw": r.text}


def _stage_blob_from_url_or_b64(pptx_url: Optional[str], pptx_b64: Optional[str], desired_name: Optional[str]) -> Optional[str]:
    """
    If pptx_blob not provided, and we have a URL or base64, upload to COMING_CONTAINER and return the blob name.
    Requires AzureWebJobsStorage and azure-storage-blob.
    """
    if not (pptx_url or pptx_b64):
        return None
    if not BlobServiceClient:
        raise RuntimeError("azure-storage-blob not available to stage PPTX. Provide 'pptx_blob' or install dependency.")

    conn = os.getenv("AzureWebJobsStorage")
    if not conn:
        raise RuntimeError("AzureWebJobsStorage not set; cannot stage PPTX into blob.")

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
        name = desired_name or os.path.basename(pptx_url.split("?")[0]) or "input.pptx"
    else:
        content = base64.b64decode(pptx_b64)
        name = desired_name or "input.pptx"

    cc.upload_blob(name=name, data=content, overwrite=True,
                   content_type="application/vnd.openxmlformats-officedocument.presentationml.presentation")
    logging.info("Staged PPTX to container '%s' as blob '%s'", COMING_CONTAINER, name)
    return name


def _extract_to_cv_or_text(extract_res: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize pptextract output for cvnormalize.
    If pptextract already returned a 'cv', pass it through.
    Else pass text-like content (slides_text/raw/text).
    """
    # If pptextract already did the heavy lifting
    if isinstance(extract_res.get("cv"), dict):
        return {"cv": extract_res["cv"]}

    # try common keys
    for k in ("slides_text", "raw", "text", "content"):
        if k in extract_res and extract_res[k]:
            return {"text": extract_res[k], "extracted": extract_res}

    # If unknown shape, pass the entire object under "extracted"
    return {"extracted": extract_res}


# =========================
# Azure Function
# =========================

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Orchestrator: pptextract -> cvnormalize -> renderpdf_html

    POST body supports:
      {
        "file_name": "cv.pdf",
        "pptx_blob": "mycv.pptx",           # blob name inside COMING_CONTAINER
        "pptx_url": "https://...",          # optional alternative
        "pptx_base64": "<...>",             # optional alternative
        "pptx_name": "input.pptx",          # optional name when staging url/base64
        "template": "cv_europass.html",     # optional; renderpdf_html respects CV_TEMPLATE_NAME too
        "return": "url"                     # optional hint; renderer may return url or base64
      }
    """
    if req.method != "POST":
        return func.HttpResponse("POST only", status_code=405)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    file_name = (body.get("file_name") or "cv.pdf").strip()
    template_name = body.get("template")  # None means renderer default
    want = (body.get("return") or "").lower()  # optional preference

    pptx_blob = body.get("pptx_blob")
    pptx_url = body.get("pptx_url")
    pptx_b64 = body.get("pptx_base64")
    pptx_name = body.get("pptx_name")

    # If no blob name provided, stage from URL / base64 (if given)
    if not pptx_blob:
        try:
            staged = _stage_blob_from_url_or_b64(pptx_url, pptx_b64, pptx_name)
        except Exception as e:
            return func.HttpResponse(f"Unable to stage PPTX: {e}", status_code=400)
        if staged:
            pptx_blob = staged
        else:
            return func.HttpResponse("Missing 'pptx_blob' (name inside COMING_CONTAINER), or provide 'pptx_url'/'pptx_base64'.", status_code=400)

    # 1) Extract from PPTX
    extract_payload = {
        "pptx_blob": pptx_blob,
        # pass both keys so pptextract can read either naming
        "container": COMING_CONTAINER,
        "coming_container": COMING_CONTAINER
    }
    try:
        extract_res = _http_call(PPTXEXTRACT_PATH, PPTXEXTRACT_KEY, extract_payload, TIMEOUT_EXTRACT)
    except Exception as e:
        logging.exception("pptextract failed")
        return func.HttpResponse(f"pptextract error: {e}", status_code=502)

    # 2) Normalize to CV JSON (skip if extract already returned 'cv')
    norm_input = _extract_to_cv_or_text(extract_res)
    if "cv" not in norm_input:
        try:
            norm_res = _http_call(CVNORMALIZE_PATH, CVNORMALIZE_KEY, norm_input, TIMEOUT_NORMALIZE)
        except Exception as e:
            logging.exception("cvnormalize failed")
            return func.HttpResponse(f"cvnormalize error: {e}", status_code=502)

        cv = norm_res.get("cv") or norm_res.get("normalized") or norm_res.get("result")
        if not isinstance(cv, dict):
            # fall back: if cvnormalize echoed text, construct minimal CV
            cv = {"personal_info": {}, "work_experience": [], "education": [], "skills_groups": [], "summary": norm_res}
    else:
        cv = norm_input["cv"]

    # 3) Render PDF
    render_payload = {"file_name": file_name, "cv": cv}
    if template_name:
        # our renderer inspects CV_TEMPLATE_NAME env, but also accepts 'template' in payload
        render_payload["template"] = template_name
    if want:
        render_payload["return"] = want  # some renderers accept this hint

    try:
        render_res = _http_call(RENDER_PATH, RENDER_KEY, render_payload, TIMEOUT_RENDER)
    except Exception as e:
        logging.exception("renderpdf_html failed")
        return func.HttpResponse(f"renderpdf_html error: {e}", status_code=502)

    # Bubble up renderer response as-is so callers can read {url|content_base64}
    return func.HttpResponse(json.dumps(render_res), status_code=200, mimetype="application/json")
