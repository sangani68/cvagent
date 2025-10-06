# /cvagent/__init__.py
import os
import json
import logging
from typing import Any, Dict, Optional

import azure.functions as func
import requests


# --------- ENV / CONFIG -----------
FUNCS_BASE_URL = os.getenv("FUNCS_BASE_URL", "").rstrip("/")
FUNCS_KEY      = os.getenv("FUNCS_KEY", "")
TIMEOUT_S      = int(os.getenv("CVAGENT_HTTP_TIMEOUT", "180"))

COMING_CONTAINER        = os.getenv("COMING_CONTAINER", "incoming")
JSON_PARSED_CONTAINER   = os.getenv("JSON_PARSED_CONTAINER", "json-parsed")
PDF_OUT_CONTAINER       = os.getenv("PDF_OUT_CONTAINER", "pdf-out")

# Optional SAS bases for composing direct download URLs
STORAGE_JSON_BASE = os.getenv("STORAGE_JSON_BASE", "").rstrip("/")
PDF_OUT_BASE      = os.getenv("PDF_OUT_BASE", "").rstrip("/")


# --------- HELPERS ----------
class StepError(Exception):
    pass


def _assert_config():
    missing = []
    if not FUNCS_BASE_URL: missing.append("FUNCS_BASE_URL")
    if not FUNCS_KEY:      missing.append("FUNCS_KEY")
    if missing:
        raise StepError(f"Missing app settings: {', '.join(missing)}")


def _call_func(path: str, payload: Dict[str, Any], timeout: int = TIMEOUT_S) -> Dict[str, Any]:
    """
    POST to an internal function endpoint with the shared function key.
    Raises StepError with a readable message on errors.
    """
    url = f"{FUNCS_BASE_URL}{path}"
    try:
        r = requests.post(
            f"{url}?code={FUNCS_KEY}",
            json=payload,
            timeout=timeout,
        )
    except requests.RequestException as e:
        raise StepError(f"HTTP error calling {path}: {e}")

    if r.status_code >= 400:
        # Try to surface JSON error if present
        try:
            err = r.json()
        except Exception:
            err = r.text
        raise StepError(f"{path} failed {r.status_code}: {err}")

    try:
        return r.json()
    except Exception:
        # If child returned plain text, wrap it
        return {"_raw_text": r.text}


def _compose_blob_url(base: str, blob_name: str) -> Optional[str]:
    """
    If `base` is a container SAS (or public container URL), append blob.
    Accepts:
      - https://acct.blob.core.windows.net/container?sv=...  (container SAS)
      - https://acct.blob.core.windows.net/container          (public)
    """
    if not base:
        return None
    if "?" in base:
        # container SAS
        pre, qs = base.split("?", 1)
        return f"{pre.rstrip('/')}/{blob_name}?{qs}"
    return f"{base.rstrip('/')}/{blob_name}"


def _first_non_empty(*vals):
    for v in vals:
        if v not in (None, "", []):
            return v
    return None


# --------- MAIN ORCHESTRATION ----------
def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Request body (JSON):
      {
        "pptx_blob": "somefile.pptx",          # REQUIRED, inside COMING_CONTAINER
        "out_pdf_name": "CV_Foo.pdf",          # OPTIONAL (default derived from name)
        "proofread": true,                     # OPTIONAL, passed to cvnormalize
        "keep_raw": true,                      # OPTIONAL, let pptxextract persist raw JSON to blob
        "keep_normalized": true,               # OPTIONAL, let cvnormalize persist normalized JSON
        "return_urls_only": true,              # OPTIONAL
        "cv_template_name": "cv_template.html" # NOTE: renderer uses env CV_TEMPLATE_NAME; keep here for trace
      }

    Response (JSON):
      {
        "ok": true,
        "steps": {
          "pptxextract": {...},
          "cvnormalize": {...},
          "renderpdf_html": {...}
        },
        "artifacts": {
          "raw_json_blob": "...",
          "raw_json_url": "...",
          "normalized_blob": "...",
          "normalized_url": "...",
          "pdf_blob": "...",
          "pdf_url": "..."
        }
      }
    """
    logging.info("cvagent: request received")
    try:
        _assert_config()

        try:
            body = req.get_json()
        except Exception:
            return func.HttpResponse("Invalid JSON body", status_code=400)

        pptx_blob       = body.get("pptx_blob")
        if not pptx_blob:
            return func.HttpResponse("Missing 'pptx_blob' (name inside COMING_CONTAINER).", status_code=400)

        out_pdf_name    = body.get("out_pdf_name") or os.path.splitext(os.path.basename(pptx_blob))[0] + ".pdf"
        proofread       = bool(body.get("proofread", False))
        keep_raw        = bool(body.get("keep_raw", True))
        keep_normalized = bool(body.get("keep_normalized", True))
        return_urls_only= bool(body.get("return_urls_only", False))
        cv_template_name= body.get("cv_template_name")  # informational; renderer reads env

        steps: Dict[str, Any] = {}
        artifacts: Dict[str, Optional[str]] = {
            "raw_json_blob": None,
            "raw_json_url": None,
            "normalized_blob": None,
            "normalized_url": None,
            "pdf_blob": None,
            "pdf_url": None
        }

        # ---- STEP 1: EXTRACT PPTX ----
        logging.info("cvagent: calling /api/pptxextract")
        extract_payload = {
            "container": COMING_CONTAINER,
            "blob": pptx_blob,
            # prefer both: keep to blob, and also return inline to reduce hops
            "keep_raw": keep_raw,
            "return_inline": True
        }
        ext = _call_func("/api/pptxextract", extract_payload)
        steps["pptxextract"] = ext

        # Retrieve raw JSON (inline or blob), and optional URLs
        raw_inline = ext.get("raw") or ext.get("raw_json")
        raw_blob   = _first_non_empty(ext.get("raw_blob"), ext.get("raw_json_blob"))
        raw_url    = _first_non_empty(
            ext.get("url"), ext.get("raw_url"),
            _compose_blob_url(STORAGE_JSON_BASE, raw_blob) if raw_blob else None
        )
        artifacts["raw_json_blob"] = raw_blob
        artifacts["raw_json_url"]  = raw_url

        if not (raw_inline or raw_blob or raw_url):
            raise StepError("pptxextract returned no raw JSON (neither inline nor blob/url).")

        # ---- STEP 2: NORMALIZE ----
        logging.info("cvagent: calling /api/cvnormalize")
        norm_payload: Dict[str, Any] = {
            "proofread": proofread,
            "keep_normalized": keep_normalized
        }
        # Pass whichever raw we have
        if raw_inline:
            norm_payload["raw"] = raw_inline
        if raw_blob:
            norm_payload["raw_blob"] = raw_blob
            norm_payload["raw_container"] = JSON_PARSED_CONTAINER  # if extractor wrote raw there
        if raw_url:
            norm_payload["raw_url"] = raw_url

        norm = _call_func("/api/cvnormalize", norm_payload)
        steps["cvnormalize"] = norm

        # Retrieve normalized CV (inline or blob/url)
        cv_inline  = _first_non_empty(norm.get("cv"), norm.get("normalized"))
        cv_blob    = _first_non_empty(norm.get("cv_blob"), norm.get("normalized_blob"))
        cv_url     = _first_non_empty(
            norm.get("url"), norm.get("normalized_url"),
            _compose_blob_url(STORAGE_JSON_BASE, cv_blob) if cv_blob else None
        )
        artifacts["normalized_blob"] = cv_blob
        artifacts["normalized_url"]  = cv_url

        if not (cv_inline or cv_blob or cv_url):
            raise StepError("cvnormalize returned no normalized CV (neither inline nor blob/url).")

        # ---- STEP 3: RENDER (HTML/CSS → PDF with Playwright) ----
        logging.info("cvagent: calling /api/renderpdf_html")
        render_payload: Dict[str, Any] = {
            "file_name": out_pdf_name
        }
        # Prefer inline CV if available (fastest path)
        if cv_inline:
            render_payload["cv"] = cv_inline
        else:
            # If no inline CV, try to let the renderer fetch it if your renderer supports it.
            # (The provided renderpdf_html expects inline 'cv', so safest is to fetch normalized JSON now.)
            # If cv_url exists, pull it, else if cv_blob exists + JSON SAS base, pull via composed URL.
            cv_src_url = cv_url or (_compose_blob_url(STORAGE_JSON_BASE, cv_blob) if cv_blob else None)
            if not cv_src_url:
                raise StepError("Renderer requires inline 'cv'; no accessible normalized JSON URL available.")
            try:
                rj = requests.get(cv_src_url, timeout=30)
                rj.raise_for_status()
                render_payload["cv"] = rj.json()
            except Exception as e:
                raise StepError(f"Failed to download normalized JSON for renderer: {e}")

        rnd = _call_func("/api/renderpdf_html", render_payload)
        steps["renderpdf_html"] = rnd

        pdf_blob = rnd.get("pdf_blob") or out_pdf_name
        pdf_url  = rnd.get("url") or _compose_blob_url(PDF_OUT_BASE, pdf_blob)
        artifacts["pdf_blob"] = pdf_blob
        artifacts["pdf_url"]  = pdf_url

        result = {
            "ok": True,
            "steps": steps if not return_urls_only else {},
            "artifacts": artifacts
        }
        return func.HttpResponse(
            json.dumps(result, ensure_ascii=False, indent=None),
            status_code=200,
            mimetype="application/json"
        )

    except StepError as e:
        logging.exception("cvagent: step error")
        return func.HttpResponse(json.dumps({"ok": False, "error": str(e)}), status_code=502, mimetype="application/json")
    except Exception as e:
        logging.exception("cvagent: fatal error")
        return func.HttpResponse(json.dumps({"ok": False, "error": f"unexpected: {e}"}), status_code=500, mimetype="application/json")
