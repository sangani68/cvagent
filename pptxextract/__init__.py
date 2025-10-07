import io
import os
import json
import logging
from typing import Any, Dict, List, Tuple, Optional

import requests
import azure.functions as func
from pptx import Presentation

def _download_pptx(ppt_blob_sas: str) -> bytes:
    r = requests.get(ppt_blob_sas, timeout=120)
    r.raise_for_status()
    return r.content

def _extract_slide(slide) -> Tuple[str, List[str]]:
    """Return (title, bullets/lines) for a slide."""
    title = ""
    lines: List[str] = []
    # Title placeholder if present
    if getattr(slide, "shapes", None):
        for shape in slide.shapes:
            if not hasattr(shape, "has_text_frame") or not shape.has_text_frame:
                continue
            text = shape.text or ""
            if not text.strip():
                continue
            # crude title detection: placeholder title or first large text
            try:
                if getattr(shape, "is_placeholder", False) and getattr(shape.placeholder_format, "type", None) == 1:
                    title = text.strip()
                    continue
            except Exception:
                pass
            # gather paragraph runs as bullet lines
            for p in shape.text_frame.paragraphs:
                t = "".join(run.text for run in p.runs).strip()
                if t:
                    lines.append(t)
    # notes (optional)
    try:
        if getattr(slide, "notes_slide", None) and slide.notes_slide and slide.notes_slide.notes_text_frame:
            note_text = (slide.notes_slide.notes_text_frame.text or "").strip()
            if note_text:
                lines.append(f"(Notes) {note_text}")
    except Exception:
        pass
    return title, lines

def _slides_to_text(slides: List[Dict[str, Any]]) -> str:
    out: List[str] = []
    for i, s in enumerate(slides, 1):
        t = s.get("title")
        if t: out.append(f"# {t}")
        for b in s.get("bullets", []):
            out.append(f"- {b}")
        if s.get("notes"):
            out.append(f"Notes: {s['notes']}")
        out.append("")  # blank line
    return "\n".join(out).strip()

def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method != "POST":
        return func.HttpResponse("POST only", status_code=405)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    ppt_sas = body.get("ppt_blob_sas")
    if not ppt_sas:
        return func.HttpResponse("Missing 'ppt_blob_sas'", status_code=400)

    try:
        blob = _download_pptx(ppt_sas)
        prs = Presentation(io.BytesIO(blob))
    except Exception as e:
        logging.exception("Failed to open PPTX")
        return func.HttpResponse(f"Could not read PPTX: {e}", status_code=400)

    slides: List[Dict[str, Any]] = []
    for slide in prs.slides:
        title, bullets = _extract_slide(slide)
        slides.append({
            "title": title,
            "bullets": bullets
        })

    slides_text = _slides_to_text(slides)
    # You can also return 'raw' duplicating slides_text for downstream compatibility
    return func.HttpResponse(
        json.dumps({
            "ok": True,
            "slides": slides,
            "slides_text": slides_text,
            "raw": slides_text
        }),
        status_code=200, mimetype="application/json"
    )
