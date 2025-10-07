import os
import json
import logging
from typing import Any, Dict, Optional

import azure.functions as func
from openai import AzureOpenAI

# ---------- env helpers ----------
def _get(name: str, *aliases: str, default: Optional[str] = None) -> Optional[str]:
    for k in (name, *aliases):
        v = os.getenv(k)
        if v:
            return v
    return default

# Support your AOAI_* names (and AZURE_OPENAI_* aliases)
AOAI_ENDPOINT     = _get("AOAI_ENDPOINT", "AZURE_OPENAI_ENDPOINT")
AOAI_KEY          = _get("AOAI_KEY", "AZURE_OPENAI_API_KEY")
AOAI_DEPLOYMENT   = _get("AOAI_DEPLOYMENT", "AZURE_OPENAI_DEPLOYMENT", default="gpt-4.1")
AOAI_API_VERSION  = _get("AOAI_API_VERSION", "AZURE_OPENAI_API_VERSION", default="2024-10-21")

_client: Optional[AzureOpenAI] = None
def client() -> AzureOpenAI:
    global _client
    if _client is None:
        _client = AzureOpenAI(azure_endpoint=AOAI_ENDPOINT, api_key=AOAI_KEY, api_version=AOAI_API_VERSION)
    return _client

# ---------- CV schema (optional fields allowed) ----------
CV_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "personal_info": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "full_name": {"type": "string"},
                "headline": {"type": "string"},
                "address": {"type": "string"},
                "city": {"type": "string"},
                "country": {"type": "string"},
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "website": {"type": "string"},
                "linkedin": {"type": "string"},
                "nationality": {"type": "string"},
                "date_of_birth": {"type": "string"},
                "gender": {"type": "string"},
                "summary": {"type": "string"}
            },
            "required": ["full_name"]
        },
        "summary": {"type": "string"},
        "skills_groups": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "group": {"type": "string"},
                    "items": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["group", "items"]
            }
        },
        "work_experience": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "title": {"type": "string"},
                    "company": {"type": "string"},
                    "location": {"type": "string"},
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "description": {"type": "string"},
                    "bullets": {"type": "array", "items": {"type": "string"}}
                },
                "required": ["title", "company"]
            }
        },
        "education": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "degree": {"type": "string"},
                    "title": {"type": "string"},
                    "institution": {"type": "string"},
                    "location": {"type": "string"},
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                    "details": {"type": "string"}
                },
                "required": ["institution"]
            }
        },
        "languages": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {"type": "string"},
                    "level": {"type": "string"}
                },
                "required": ["name"]
            }
        },
        "provenance": {
            "type": "object",
            "additionalProperties": True
        }
    },
    "required": ["personal_info"]
}

SYSTEM_PROMPT = """You are an expert CV normalizer.
You receive noisy content extracted from a PPTX (text + blocks + hints).
Your goal: return a clean JSON CV that matches the schema. Rules:
- Use ALL relevant content from the provided text and blocks. Do not drop information.
- Proofread grammar & spelling. Keep meaning; do not fabricate facts.
- Normalize dates into clean strings (e.g., "Jan 2023", "2019–2022", "Present").
- Merge duplicates; keep concise bullets (≤ 8 per role). Preserve metrics and impact.
- Group skills into logical categories; deduplicate.
- If field unknown, omit it rather than invent.
Return ONLY JSON (no commentary).
"""

def _normalize_with_llm(raw_text: str, blocks: Optional[Any], hints: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    # Build a single 'user' message with both the linear text and a compact view of blocks + hints.
    # This improves recall (tables & side columns are included).
    payload = {
        "raw_text": raw_text,
        "blocks": blocks or [],
        "hints": hints or {}
    }

    resp = client().chat.completions.create(
        model=AOAI_DEPLOYMENT,
        temperature=0.1,
        max_tokens=4000,
        response_format={"type": "json_schema", "json_schema": {"name": "CVSchema", "schema": CV_SCHEMA}},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}
        ]
    )
    content = resp.choices[0].message.content
    return json.loads(content)

def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method != "POST":
        return func.HttpResponse("POST only", status_code=405)

    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    text  = body.get("text") or body.get("slides_text") or body.get("raw")
    blocks = body.get("blocks")
    hints  = body.get("hints")

    if not text or not isinstance(text, str):
        return func.HttpResponse("Missing 'text' (or 'slides_text'/'raw')", status_code=400)

    try:
        cv = _normalize_with_llm(text, blocks, hints)
        # add simple provenance
        cv["provenance"] = {"normalized_at": __import__("datetime").datetime.utcnow().isoformat() + "Z", "model": AOAI_DEPLOYMENT}
    except Exception as e:
        logging.exception("AOAI normalization failed")
        return func.HttpResponse(json.dumps({"error": f"normalize failed: {e}"}), status_code=502, mimetype="application/json")

    return func.HttpResponse(json.dumps({"cv": cv}), status_code=200, mimetype="application/json")
