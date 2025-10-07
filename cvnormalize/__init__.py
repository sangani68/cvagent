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

# Support both AOAI_* and AZURE_OPENAI_* naming
AOAI_ENDPOINT     = _get("AOAI_ENDPOINT", "AZURE_OPENAI_ENDPOINT")
AOAI_KEY          = _get("AOAI_KEY", "AZURE_OPENAI_API_KEY")
AOAI_DEPLOYMENT   = _get("AOAI_DEPLOYMENT", "AZURE_OPENAI_DEPLOYMENT", default="gpt-4.1")
AOAI_API_VERSION  = _get("AOAI_API_VERSION", "AZURE_OPENAI_API_VERSION", default="2024-10-21")

if not (AOAI_ENDPOINT and AOAI_KEY):
    logging.warning("Azure OpenAI endpoint/key not set. Set AOAI_ENDPOINT and AOAI_KEY (or AZURE_OPENAI_*).")

_client: Optional[AzureOpenAI] = None
def client() -> AzureOpenAI:
    global _client
    if _client is None:
        _client = AzureOpenAI(
            azure_endpoint=AOAI_ENDPOINT,
            api_key=AOAI_KEY,
            api_version=AOAI_API_VERSION,
        )
    return _client

# ---------- JSON Schema (optional fields allowed) ----------
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
        }
    },
    "required": ["personal_info"]
}

SYSTEM_PROMPT = """You are an expert CV normalizer.
Transform noisy text (from a PPTX CV) into a clean JSON that fits the provided JSON Schema.
- Proofread grammar/spelling without changing meaning.
- Normalize dates (e.g., "Jan 2023", "2019–2022", or "Present").
- Merge duplicates; keep concise bullet points (max 7 per job).
- Group skills into logical groups; deduplicate items.
- Do not invent facts. If a field is unknown, omit it.
Return ONLY JSON that validates against the schema.
"""

def _normalize_with_llm(raw_text: str) -> Dict[str, Any]:
    # NOTE: no "strict": True here — optional fields remain optional.
    resp = client().chat.completions.create(
        model=AOAI_DEPLOYMENT,
        temperature=0.2,
        max_tokens=3500,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "CVSchema",
                "schema": CV_SCHEMA
            }
        },
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": raw_text}
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

    text = body.get("text") or body.get("slides_text") or body.get("raw")
    if not text or not isinstance(text, str):
        return func.HttpResponse("Missing 'text' (or 'slides_text'/'raw')", status_code=400)

    try:
        cv = _normalize_with_llm(text)
    except Exception as e:
        logging.exception("AOAI normalization failed")
        return func.HttpResponse(
            json.dumps({"error": f"normalize failed: {e}"}),
            status_code=502, mimetype="application/json"
        )

    return func.HttpResponse(json.dumps({"cv": cv}), status_code=200, mimetype="application/json")
