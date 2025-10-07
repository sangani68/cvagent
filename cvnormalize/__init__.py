import os, json, logging
from typing import Any, Dict, Optional
import azure.functions as func
from openai import AzureOpenAI

def _get(name, *aliases, default=None):
    for k in (name, *aliases):
        v = os.getenv(k)
        if v: return v
    return default

AOAI_ENDPOINT    = _get("AOAI_ENDPOINT", "AZURE_OPENAI_ENDPOINT")
AOAI_KEY         = _get("AOAI_KEY", "AZURE_OPENAI_API_KEY")
AOAI_DEPLOYMENT  = _get("AOAI_DEPLOYMENT", "AZURE_OPENAI_DEPLOYMENT", default="gpt-4.1")
AOAI_API_VERSION = _get("AOAI_API_VERSION", "AZURE_OPENAI_API_VERSION", default="2024-10-21")

_client: Optional[AzureOpenAI] = None
def client() -> AzureOpenAI:
    global _client
    if _client is None:
        _client = AzureOpenAI(azure_endpoint=AOAI_ENDPOINT, api_key=AOAI_KEY, api_version=AOAI_API_VERSION)
    return _client

CV_SCHEMA: Dict[str, Any] = {
    "type":"object","additionalProperties":False,
    "properties":{
        "personal_info":{
            "type":"object","additionalProperties":False,
            "properties":{
                "full_name":{"type":"string"},"headline":{"type":"string"},"address":{"type":"string"},
                "city":{"type":"string"},"country":{"type":"string"},"email":{"type":"string"},
                "phone":{"type":"string"},"website":{"type":"string"},"linkedin":{"type":"string"},
                "nationality":{"type":"string"},"date_of_birth":{"type":"string"},"gender":{"type":"string"},
                "summary":{"type":"string"}
            },
            "required":["full_name"]
        },
        "summary":{"type":"string"},
        "skills_groups":{"type":"array","items":{"type":"object","additionalProperties":False,
            "properties":{"group":{"type":"string"},"items":{"type":"array","items":{"type":"string"}}},
            "required":["group","items"]}},
        "work_experience":{"type":"array","items":{"type":"object","additionalProperties":False,
            "properties":{"title":{"type":"string"},"company":{"type":"string"},"location":{"type":"string"},
                "start_date":{"type":"string"},"end_date":{"type":"string"},"description":{"type":"string"},
                "bullets":{"type":"array","items":{"type":"string"}}},
            "required":["title","company"]}},
        "education":{"type":"array","items":{"type":"object","additionalProperties":False,
            "properties":{"degree":{"type":"string"},"title":{"type":"string"},"institution":{"type":"string"},
                "location":{"type":"string"},"start_date":{"type":"string"},"end_date":{"type":"string"},"details":{"type":"string"}},
            "required":["institution"]}},
        "languages":{"type":"array","items":{"type":"object","additionalProperties":False,
            "properties":{"name":{"type":"string"},"level":{"type":"string"}},
            "required":["name"]}},
        "provenance":{"type":"object","additionalProperties":True}
    },
    "required":["personal_info"]
}

SYSTEM_PROMPT = """You are an expert CV normalizer.
Inputs:
- 'raw_text': high-recall text with [L]/[R] column cues and slide titles
- 'blocks': structured blocks (text/table/alt) with left/right column and positions
- 'hints': detected emails/phones/urls/linkedin
Task: produce a complete CV JSON that fits the schema.
Rules:
- Use ALL relevant info, including side-column and tables.
- Proofread grammar & spelling; keep meaning (no fabrication).
- Normalize dates ("Jan 2023", "2019–2022", "Present").
- Merge duplicates; concise bullets (<= 8) with impact/metrics.
- Group skills logically; deduplicate.
Return ONLY JSON.
"""

def _normalize(raw_text: str, blocks: Any, hints: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"raw_text": raw_text, "blocks": blocks or [], "hints": hints or {}}
    resp = client().chat.completions.create(
        model=AOAI_DEPLOYMENT, temperature=0.1, max_tokens=4000,
        response_format={"type":"json_schema","json_schema":{"name":"CVSchema","schema":CV_SCHEMA}},
        messages=[{"role":"system","content":SYSTEM_PROMPT},
                  {"role":"user","content":json.dumps(payload, ensure_ascii=False)}]
    )
    content = resp.choices[0].message.content
    return json.loads(content)

def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method != "POST": return func.HttpResponse("POST only", status_code=405)
    try: body = req.get_json()
    except ValueError: return func.HttpResponse("Invalid JSON", status_code=400)

    text = body.get("text") or body.get("slides_text") or body.get("raw")
    blocks = body.get("blocks")
    hints = body.get("hints")
    if not text or not isinstance(text, str):
        return func.HttpResponse("Missing 'text' (or 'slides_text'/'raw')", status_code=400)

    try:
        cv = _normalize(text, blocks, hints)
        cv["provenance"] = {"model": AOAI_DEPLOYMENT, "normalized_at": __import__("datetime").datetime.utcnow().isoformat()+"Z"}
    except Exception as e:
        logging.exception("normalize failed")
        return func.HttpResponse(json.dumps({"error": f"normalize failed: {e}"}), status_code=502, mimetype="application/json")

    return func.HttpResponse(json.dumps({"cv": cv}), status_code=200, mimetype="application/json")
