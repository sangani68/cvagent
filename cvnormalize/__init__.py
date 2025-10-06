import os, json, uuid, datetime, traceback
import azure.functions as func
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ---- Config (you said you use gpt-4.1 + 2024-12-01-preview) ----
AOAI_ENDPOINT    = os.environ["AOAI_ENDPOINT"]
AOAI_KEY         = os.environ["AOAI_KEY"]
AOAI_DEPLOYMENT  = os.environ.get("AOAI_DEPLOYMENT", "gpt-4.1")
AOAI_API_VERSION = os.environ.get("AOAI_API_VERSION", "2024-12-01-preview")

JSON_BASE        = os.environ["STORAGE_JSON_BASE"]  # SAS to json-parsed (Create+Write)

SYSTEM = (
  "You are a meticulous CV extractor/normalizer. Return ONLY valid JSON. "
  "Infer sections dynamically from headings and context — do NOT drop information. "
  "Schema (keys):\n"
  "candidate{full_name,email,phone,location,links{linkedin,github,portfolio}},\n"
  "summary,\n"
  "experience[{title,company,employment_type,start_date,end_date,location,bullets[],tech[]}],\n"
  "education[{degree,field,institution,start_year,end_year}],\n"
  "certifications[{name,issuer,date}],\n"
  "projects[{name,description,tech[]}],\n"
  "skills (object, optional),\n"
  "skills_groups (array of {name,items[]}) ← use this for dynamic categories (e.g., 'Cloud Platforms', 'Analytics & Visualization', 'Integration Tools', etc.),\n"
  "languages (array of strings),\n"
  "sections_extra (array of {name, items[] or paragraphs[]}) for any other named sections (e.g., Publications, Trainings),\n"
  "provenance{model,normalized_at}.\n"
  "Rules:\n"
  "- Keep facts verbatim; no hallucinations.\n"
  "- Preserve EVERY bullet. If the source uses tables/sidebars, transform them into appropriate lists.\n"
  "- Dates: YYYY-MM if available; otherwise YYYY or null.\n"
  "- Standardize tech names but do not invent.\n"
  "- If both a legacy 'skills' object and 'skills_groups' make sense, you may include both; 'skills_groups' takes priority for rendering.\n"
)

def http_get(url: str) -> bytes:
    with urlopen(url) as r:
        return r.read()

def aoai_normalize(raw_text: str, blocks: list|None, slides: list|None, proofread: bool=False) -> dict:
    user_parts = [
        "Normalize this CV using BOTH raw text and structured blocks so NOTHING is dropped.",
        "Return strict JSON with the schema described by the system message.",
        f"Proofread minor grammar/punctuation? {'yes' if proofread else 'no'} (keep facts).",
        "RAW_TEXT:\n"+raw_text[:180000]
    ]
    if blocks:
        user_parts.append("BLOCKS(JSON):\n"+json.dumps(blocks)[:120000])
    if slides:
        user_parts.append("SLIDES(JSON):\n"+json.dumps(slides)[:80000])

    payload = {
        "messages": [
            {"role":"system","content": SYSTEM},
            {"role":"user","content": "\n\n".join(user_parts)}
        ],
        "temperature": 0.1,
        "response_format": {"type":"json_object"}
    }
    url = f"{AOAI_ENDPOINT}/openai/deployments/{AOAI_DEPLOYMENT}/chat/completions?api-version={AOAI_API_VERSION}"
    req = Request(url, data=json.dumps(payload).encode("utf-8"), method="POST",
                  headers={"Content-Type":"application/json","api-key":AOAI_KEY})
    with urlopen(req) as resp:
        data = json.loads(resp.read().decode("utf-8"))
        content = data["choices"][0]["message"]["content"]
        obj = json.loads(content)
        obj.setdefault("provenance", {})
        obj["provenance"]["model"] = AOAI_DEPLOYMENT
        obj["provenance"]["normalized_at"] = datetime.datetime.utcnow().isoformat()+"Z"
        return obj

def put_json(container_sas: str, blob_name: str, obj: dict) -> str:
    if "?" in container_sas:
        prefix, qs = container_sas.split("?", 1)
        dest = f"{prefix.rstrip('/')}/{blob_name}?{qs}"
    else:
        dest = f"{container_sas.rstrip('/')}/{blob_name}"
    req = Request(dest, data=json.dumps(obj).encode("utf-8"), method="PUT",
                  headers={"x-ms-blob-type":"BlockBlob","Content-Type":"application/json"})
    with urlopen(req) as r:
        _ = r.read()
        return dest

async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        name_hint = body.get("name_hint","cv")
        proofread = bool(body.get("proofread", False))

        if "raw_json_url" in body:
            raw = json.loads(http_get(body["raw_json_url"]).decode("utf-8"))
            raw_text = raw.get("raw_text","")
            blocks = raw.get("blocks")
            slides = raw.get("slides")
        elif "text" in body:
            raw_text = body["text"]
            blocks = None
            slides = None
        else:
            return func.HttpResponse(json.dumps({"error":"Provide 'raw_json_url' or 'text'."}), status_code=400)

        cv = aoai_normalize(raw_text, blocks, slides, proofread=proofread)

        key = f"{name_hint}-{uuid.uuid4()}.json"
        json_url = put_json(JSON_BASE, key, cv)
        return func.HttpResponse(json.dumps({"cv":cv,"json_url":json_url}), mimetype="application/json")

    except (HTTPError, URLError) as e:
        detail=""
        try: detail = e.read().decode("utf-8","ignore")
        except Exception: pass
        return func.HttpResponse(json.dumps({"error":"AOAI/HTTP","detail":detail}), status_code=500)
    except Exception as e:
        return func.HttpResponse(json.dumps({"error":str(e),"trace":traceback.format_exc()}), status_code=500)
