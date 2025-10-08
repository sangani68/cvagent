import os
import json
import base64
import logging
from typing import Dict, Any, Optional

import azure.functions as func
import requests

# -------- Optional: use your repo's pipeline if available --------
# Adjust these imports to your repo structure. They are wrapped to avoid crashing if paths differ.
EXTRACT_AVAILABLE = False
NORMALIZE_AVAILABLE = False

try:
    # Example expected call signatures:
    #   run_pptx_extract(pptx_bytes: bytes) -> Dict
    #   run_normalize(extract_result: Dict) -> Dict (normalized CV)
    from .pipeline.extract import run_pptx_extract       # <-- adjust if needed
    from .pipeline.normalize import run_normalize         # <-- adjust if needed
    EXTRACT_AVAILABLE = True
    NORMALIZE_AVAILABLE = True
except Exception as e:
    logging.warning("Pipeline imports not found; falling back to error for normalize_only. Detail: %s", e)

# -------- HTML templates (Europass & Kyndryl) --------

def _esc(s: Optional[str]) -> str:
    if s is None:
        return ""
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;"))

def _render_euro_like(cv: Dict[str, Any], theme: Dict[str, str]) -> str:
    pi = cv.get("personal_info", {}) or {}
    name = _esc(pi.get("full_name", ""))
    role = _esc(pi.get("headline", ""))
    location = _esc(", ".join([x for x in [pi.get("city"), pi.get("country")] if x]))
    summary = _esc(cv.get("summary") or pi.get("summary") or "")

    langs = cv.get("languages") or []
    lang_items = []
    for l in langs:
        nm = l.get("name") or l.get("language") or l.get("lang") or ""
        lvl = l.get("level") or ""
        lang_items.append(_esc(" — ".join([x for x in [nm, lvl] if x])))

    skills_html = ""
    skills_groups = cv.get("skills_groups") or []
    flat_skills = []
    for g in skills_groups:
        flat_skills.extend(g.get("items") or [])
    if flat_skills:
        chips = "".join(f'<span class="chip">{_esc(s)}</span>' for s in flat_skills)
        skills_html = f'<div class="sec"><h2>{theme["skillsLabel"]}</h2><div>{chips}</div></div>'

    exp_html = ""
    exps = cv.get("work_experience") or []
    parts = []
    for e in exps:
        dates = _esc(f'{e.get("start_date","")} – {e.get("end_date","Present")}')
        bullets = e.get("bullets") or []
        bhtml = "".join(f"<li>{_esc(b)}</li>" for b in bullets)
        parts.append(
            f'''<div style="margin:10px 0 8px 0">
                <div><strong>{_esc(e.get("title",""))}</strong> — {_esc(e.get("company",""))}</div>
                <div class="line2">{dates}{(" • "+_esc(e.get("location"))) if e.get("location") else ""}</div>
                {f'<div style="margin-top:6px">{_esc(e.get("description",""))}</div>' if e.get("description") else ""}
                {f'<ul>{bhtml}</ul>' if bhtml else ""}
            </div>'''
        )
    if parts:
        exp_html = f'<div class="sec"><h2>{theme["expLabel"]}</h2>{"".join(parts)}</div>'

    edu_html = ""
    edus = cv.get("education") or []
    edparts = []
    for ed in edus:
        dates = _esc((ed.get("start_date") or "") + (f' – {ed.get("end_date")}' if ed.get("end_date") else ""))
        edparts.append(
            f'''<div style="margin:10px 0 8px 0">
                <div><strong>{_esc(ed.get("degree") or ed.get("title") or "")}</strong> — {_esc(ed.get("institution",""))}</div>
                <div class="line2">{dates}{(" • "+_esc(ed.get("location"))) if ed.get("location") else ""}</div>
                {f'<div style="margin-top:6px">{_esc(ed.get("details",""))}</div>' if ed.get("details") else ""}
            </div>'''
        )
    if edparts:
        edu_html = f'<div class="sec"><h2>{theme["eduLabel"]}</h2>{"".join(edparts)}</div>'

    photo_b64 = pi.get("photo_base64")
    photo_tag = f'<div style="text-align:center;margin-bottom:10px"><img class="photo" src="data:image/png;base64,{_esc(photo_b64)}" alt="Profile"/></div>' if photo_b64 else ""

    logo_tag = ""
    if theme.get("useLogo"):
        logo_src = theme.get("logoData") or theme.get("logoUrl") or ""
        if logo_src:
            logo_tag = f'<div style="text-align:center;margin-top:16px"><img class="logo" src="{_esc(logo_src)}" alt="Logo"/></div>'

    about_block = f'<div class="sec"><h2>{theme["aboutLabel"]}</h2><div>{summary}</div></div><hr class="hr"/>' if summary else ""

    lang_block = ""
    if lang_items:
        lis = "".join(f"<li>{_esc(it)}</li>" for it in lang_items)
        lang_block = f'<div class="sec"><h2>{theme["langLabel"]}</h2><ul style="list-style:none;margin-left:0">{lis}</ul></div>'

    html = f"""<!doctype html><html><head><meta charset="utf-8"/>
  <style>
    @page {{ size:A4; margin:0 }} body{{margin:0;font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#0f172a}}
    table.layout{{width:100%;border-spacing:0;table-layout:fixed}}
    td.side{{width:32%;vertical-align:top;background:{theme["sideBg"]};color:{theme["sideFg"]};padding:18px 16px}}
    td.main{{width:68%;vertical-align:top;padding:20px 22px}}
    h1{{font-size:24px;margin:0 0 2px 0;color:{theme["nameColor"]}}} .sub{{color:{theme["titleColor"]};font-size:13px;margin:4px 0 12px}}
    .sec{{margin:14px 0 0}} .sec h2{{font-size:14px;margin:0 0 8px;text-transform:uppercase;letter-spacing:.06em;color:{theme["hColor"]}}}
    .chip{{display:inline-block;border-radius:999px;padding:3px 10px;margin:3px 6px 0 0;font-size:11px;border:1px solid {theme["chipBorder"]};background:{theme["chipBg"]};color:{theme["chipFg"]}}}
    .line2{{color:#64748b;font-size:12px;margin:2px 0}}
    ul{{margin:6px 0 6px 18px;padding:0}}
    .logo{{width:110px}}
    .photo{{width:100px;height:100px;border-radius:50%;object-fit:cover}}
    .hr{{border:0;border-top:1px solid #e5e7eb;margin:12px 0}}
  </style></head>
  <body>
  <table class="layout"><tr>
    <td class="side">
      {photo_tag}
      <h1>{name}</h1>
      {f'<div class="sub">{role}</div>' if role else ""}
      {f'<div class="sub" style="margin-top:0">{location}</div>' if location else ""}
      {lang_block}
      {logo_tag}
    </td>
    <td class="main">
      {about_block}
      {exp_html}
      {skills_html}
      {edu_html}
    </td>
  </tr></table>
  </body></html>"""
    return html

def render_template(template: str, cv: Dict[str, Any], kyndryl_logo_data: Optional[str]) -> str:
    t = (template or "europass").lower()
    if t == "kyndryl":
        theme = {
            "sideBg":"#FF462D","sideFg":"#FFFFFF","nameColor":"#FFFFFF","titleColor":"#FFFFFF","hColor":"#FFFFFF",
            "chipBg":"#FFFFFF","chipFg":"#FF462D","chipBorder":"#FFFFFF",
            "langLabel":"LANGUAGES","aboutLabel":"ABOUT ME","expLabel":"PREVIOUS ROLES","skillsLabel":"SKILLS","eduLabel":"EDUCATION",
            "useLogo":True,"logoUrl":"https://upload.wikimedia.org/wikipedia/commons/7/73/Kyndryl_logo.svg",
            "logoData": kyndryl_logo_data or ""
        }
        return _render_euro_like(cv, theme)
    else:
        theme = {
            "sideBg":"#F8FAFC","sideFg":"#0F172A","nameColor":"#0F172A","titleColor":"#475569","hColor":"#0F172A",
            "chipBg":"#EEF2FF","chipFg":"#3730A3","chipBorder":"#E0E7FF",
            "langLabel":"Languages","aboutLabel":"About Me","expLabel":"Work Experience","skillsLabel":"Skills","eduLabel":"Education & Training",
            "useLogo":False,"logoUrl":""
        }
        return _render_euro_like(cv, theme)

# -------- render forwarding --------

def forward_to_renderer(html: str, file_name: str, want: str) -> Dict[str, Any]:
    """
    Forward HTML to your internal renderer if configured.
    Env:
      RENDERPDF_ENDPOINT: e.g. '/api/renderpdf_html' or full URL
    """
    endpoint = os.getenv("RENDERPDF_ENDPOINT", "").strip()
    if not endpoint:
        # No renderer configured; return HTML so caller can inspect
        return {"html": html, "message": "No RENDERPDF_ENDPOINT set; returning HTML instead."}

    # Build absolute URL if needed
    if endpoint.startswith("/"):
        host = os.getenv("WEBSITE_HOSTNAME", "").strip()
        scheme = "https" if host else ""
        if not host:
            return {"html": html, "message": "RENDERPDF_ENDPOINT is relative but WEBSITE_HOSTNAME is not set."}
        url = f"{scheme}://{host}{endpoint}"
    else:
        url = endpoint

    payload = {
        "html": html,
        "file_name": file_name or "cv.pdf",
        "return": want or "url"
    }
    try:
        r = requests.post(url, json=payload, timeout=60)
    except Exception as e:
        logging.exception("Error calling renderer")
        return {"error": f"Downstream error calling renderer: {e}"}
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    if not r.ok:
        err = data.get("error") or data.get("message") or f"HTTP {r.status_code}"
        return {"error": f"Downstream error {r.status_code} calling {endpoint}: {err}"}
    return data

# -------- normalize from PPTX (using your pipeline when available) --------

def normalize_from_pptx_b64(pptx_b64: str, pptx_name: Optional[str]) -> Dict[str, Any]:
    if not EXTRACT_AVAILABLE or not NORMALIZE_AVAILABLE:
        raise ValueError("normalize_only requested but extract/normalize pipeline not available in this deployment.")
    try:
        ppt_bytes = base64.b64decode(pptx_b64, validate=True)
    except Exception:
        ppt_bytes = base64.b64decode(pptx_b64)  # fallback
    extracted = run_pptx_extract(ppt_bytes)   # your repo function
    normalized = run_normalize(extracted)     # your repo function
    return normalized

# -------- Azure Function entry --------

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error": "Invalid JSON"}), status_code=400, mimetype="application/json")

    mode = (body.get("mode") or "").lower().strip()
    template = (body.get("template") or "europass").lower().strip()
    want = (body.get("return") or "url").lower().strip()
    file_name = body.get("file_name") or "cv.pdf"
    kyndryl_logo_data = body.get("kyndryl_logo_data")  # optional data URL
    logging.info("cvagent: mode=%s template=%s want=%s file_name=%s", mode, template, want, file_name)

    # --- 1) Extract + Normalize path ---
    if mode == "normalize_only":
        ppt_b64 = body.get("pptx_base64") or body.get("ppt_base64")
        ppt_name = body.get("pptx_name") or body.get("ppt_name")
        if not ppt_b64:
            return func.HttpResponse(json.dumps({"error": "Missing 'pptx_base64'"}), status_code=400, mimetype="application/json")
        try:
            cv = normalize_from_pptx_b64(ppt_b64, ppt_name)
        except Exception as e:
            logging.exception("normalize_only failed")
            return func.HttpResponse(json.dumps({"error": f"pptxextract/normalize failed: {e}"}), status_code=400, mimetype="application/json")
        return func.HttpResponse(json.dumps({"cv": cv}), status_code=200, mimetype="application/json")

    # --- 2) Render path (expects a ready CV JSON) ---
    cv = body.get("cv")
    html = body.get("html")
    if not html:
        if not cv:
            return func.HttpResponse(json.dumps({"error":"Missing 'cv' (or provide 'html')"}), status_code=400, mimetype="application/json")
        try:
            html = render_template(template, cv, kyndryl_logo_data)
        except Exception as e:
            logging.exception("template render failed")
            return func.HttpResponse(json.dumps({"error": f"Template render failed: {e}"}), status_code=400, mimetype="application/json")

    # If caller wants raw html
    if want == "html":
        return func.HttpResponse(json.dumps({"html": html, "file_name": file_name}), status_code=200, mimetype="application/json")

    # Forward to renderer if configured, else return html
    result = forward_to_renderer(html, file_name, want)
    status = 200 if not result.get("error") else 400
    return func.HttpResponse(json.dumps(result), status_code=status, mimetype="application/json")
