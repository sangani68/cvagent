import os, json, uuid, traceback
import azure.functions as func
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

try:
    from fpdf import FPDF
except Exception as e:
    FPDF = None
    FERR = str(e)

PDF_OUT_BASE = os.environ.get("PDF_OUT_BASE")
FONT_DIR = os.environ.get("PDF_FONT_DIR","/site/wwwroot/fonts")

# Accent color (Europass-like blue). Overridable via app setting "PDF_PRIMARY_RGB = R,G,B"
def _accent():
    raw = os.environ.get("PDF_PRIMARY_RGB","29,78,153")  # default #1D4E99-ish
    try:
        r,g,b = [int(x.strip()) for x in raw.split(",")]
        return (max(0,min(255,r)), max(0,min(255,g)), max(0,min(255,b)))
    except Exception:
        return (29,78,153)

ACCENT = _accent()
HAIR   = tuple(int(c*0.75) for c in ACCENT)  # slightly darker for rules

def http_get_json(url: str) -> dict:
    with urlopen(url) as r: return json.loads(r.read().decode("utf-8"))

def put_blob_with_sas(container_sas: str, blob_name: str, content: bytes, content_type="application/pdf") -> str:
    if "?" in container_sas:
        prefix, qs = container_sas.split("?",1)
        dest = f"{prefix.rstrip('/')}/{blob_name}?{qs}"
    else:
        dest = f"{container_sas.rstrip('/')}/{blob_name}"
    req = Request(dest, data=content, method="PUT",
                  headers={"x-ms-blob-type":"BlockBlob","Content-Type":content_type})
    with urlopen(req) as r:
        _ = r.read()
        return dest

# ---------- font / text helpers ----------
def _font_paths():
    reg = os.path.join(FONT_DIR,"DejaVuSans.ttf")
    bld = os.path.join(FONT_DIR,"DejaVuSans-Bold.ttf")
    return (reg if os.path.exists(reg) else None, bld if os.path.exists(bld) else None)

def use_unicode(pdf: "FPDF", size=10, bold=False) -> bool:
    """Try to use DejaVu (Unicode). Returns True if active; else falls back to Helvetica."""
    reg,bld = _font_paths()
    fam="DejaVu"
    if reg:
        try:
            pdf.add_font(fam,"",reg,uni=True)
        except Exception:
            pass
        if bold and bld:
            try:
                pdf.add_font(fam,"B",bld,uni=True)
                pdf.set_font(fam,"B",size); return True
            except Exception:
                pass
        pdf.set_font(fam,"",size); return True
    pdf.set_font("Helvetica","B" if bold else "", size)
    return False

def sanitize_ascii(s: str) -> str:
    if s is None: return ""
    return (s.replace("\u2014","-").replace("\u2013","-").replace("\u2022","*").replace("\u00b7","*")
             .replace("\u2018","'").replace("\u2019","'").replace("\u201c",'"').replace("\u201d",'"')
           ).encode("latin-1","ignore").decode("latin-1","ignore")

def mc_w(pdf: "FPDF", width: float, text: str, h=5.0, align="J", unicode_ok=True):
    if not text: return
    t = text if unicode_ok else sanitize_ascii(text)
    pdf.multi_cell(width, h, t, align=align)

def cell_w(pdf: "FPDF", width: float, text: str, h=5.0, ln=1, unicode_ok=True):
    t = text if unicode_ok else sanitize_ascii(text)
    pdf.cell(width, h, t, ln=ln)

def bullet_list(pdf: "FPDF", width: float, items, unicode_ok=True):
    for it in items or []:
        mc_w(pdf, width, f"• {it}" if unicode_ok else f"- {sanitize_ascii(it)}", h=5.0, align="J", unicode_ok=unicode_ok)

def page_w(pdf): return pdf.w - pdf.l_margin - pdf.r_margin

def rule(pdf, x1, y, x2):
    pdf.set_draw_color(*HAIR); pdf.line(x1, y, x2, y)

def h1(pdf, text, unicode_ok=True):
    # Name heading
    if unicode_ok: pdf.set_text_color(*ACCENT); use_unicode(pdf, 22, True)
    else: pdf.set_text_color(*ACCENT); pdf.set_font("Helvetica","B",22)
    mc_w(pdf, page_w(pdf), text, h=10, align="L", unicode_ok=unicode_ok)
    pdf.set_text_color(0,0,0)

def hsec_left(pdf, title, left_x, left_w, unicode_ok=True):
    # Left-column section header with accent underline
    pdf.set_xy(left_x, pdf.get_y())
    if unicode_ok: use_unicode(pdf, 12, True); pdf.set_text_color(*ACCENT)
    else: pdf.set_font("Helvetica","B",12); pdf.set_text_color(*ACCENT)
    cell_w(pdf, left_w, title, h=6, ln=1, unicode_ok=unicode_ok)
    y=pdf.get_y(); pdf.set_draw_color(*HAIR); pdf.line(left_x, y, left_x+left_w, y)
    pdf.ln(1); pdf.set_text_color(0,0,0)
    use_unicode(pdf, 10, False) if unicode_ok else pdf.set_font("Helvetica","",10)

def hsec_right(pdf, title, right_x, right_w, unicode_ok=True):
    pdf.set_xy(right_x, pdf.get_y())
    if unicode_ok: use_unicode(pdf, 12, True); pdf.set_text_color(*ACCENT)
    else: pdf.set_font("Helvetica","B",12); pdf.set_text_color(*ACCENT)
    cell_w(pdf, right_w, title, h=6, ln=1, unicode_ok=unicode_ok)
    y=pdf.get_y(); pdf.set_draw_color(*HAIR); pdf.line(right_x, y, right_x+right_w, y)
    pdf.ln(1); pdf.set_text_color(0,0,0)
    use_unicode(pdf, 10, False) if unicode_ok else pdf.set_font("Helvetica","",10)

# ---------- Europass-like two column layout ----------
def europass(cv: dict) -> bytes:
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(True, margin=14)
    pdf.set_margins(14, 14, 14)
    pdf.add_page()

    unicode_ok = use_unicode(pdf, size=22, bold=True)

    gutter = 7
    left_w = 68
    right_w = page_w(pdf) - left_w - gutter
    left_x = pdf.l_margin
    right_x = left_x + left_w + gutter

    # Header
    name = ((cv.get("candidate") or {}).get("full_name")) or "Candidate"
    h1(pdf, name, unicode_ok=unicode_ok)
    pdf.ln(2)
    rule(pdf, pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin)
    pdf.ln(2)

    base_y = pdf.get_y()

    # LEFT column
    pdf.set_xy(left_x, base_y)

    # About me
    hsec_left(pdf, "ABOUT ME", left_x, left_w, unicode_ok)
    mc_w(pdf, left_w, cv.get("summary",""), h=5.1, align="J", unicode_ok=unicode_ok)
    pdf.ln(2)

    # Contact
    c = cv.get("candidate") or {}
    contacts=[]
    if c.get("email"): contacts.append(c["email"])
    if c.get("phone"): contacts.append(c["phone"])
    if c.get("location"): contacts.append(c["location"])
    li=(c.get("links") or {}).get("linkedin"); gh=(c.get("links") or {}).get("github"); pf=(c.get("links") or {}).get("portfolio")
    for v in [li, gh, pf]:
        if v: contacts.append(v)
    if contacts:
        hsec_left(pdf,"CONTACT", left_x, left_w, unicode_ok)
        for line in contacts: mc_w(pdf, left_w, line, h=5.0, align="L", unicode_ok=unicode_ok)
        pdf.ln(1)

    # Skills (dynamic groups first, fallback to skills{})
    groups = cv.get("skills_groups") or []
    if not groups and isinstance(cv.get("skills"), dict):
        groups = [{"name": k.replace("_"," ").title(), "items": v}
                  for k,v in cv["skills"].items() if isinstance(v, list) and v]
    if groups:
        hsec_left(pdf,"SKILLS", left_x, left_w, unicode_ok)
        for grp in groups:
            nm = grp.get("name") or "Skills"
            items = grp.get("items") or []
            if unicode_ok: use_unicode(pdf, 10, True)
            else: pdf.set_font("Helvetica","B",10)
            mc_w(pdf, left_w, nm, h=5.0, align="L", unicode_ok=unicode_ok)
            if unicode_ok: use_unicode(pdf, 10, False)
            else: pdf.set_font("Helvetica","",10)
            mc_w(pdf, left_w, ", ".join(items), h=5.0, align="J", unicode_ok=unicode_ok)
            pdf.ln(0.5)

    # Languages
    langs = cv.get("languages") or []
    if not langs and isinstance(cv.get("skills"), dict):
        langs = cv["skills"].get("languages") or []
    if langs:
        hsec_left(pdf,"LANGUAGES", left_x, left_w, unicode_ok)
        for l in langs: mc_w(pdf, left_w, l, h=5.0, align="L", unicode_ok=unicode_ok)

    # Certifications
    certs = cv.get("certifications") or []
    if certs:
        hsec_left(pdf,"CERTIFICATIONS", left_x, left_w, unicode_ok)
        for ce in certs:
            line = ce.get("name","")
            if ce.get("issuer"): line += f" — {ce['issuer']}"
            if ce.get("date"):   line += f" ({ce['date']})"
            mc_w(pdf, left_w, line, h=5.0, align="L", unicode_ok=unicode_ok)

    # RIGHT column
    pdf.set_xy(right_x, base_y)

    # Experience
    exps = cv.get("experience") or []
    if exps:
        hsec_right(pdf,"WORK EXPERIENCE", right_x, right_w, unicode_ok)
        for e in exps:
            hdr = " · ".join([x for x in [e.get("title"), e.get("company"), e.get("employment_type")] if x])
            dates = " — ".join([x for x in [e.get("start_date"), e.get("end_date") or "Present"] if x])
            where = e.get("location")

            if unicode_ok: use_unicode(pdf, 11, True)
            else: pdf.set_font("Helvetica","B",11)
            mc_w(pdf, right_w, hdr, h=5.0, align="L", unicode_ok=unicode_ok)

            if unicode_ok: use_unicode(pdf, 10, False)
            else: pdf.set_font("Helvetica","",10)
            meta = " | ".join([x for x in [dates, where] if x])
            if meta: mc_w(pdf, right_w, meta, h=5.0, align="L", unicode_ok=unicode_ok)

            bullet_list(pdf, right_w, (e.get("bullets") or [])[:14], unicode_ok=unicode_ok)
            tech = e.get("tech") or []
            if tech: mc_w(pdf, right_w, "Tech: " + ", ".join(tech), h=5.0, align="J", unicode_ok=unicode_ok)
            pdf.ln(1.2)

    # Education
    edus = cv.get("education") or []
    if edus:
        hsec_right(pdf,"EDUCATION AND TRAINING", right_x, right_w, unicode_ok)
        for ed in edus:
            parts=[ed.get("degree"), ed.get("field") or ed.get("field_of_study"), ed.get("institution") or ed.get("school")]
            years = " ".join([str(ed.get("start_year") or ""), str(ed.get("end_year") or "")]).strip()
            if years: parts.append(years)
            mc_w(pdf, right_w, " · ".join([p for p in parts if p]), h=5.0, align="L", unicode_ok=unicode_ok)
            pdf.ln(0.5)

    # Projects
    projs = cv.get("projects") or []
    if projs:
        hsec_right(pdf,"PROJECTS", right_x, right_w, unicode_ok)
        for p in projs:
            name = p.get("name") or "Project"
            if unicode_ok: use_unicode(pdf, 11, True)
            else: pdf.set_font("Helvetica","B",11)
            mc_w(pdf, right_w, name, h=5.0, align="L", unicode_ok=unicode_ok)

            if unicode_ok: use_unicode(pdf, 10, False)
            else: pdf.set_font("Helvetica","",10)
            if p.get("description"): mc_w(pdf, right_w, p["description"], h=5.0, align="J", unicode_ok=unicode_ok)
            tech = p.get("tech") or []
            if tech: mc_w(pdf, right_w, "Tech: " + ", ".join(tech), h=5.0, align="J", unicode_ok=unicode_ok)
            pdf.ln(1)

    # Extra named sections (dynamic)
    extras = cv.get("sections_extra") or []
    for sec in extras:
        nm = sec.get("name") or "Additional"
        items = sec.get("items") or []
        paras = sec.get("paragraphs") or []
        hsec_right(pdf, nm.upper(), right_x, right_w, unicode_ok)
        if items: bullet_list(pdf, right_w, items, unicode_ok=unicode_ok)
        for para in paras:
            mc_w(pdf, right_w, para, h=5.0, align="J", unicode_ok=unicode_ok)
        pdf.ln(0.5)

    data = pdf.output(dest="S")
    return bytes(data) if isinstance(data,(bytes,bytearray)) else data.encode("latin-1","ignore")

def render(cv: dict, template: str) -> bytes:
    return europass(cv)

async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        if FPDF is None:
            return func.HttpResponse(json.dumps({"error":"fpdf2 not found","detail":FERR}), status_code=500, mimetype="application/json")
        if not PDF_OUT_BASE:
            return func.HttpResponse(json.dumps({"error":"Missing app setting PDF_OUT_BASE"}), status_code=500, mimetype="application/json")

        body = req.get_json()
        name_hint = body.get("name_hint","cv")

        if "cv" in body: cv = body["cv"]
        elif "cv_json_url" in body: cv = http_get_json(body["cv_json_url"])
        else:
            return func.HttpResponse(json.dumps({"error":"Provide 'cv' or 'cv_json_url'"}), status_code=400, mimetype="application/json")

        pdf_bytes = render(cv, (body.get("template") or "europass"))
        fname = f"europass-{name_hint}-{uuid.uuid4()}.pdf"
        url = put_blob_with_sas(PDF_OUT_BASE, fname, pdf_bytes)
        return func.HttpResponse(json.dumps({"pdf_url":url}), mimetype="application/json")
    except (HTTPError, URLError) as e:
        detail=""; 
        try: detail = e.read().decode("utf-8","ignore")
        except Exception: pass
        return func.HttpResponse(json.dumps({"error":"network","detail":str(e),"body":detail}), status_code=500, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(json.dumps({"error":str(e),"trace":traceback.format_exc()}), status_code=500, mimetype="application/json")
