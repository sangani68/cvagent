import os, json, uuid, traceback, zipfile
import azure.functions as func
from io import BytesIO
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
import xml.etree.ElementTree as ET

JSON_BASE = os.environ["STORAGE_JSON_BASE"]  # SAS to 'json-parsed' (with Create+Write)

NS = {
    "p":   "http://schemas.openxmlformats.org/presentationml/2006/main",
    "a":   "http://schemas.openxmlformats.org/drawingml/2006/main",
    "r":   "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "dgm": "http://schemas.openxmlformats.org/drawingml/2006/diagram"  # SmartArt
}

EMU_PER_PX = 9525  # 914400 / 96

def emu_to_px(v):
    try: return int(v) // EMU_PER_PX
    except Exception: return None

def get_text_from_a_p(a_p):
    # collect text and preserve explicit line breaks <a:br/>
    parts = []
    for node in list(a_p):
        tag = node.tag.split('}')[-1]
        if tag == "r":  # run
            t = node.find("a:t", NS)
            if t is not None and t.text:
                parts.append(t.text)
        elif tag == "br":  # line break
            parts.append("\n")
    return "".join(parts).strip()

def get_level_from_a_p(a_p):
    pPr = a_p.find("a:pPr", NS)
    if pPr is not None and "lvl" in pPr.attrib:
        try: return int(pPr.attrib["lvl"])
        except Exception: pass
    return 0

def xfrm_rect(parent):
    # returns absolute-ish geometry from a:xfrm off/ext if present
    xfrm = parent.find(".//a:xfrm", NS)
    if xfrm is None: return None, None, None, None
    off = xfrm.find("a:off", NS); ext = xfrm.find("a:ext", NS)
    if off is None or ext is None: return None, None, None, None
    return (
        emu_to_px(off.attrib.get("x")), emu_to_px(off.attrib.get("y")),
        emu_to_px(ext.attrib.get("cx")), emu_to_px(ext.attrib.get("cy"))
    )

def smartart_texts(graphicData):
    # Pull all a:t within SmartArt (dgm) trees
    texts = []
    for t in graphicData.findall(".//a:t", NS):
        if t.text and t.text.strip():
            texts.append(t.text.strip())
    return texts

def walk_tree(node, blocks, gx=0, gy=0, slide_idx=0):
    """
    Walks p:spTree and nested groups.
    gx, gy = accumulated group offsets (very close approximation).
    """
    for child in list(node):
        tag = child.tag.split('}')[-1]

        if tag == "grpSp":
            # group transform offset
            grp = child.find("p:grpSpPr/a:xfrm", NS)
            add_x = add_y = 0
            if grp is not None:
                off = grp.find("a:off", NS)
                if off is not None:
                    add_x = emu_to_px(off.attrib.get("x")) or 0
                    add_y = emu_to_px(off.attrib.get("y")) or 0
            walk_tree(child, blocks, gx+add_x, gy+add_y, slide_idx)
            continue

        if tag == "sp":
            # text shape
            x, y, w, h = xfrm_rect(child.find("p:spPr", NS))
            if x is not None: x += gx
            if y is not None: y += gy
            tx = child.find("p:txBody", NS)
            if tx is not None:
                for a_p in tx.findall("a:p", NS):
                    txt = get_text_from_a_p(a_p)
                    if not txt: continue
                    lvl = get_level_from_a_p(a_p)
                    blocks.append({
                        "type":"paragraph","slide":slide_idx,
                        "x":x,"y":y,"w":w,"h":h,"level":lvl,"text":txt
                    })
            continue

        if tag == "graphicFrame":
            x, y, w, h = xfrm_rect(child)
            if x is not None: x += gx
            if y is not None: y += gy

            # Table?
            tbl = child.find(".//a:tbl", NS)
            if tbl is not None:
                rows = []
                for a_tr in tbl.findall("a:tr", NS):
                    row=[]
                    for a_tc in a_tr.findall("a:tc", NS):
                        cell_txt=[]
                        txBody = a_tc.find("a:txBody", NS)
                        if txBody is not None:
                            for a_p in txBody.findall("a:p", NS):
                                t = get_text_from_a_p(a_p)
                                if t: cell_txt.append(t)
                        row.append(" ".join(cell_txt).strip())
                    rows.append(row)
                blocks.append({"type":"table","slide":slide_idx,"x":x,"y":y,"w":w,"h":h,"rows":rows})
                continue

            # SmartArt?
            g = child.find("a:graphic/a:graphicData", NS)
            if g is not None:
                uri = g.attrib.get("{%s}uri" % NS["r"]) or g.attrib.get("uri")
                # For SmartArt the URI ends with '/diagram'
                if uri and uri.endswith("/diagram"):
                    texts = smartart_texts(g)
                    for t in texts:
                        if t:
                            blocks.append({"type":"paragraph","slide":slide_idx,"x":x,"y":y,"w":w,"h":h,"level":1,"text":t})
                    continue

            # Some frames still have a txBody directly (rare)
            tx = child.find(".//a:txBody", NS)
            if tx is not None:
                for a_p in tx.findall("a:p", NS):
                    t = get_text_from_a_p(a_p)
                    if t:
                        blocks.append({"type":"paragraph","slide":slide_idx,"x":x,"y":y,"w":w,"h":h,"level":0,"text":t})
            continue
        # pictures/charts without text ignored

def parse_slide_xml(zf, slide_path, idx):
    with zf.open(slide_path) as f:
        root = ET.parse(f).getroot()
    # Title
    title_text = ""
    for sp in root.findall(".//p:sp", NS):
        ph = sp.find("p:nvSpPr/p:nvPr/p:ph", NS)
        if ph is not None and ph.attrib.get("type") in ("title","ctrTitle"):
            tx = sp.find("p:txBody", NS)
            if tx is not None:
                parts=[]
                for a_p in tx.findall(".//a:p", NS):
                    t = get_text_from_a_p(a_p)
                    if t: parts.append(t)
                title_text = " ".join(parts).strip()
                break
    # Shapes
    blocks=[]
    spTree = root.find(".//p:cSld/p:spTree", NS)
    if spTree is not None:
        walk_tree(spTree, blocks, 0, 0, idx)
    return title_text, blocks

def parse_notes_xml(zf, idx):
    path = f"ppt/notesSlides/notesSlide{idx}.xml"
    try:
        with zf.open(path) as f:
            root = ET.parse(f).getroot()
        parts=[]
        for a_p in root.findall(".//a:p", NS):
            t = get_text_from_a_p(a_p)
            if t: parts.append(t)
        return "\n".join(parts).strip()
    except KeyError:
        return ""
    except Exception:
        return ""

def build_raw_text(slides_meta, blocks):
    lines=[]
    for s in slides_meta:
        lines.append(f"--- Slide {s['slide']} ---")
        if s.get("title"): lines.append(s["title"])
        for b in (bb for bb in blocks if bb["slide"]==s["slide"]):
            if b["type"]=="paragraph":
                lvl=b.get("level",0) or 0
                # expand embedded newlines into separate bullets
                for seg in (b["text"].split("\n") if "\n" in b["text"] else [b["text"]]):
                    seg=seg.strip()
                    if not seg: continue
                    bullet = "• " if lvl>0 else ""
                    indent = "  " * max(0, lvl-1)
                    lines.append(indent + bullet + seg)
            elif b["type"]=="table":
                for row in b["rows"]:
                    lines.append(" | ".join([c for c in row if c]))
        if s.get("notes"): lines.append("[NOTES] " + s["notes"])
    return "\n".join(lines)

def download_bytes(url:str)->bytes:
    with urlopen(url) as r: return r.read()

def put_blob_with_sas(container_sas: str, blob_name: str, content: bytes, content_type="application/json") -> str:
    if "?" in container_sas:
        prefix, qs = container_sas.split("?", 1)
        dest = f"{prefix.rstrip('/')}/{blob_name}?{qs}"
    else:
        dest = f"{container_sas.rstrip('/')}/{blob_name}"
    req = Request(dest, data=content, method="PUT",
                  headers={"x-ms-blob-type":"BlockBlob","Content-Type":content_type})
    with urlopen(req) as resp:
        _ = resp.read()
        return dest

def parse_pptx_bytes(data: bytes):
    zf = zipfile.ZipFile(BytesIO(data))
    slide_paths = sorted([p for p in zf.namelist() if p.startswith("ppt/slides/slide") and p.endswith(".xml")],
                         key=lambda p: int("".join(ch for ch in p if ch.isdigit())))
    slides_meta=[]; all_blocks=[]
    for idx, spath in enumerate(slide_paths, start=1):
        title, blocks = parse_slide_xml(zf, spath, idx)
        for b in blocks: b["slide"]=idx
        notes = parse_notes_xml(zf, idx)
        slides_meta.append({"slide":idx,"title":title,"notes":notes})
        all_blocks.extend(blocks)
    # Order: slide, then y, then x
    all_blocks.sort(key=lambda b:(b["slide"], b.get("y") or 0, b.get("x") or 0))
    raw_text = build_raw_text(slides_meta, all_blocks)
    return {"slides":slides_meta,"blocks":all_blocks,"raw_text":raw_text}

async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        ppt = body["ppt_blob_sas"]  # blob SAS to .pptx
        name_hint = body.get("name_hint","cv")

        if ".pptx?" not in ppt.lower() and not ppt.lower().endswith(".pptx"):
            return func.HttpResponse(json.dumps({"error":"Provide a blob SAS URL to a .pptx file"}), status_code=400)

        data = download_bytes(ppt)
        parsed = parse_pptx_bytes(data)
        key = f"{name_hint}-{uuid.uuid4()}-raw3.json"
        raw_json_url = put_blob_with_sas(JSON_BASE, key, json.dumps(parsed).encode("utf-8"))

        return func.HttpResponse(json.dumps({
            "raw_json_url": raw_json_url,
            "text": parsed["raw_text"],
            "blocks_count": len(parsed["blocks"]),
            "slides_count": len(parsed["slides"])
        }), mimetype="application/json")
    except (HTTPError, URLError) as e:
        detail=""
        try: detail = e.read().decode("utf-8","ignore")
        except Exception: pass
        return func.HttpResponse(json.dumps({"error":"network","detail":str(e),"body":detail}), status_code=500)
    except Exception as e:
        return func.HttpResponse(json.dumps({"error":str(e),"trace":traceback.format_exc()}), status_code=500)
