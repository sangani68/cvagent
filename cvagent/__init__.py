import os, json, logging, base64, requests, azure.functions as func
from jinja2 import Environment, BaseLoader, select_autoescape

# -------------------------------------------------------------------
# Jinja2 Europass template (you can later add more templates here)
# -------------------------------------------------------------------
_EUROPASS_HTML = """<!doctype html>
<html><head>
<meta charset="utf-8"/>
<title>{{ person.full_name or 'Curriculum Vitae' }}</title>
<style>
  @page { size: A4; margin: 10mm }
  body{margin:0;font-family:"DejaVu Sans",Arial,Helvetica,sans-serif;font-size:12px;color:#0f172a}
  .eu-root{display:grid;grid-template-columns:320px 1fr;min-height:100vh}
  .eu-side{background:#f8fafc;border-right:1px solid #e5e7eb;padding:22px}
  .eu-main{padding:22px 26px}
  .eu-name{font-size:26px;font-weight:800;margin:0}
  .eu-title{font-size:13px;color:#475569;margin-top:4px}
  .eu-kv{display:grid;grid-template-columns:22px 1fr;gap:10px;margin:6px 0}
  .ico{width:22px;height:22px;border-radius:6px;background:#e2e8f0;display:flex;align-items:center;justify-content:center;font-size:12px}
  .eu-sec{margin-top:16px}
  .eu-sec h2{font-size:14px;font-weight:800;margin:0 0 10px;text-transform:uppercase;letter-spacing:.06em}
  .eu-chip{display:inline-block;background:#eef2ff;color:#3730a3;border:1px solid #e0e7ff;border-radius:999px;padding:3px 10px;margin:3px 6px 0 0;font-size:11px}
  .eu-job{margin:12px 0 10px}
  .line2{color:#64748b;font-size:12px;margin-top:2px}
  .desc{margin-top:6px}
  .eu-job ul{margin:6px 0 0 18px}
  .hr{height:1px;background:linear-gradient(90deg,#e5e7eb 60%,transparent 0) repeat-x;background-size:8px 1px;margin:14px 0}
</style></head>
<body>
<div class="eu-root">
  <aside class="eu-side">
    <h1 class="eu-name">{{ person.full_name or '' }}</h1>
    {% if person.title %}<div class="eu-title">{{ person.title }}</div>{% endif %}
    <div>
      {% for c in contacts %}
        <div class="eu-kv"><div class="ico">{{ c.ico }}</div><div>{{ c.txt }}</div></div>
      {% endfor %}
    </div>
    {% if skills %}
    <div class="eu-sec"><h2>Skills</h2><div>{% for s in skills %}<span class="eu-chip">{{ s }}</span>{% endfor %}</div></div>
    {% endif %}
    {% if languages %}
    <div class="eu-sec"><h2>Languages</h2><div>{% for l in languages %}<span class="eu-chip">{{ l.name }}{% if l.level %} — {{ l.level }}{% endif %}</span>{% endfor %}</div></div>
    {% endif %}
  </aside>
  <main class="eu-main">
    {% if summary %}
      <section class="eu-sec"><h2>About Me</h2><div>{{ summary }}</div></section><div class="hr"></div>
    {% endif %}
    {% if experiences %}
      <section class="eu-sec"><h2>Work Experience</h2>
        {% for e in experiences %}
          <div class="eu-job">
            <div class="line1"><strong>{{ e.title }}</strong> — {{ e.company }}</div>
            <div class="line2">{{ e.start_date }}{% if e.end_date %} – {{ e.end_date }}{% else %} – Present{% endif %}{% if e.location %} • {{ e.location }}{% endif %}</div>
            {% if e.description %}<div class="desc">{{ e.description }}</div>{% endif %}
            {% if e.bullets %}<ul>{% for b in e.bullets %}<li>{{ b }}</li>{% endfor %}</ul>{% endif %}
          </div>
        {% endfor %}
      </section>
    {% endif %}
    {% if education %}
      <section class="eu-sec"><h2>Education & Training</h2>
        {% for ed in education %}
          <div class="eu-edu">
            <div class="line1"><strong>{{ ed.degree or ed.title }}</strong> — {{ ed.institution }}</div>
            <div class="line2">{{ ed.start_date }}{% if ed.end_date %} – {{ ed.end_date }}{% endif %}{% if ed.location %} • {{ ed.location }}{% endif %}</div>
            {% if ed.details %}<div class="desc">{{ ed.details }}</div>{% endif %}
          </div>
        {% endfor %}
      </section>
    {% endif %}
  </main>
</div>
</body></html>
"""

def _build_html_from_cv(cv: dict, template_name: str = "europass") -> str:
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(['html']))
    j = env.from_string(_EUROPASS_HTML)
    pi = (cv.get("personal_info") or {}) if isinstance(cv, dict) else {}
    contacts = []
    def add(icon, val): 
        if val: contacts.append({"ico": icon, "txt": val})
    add("@", pi.get("email")); add("☎", pi.get("phone")); add("in", pi.get("linkedin"))
    add("🌐", pi.get("website"))
    addr = ", ".join([pi.get("address") or "", pi.get("city") or "", pi.get("country") or ""]).strip(", ")
    add("📍", addr)
    add("🎂", pi.get("date_of_birth")); add("⚧", pi.get("gender")); add("🌎", pi.get("nationality"))
    skills = []
    for g in (cv.get("skills_groups") or []):
        skills.extend(g.get("items") or [])
    model = {
        "person": {"full_name": pi.get("full_name") or cv.get("name"), "title": pi.get("headline") or cv.get("title")},
        "contacts": contacts,
        "skills": skills,
        "languages": cv.get("languages") or [],
        "summary": cv.get("summary") or pi.get("summary"),
        "experiences": cv.get("work_experience") or cv.get("experience") or [],
        "education": cv.get("education") or [],
    }
    return j.render(**model)

# -------------------------------------------------------------------
# Main HTTP Trigger
# -------------------------------------------------------------------
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("cvagent function triggered.")
    try:
        body = req.get_json()
    except Exception:
        return func.HttpResponse(json.dumps({"error":"Invalid JSON"}), status_code=400, mimetype="application/json")

    # 1️⃣ Extract + Normalize (already working for you)
    if body.get("mode") == "normalize_only":
        # Keep your existing extraction + normalization call
        try:
            # Example placeholder for your actual normalize logic
            # normalized = call_normalize_logic(body)
            normalized = {"message": "stub normalize — replace with your real code"}
            return func.HttpResponse(json.dumps(normalized), mimetype="application/json", status_code=200)
        except Exception as e:
            logging.exception(e)
            return func.HttpResponse(json.dumps({"error": f"normalize failed: {str(e)}"}), status_code=500, mimetype="application/json")

    # 2️⃣ Export path: render PDF
    if "cv" in body:
        cv = body.get("cv")
        out_name = body.get("file_name") or "cv.pdf"
        template = (body.get("template") or "europass").lower()

        # Build HTML for renderer
        try:
            html = _build_html_from_cv(cv, template)
        except Exception as e:
            return func.HttpResponse(json.dumps({"error": f"Template render failed: {str(e)}"}), status_code=500, mimetype="application/json")

        # Call renderpdf_html (downstream)
        base = os.environ.get("DOWNSTREAM_BASE_URL") or os.environ.get("FUNCS_BASE_URL") or ""
        render_path = os.environ.get("RENDER_PATH", "/api/renderpdf_html")
        render_key = os.environ.get("RENDER_KEY")
        render_url = (base.rstrip("/") + render_path) if base else (req.url.replace("/api/cvagent", render_path))
        if render_key:
            sep = "&" if "?" in render_url else "?"
            render_url = f"{render_url}{sep}code={render_key}"

        payload = {"out_name": out_name, "html": html, "css": ""}

        try:
            r = requests.post(render_url, json=payload, timeout=180)
        except Exception as e:
            return func.HttpResponse(json.dumps({"error": f"Failed calling renderer: {str(e)}"}), status_code=502, mimetype="application/json")

        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}

        if not r.ok:
            return func.HttpResponse(
                json.dumps({"error": f"renderpdf_html error: Downstream error {r.status_code} calling {render_path}: {data}"}),
                status_code=400, mimetype="application/json"
            )

        return func.HttpResponse(json.dumps(data), status_code=200, mimetype="application/json")

    # 3️⃣ Default fallback
    return func.HttpResponse(
        json.dumps({"error": "Unsupported request. Provide mode:'normalize_only' or {cv,file_name,template}."}),
        status_code=400, mimetype="application/json"
    )
