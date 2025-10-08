import json, azure.functions as func
from jinja2 import Environment, BaseLoader, select_autoescape

# =============================
# Inline templates
# =============================

_EUROPASS_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"/>
<style>
  @page { size:A4; margin:10mm }
  body{margin:0;font-family:"DejaVu Sans",Arial,sans-serif;font-size:12px;color:#0f172a}
  .eu-root{display:grid;grid-template-columns:320px 1fr;min-height:100vh}
  .eu-side{background:#f8fafc;border-right:1px solid #e5e7eb;padding:22px}
  .eu-main{padding:22px 26px}
  .eu-name{font-size:26px;font-weight:800;margin:0}
  .eu-title{font-size:13px;color:#475569;margin-top:4px}
  .eu-chip{display:inline-block;background:#eef2ff;color:#3730a3;border-radius:999px;padding:3px 10px;margin:3px 6px 0 0;font-size:11px}
  .eu-sec{margin-top:14px}
  .line2{color:#64748b;font-size:12px;margin-top:2px}
</style></head>
<body>
<div class="eu-root">
  <aside class="eu-side">
    <div class="eu-sec">
      <div class="eu-name">{{ person.full_name }}</div>
      <div class="eu-title">{{ person.title }}</div>
    </div>
    <div class="eu-sec">
      <h2>Contact</h2>
      {% for c in contacts %}<div>{{ c.ico }} {{ c.txt }}</div>{% endfor %}
    </div>
    {% if skills %}
    <div class="eu-sec"><h2>Skills</h2>{% for s in skills %}<span class="eu-chip">{{ s }}</span>{% endfor %}</div>
    {% endif %}
  </aside>
  <main class="eu-main">
    {% if summary %}<div class="eu-sec"><h2>Summary</h2>{{ summary }}</div>{% endif %}
    {% if experiences %}
    <div class="eu-sec"><h2>Experience</h2>
      {% for x in experiences %}
      <div><strong>{{ x.role }}</strong> — {{ x.company }}<div class="line2">{{ x.start }} – {{ x.end }}</div></div>
      {% endfor %}
    </div>{% endif %}
  </main>
</div>
</body></html>"""

# Clone Europass → tweak colors for Kyndryl
_KYNDRYL_HTML = _EUROPASS_HTML \
    .replace('#f8fafc', '#b91c1c') \
    .replace('color:#0f172a', 'color:#fff') \
    .replace('#3730a3', '#fff') \
    .replace('border-right:1px solid #e5e7eb', 'border-right:1px solid #991b1b')

# =============================
# Renderer
# =============================

def render_html(cv, template):
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(['html']))
    tname = (template or 'europass').lower()
    tpl = _KYNDRYL_HTML if tname == 'kyndryl' else _EUROPASS_HTML
    j = env.from_string(tpl)

    pi = cv.get("personal_info", {})
    contacts = []
    def add(ico,val): 
        if val: contacts.append({"ico": ico, "txt": val})
    add("@",pi.get("email")); add("☎",pi.get("phone")); add("in",pi.get("linkedin"))

    skills=[]
    if isinstance(cv.get("skills_groups"), list):
        for g in cv["skills_groups"]:
            for s in g.get("items",[]): 
                if s and s not in skills: skills.append(s)

    return j.render(
        person={"full_name": pi.get("full_name",""), "title": pi.get("headline","")},
        contacts=contacts, skills=skills,
        summary=cv.get("summary") or pi.get("summary"),
        experiences=cv.get("work_experience",[])
    )

def main(req: func.HttpRequest) -> func.HttpResponse:
    body = req.get_json()
    cv = body.get("cv")
    template = body.get("template", "europass")
    html = render_html(cv, template)
    return func.HttpResponse(html, mimetype="text/html")
