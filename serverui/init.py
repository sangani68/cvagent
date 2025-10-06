import os
import mimetypes
import azure.functions as func

# Serve files from /site/wwwroot/ui  (i.e., repo path: wwwroot/ui/)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ui"))

def _safe_join(base, *paths):
    final = os.path.abspath(os.path.join(base, *paths))
    if not final.startswith(os.path.abspath(base)):
        raise ValueError("Path traversal detected")
    return final

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Route param {path}
    rel = (req.route_params.get("path") or "").strip()
    if rel == "" or rel.endswith("/"):
        rel = rel + "index.html"

    try:
        file_path = _safe_join(BASE_DIR, rel)
    except ValueError:
        return func.HttpResponse("Forbidden", status_code=403)

    # SPA fallback: if missing and no extension, try index.html
    if not os.path.isfile(file_path) and "." not in os.path.basename(rel):
        file_path = _safe_join(BASE_DIR, "index.html")

    if not os.path.isfile(file_path):
        return func.HttpResponse("Not Found", status_code=404)

    ctype, _ = mimetypes.guess_type(file_path)
    try:
        with open(file_path, "rb") as f:
            data = f.read()
    except Exception as e:
        return func.HttpResponse(f"Read error: {e}", status_code=500)

    return func.HttpResponse(
        body=data,
        status_code=200,
        mimetype=ctype or "application/octet-stream"
    )
