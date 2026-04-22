from pathlib import Path
from flask import Flask, abort, jsonify, send_file
import os

app = Flask(__name__)

BASE_DIR = Path("/app")

@app.route("/")
def home():
    return """
    <h2>Descargador CNE</h2>
    <p>Ver archivos: <a href="/files">/files</a></p>
    """

@app.route("/files")
def files():
    if not BASE_DIR.exists():
        return jsonify({"error": f"No existe {BASE_DIR}"}), 404

    resultados = []
    for p in sorted(BASE_DIR.rglob("*")):
        if p.is_file():
            rel = str(p.relative_to(BASE_DIR)).replace("\\", "/")
            resultados.append(
                f'<li><a href="/download/{rel}">{rel}</a> ({p.stat().st_size} bytes)</li>'
            )

    if not resultados:
        return "<h2>Archivos</h2><p>No hay archivos todavía.</p>"

    return "<h2>Archivos</h2><ul>" + "".join(resultados) + "</ul>"

@app.route("/download/<path:relpath>")
def download(relpath):
    target = (BASE_DIR / relpath).resolve()
    base_resolved = BASE_DIR.resolve()

    try:
        target.relative_to(base_resolved)
    except Exception:
        abort(403)

    if not target.exists() or not target.is_file():
        abort(404)

    return send_file(target, as_attachment=True)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
