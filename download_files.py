from pathlib import Path
from flask import Flask, abort, jsonify, send_file
import os

app = Flask(__name__)

BASE_DIR = Path(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/app/data"))

@app.route("/")
def home():
    return {
        "message": "Downloader activo",
        "base_dir": str(BASE_DIR),
        "routes": [
            "/files",
            "/download/<ruta_relativa>"
        ]
    }

@app.route("/files")
def files():
    if not BASE_DIR.exists():
        return jsonify({"error": f"No existe {BASE_DIR}"}), 404

    resultados = []
    for p in sorted(BASE_DIR.rglob("*")):
        if p.is_file():
            resultados.append({
                "name": p.name,
                "relative_path": str(p.relative_to(BASE_DIR)).replace("\\", "/"),
                "size_bytes": p.stat().st_size,
            })
    return jsonify(resultados)

@app.route("/download/<path:relpath>")
def download(relpath):
    target = (BASE_DIR / relpath).resolve()
    try:
        target.relative_to(BASE_DIR.resolve())
    except Exception:
        abort(403)

    if not target.exists() or not target.is_file():
        abort(404)

    return send_file(target, as_attachment=True)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
