"""
file_server.py — Servidor web CNE con upload de historicos
"""
import os, glob, base64, threading, urllib.request, urllib.error
import json
from datetime import datetime
from flask import (Flask, render_template_string, send_file,
                   abort, jsonify, request)
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB

DATA_DIR     = os.environ.get("DATA_DIR", "/app/data")
UPLOAD_TOKEN = os.environ.get("UPLOAD_TOKEN", "cne2026")
ALLOWED_EXT  = {'.csv', '.xlsx', '.pdf'}

# ── Template HTML ────────────────────────────────────────────────────────
HTML = r"""
<!DOCTYPE html><html lang="es">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>CNE — Reportes</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
:root{--bg:#0d1117;--surface:#161b22;--border:#30363d;--accent:#58a6ff;--green:#3fb950;--red:#f85149;--text:#e6edf3;--muted:#8b949e}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'IBM Plex Sans',sans-serif;min-height:100vh;padding:2rem}
header{border-bottom:1px solid var(--border);padding-bottom:1.5rem;margin-bottom:2rem}
.logo{font-family:'IBM Plex Mono',monospace;font-size:.75rem;color:var(--accent);letter-spacing:.2em;text-transform:uppercase;margin-bottom:.5rem}
h1{font-size:1.6rem;font-weight:600}
h2{font-size:1.1rem;font-weight:600;margin-bottom:1rem;color:var(--accent)}
.subtitle{color:var(--muted);font-size:.9rem;margin-top:.3rem}
.stats{display:flex;gap:2rem;margin-bottom:2rem;flex-wrap:wrap}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem 1.5rem;min-width:160px}
.stat-label{font-size:.75rem;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;font-family:'IBM Plex Mono',monospace}
.stat-value{font-size:1.4rem;font-weight:600;color:var(--accent);margin-top:.25rem}
.upload-section{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1.5rem;margin-bottom:2rem}
.upload-grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem;align-items:end}
.field{display:flex;flex-direction:column;gap:.4rem}
.field label{font-size:.75rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;font-family:'IBM Plex Mono',monospace}
.field input[type=file],.field input[type=text],.field select{background:#0d1117;border:1px solid var(--border);border-radius:6px;color:var(--text);padding:.5rem .75rem;font-size:.875rem;width:100%;font-family:'IBM Plex Sans',sans-serif}
.field input[type=file]{padding:.4rem}
.btn-upload{background:var(--green);color:#0d1117;border:none;border-radius:6px;padding:.6rem 1.4rem;font-size:.875rem;font-weight:600;cursor:pointer;font-family:'IBM Plex Mono',monospace}
.btn-upload:hover{opacity:.85}
.upload-hint{font-size:.75rem;color:var(--muted);margin-top:.5rem;font-family:'IBM Plex Mono',monospace}
.msg-ok{background:#1a3a1a;border:1px solid var(--green);color:var(--green);border-radius:6px;padding:.75rem 1rem;margin-bottom:1rem;font-size:.875rem}
.msg-err{background:#3a1a1a;border:1px solid var(--red);color:var(--red);border-radius:6px;padding:.75rem 1rem;margin-bottom:1rem;font-size:.875rem}
.folder-section{margin-bottom:2.5rem}
.folder-title{font-family:'IBM Plex Mono',monospace;font-size:.8rem;color:var(--green);margin-bottom:.75rem;display:flex;align-items:center;gap:.5rem}
.folder-title::before{content:'📁'}
.folder-title.latest::before{content:'⭐'}
table{width:100%;border-collapse:collapse;background:var(--surface);border:1px solid var(--border);border-radius:8px;overflow:hidden}
th{text-align:left;padding:.75rem 1rem;font-size:.75rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);font-family:'IBM Plex Mono',monospace;border-bottom:1px solid var(--border);background:rgba(255,255,255,.02)}
td{padding:.75rem 1rem;font-size:.875rem;border-bottom:1px solid var(--border)}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(88,166,255,.05)}
.filename{font-family:'IBM Plex Mono',monospace;color:var(--accent)}
.filesize{color:var(--muted);font-family:'IBM Plex Mono',monospace;font-size:.8rem}
.btn{display:inline-block;padding:.35rem .9rem;background:transparent;border:1px solid var(--accent);color:var(--accent);border-radius:6px;text-decoration:none;font-size:.8rem;font-family:'IBM Plex Mono',monospace;transition:all .15s}
.btn:hover{background:var(--accent);color:var(--bg)}
.empty{color:var(--muted);font-style:italic;padding:1.5rem;text-align:center}
footer{margin-top:3rem;padding-top:1rem;border-top:1px solid var(--border);font-size:.75rem;color:var(--muted);font-family:'IBM Plex Mono',monospace}
</style>
</head>
<body>
<header>
  <div class="logo">CNE · Comision Nacional de Energia</div>
  <h1>Reportes de Precios de Combustibles</h1>
  <div class="subtitle">Archivos generados automaticamente · Actualizacion periodica</div>
</header>

<div class="stats">
  <div class="stat"><div class="stat-label">Salidas scraper</div><div class="stat-value">{{ total_folders }}</div></div>
  <div class="stat"><div class="stat-label">CSV historicos</div><div class="stat-value">{{ root_count }}</div></div>
  <div class="stat"><div class="stat-label">Reportes PDF</div><div class="stat-value">{{ report_count }}</div></div>
  <div class="stat"><div class="stat-label">Ultima actualizacion</div><div class="stat-value" style="font-size:.9rem">{{ last_update }}</div></div>
</div>

<!-- UPLOAD -->
<div class="upload-section">
  <h2>Subir archivos historicos</h2>
  {% if msg_ok %}<div class="msg-ok">{{ msg_ok }}</div>{% endif %}
  {% if msg_err %}<div class="msg-err">{{ msg_err }}</div>{% endif %}
  <form method="POST" action="/upload" enctype="multipart/form-data">
    <div class="upload-grid">
      <div class="field">
        <label>Archivo (.csv / .xlsx / .pdf)</label>
        <input type="file" name="archivo" accept=".csv,.xlsx,.pdf" required>
      </div>
      <div class="field">
        <label>Destino</label>
        <select name="destino">
          <option value="raiz">Raiz de datos (CSV historicos CNE)</option>
          <option value="reportes">Carpeta reportes/ (PDFs)</option>
        </select>
      </div>
      <div class="field">
        <label>Token de acceso</label>
        <input type="text" name="token" placeholder="Token de seguridad" required>
      </div>
      <div class="field">
        <label>&nbsp;</label>
        <button type="submit" class="btn-upload">Subir archivo</button>
      </div>
    </div>
    <p class="upload-hint">
      Nombre recomendado para CSV: precios_cne_final_YYYYMMDD_HHMMSS.csv
    </p>
  </form>
</div>

<!-- CSV HISTORICOS -->
{% if root_files %}
<div class="folder-section">
  <div class="folder-title latest">CSV historicos en datos/</div>
  <table>
    <thead><tr><th>Archivo</th><th>Tamano</th><th>Fecha</th><th>Accion</th></tr></thead>
    <tbody>
      {% for f in root_files %}
      <tr>
        <td class="filename">{{ f.name }}</td>
        <td class="filesize">{{ f.size }}</td>
        <td class="filesize">{{ f.date }}</td>
        <td><a class="btn" href="/download-root/{{ f.name }}">Descargar</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}

<!-- PDF REPORTES -->
{% if report_files %}
<div class="folder-section">
  <div class="folder-title">Reportes semanales PDF</div>
  <table>
    <thead><tr><th>Archivo</th><th>Tamano</th><th>Fecha</th><th>Accion</th></tr></thead>
    <tbody>
      {% for f in report_files %}
      <tr>
        <td class="filename">{{ f.name }}</td>
        <td class="filesize">{{ f.size }}</td>
        <td class="filesize">{{ f.date }}</td>
        <td><a class="btn" href="/download-report/{{ f.name }}">Descargar</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}

<!-- SALIDAS SCRAPER -->
{% for folder in folders %}
<div class="folder-section">
  <div class="folder-title {% if loop.first %}latest{% endif %}">
    {{ folder.name }}{% if loop.first %} - MAS RECIENTE{% endif %}
  </div>
  {% if folder.files %}
  <table>
    <thead><tr><th>Archivo</th><th>Tamano</th><th>Fecha</th><th>Accion</th></tr></thead>
    <tbody>
      {% for f in folder.files %}
      <tr>
        <td class="filename">{{ f.name }}</td>
        <td class="filesize">{{ f.size }}</td>
        <td class="filesize">{{ f.date }}</td>
        <td><a class="btn" href="/download/{{ folder.name }}/{{ f.name }}">Descargar</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p class="empty">Sin archivos en esta carpeta.</p>
  {% endif %}
</div>
{% endfor %}

{% if not folders and not root_files and not report_files %}
<p class="empty">No hay reportes disponibles todavia. El scraper los generara en la proxima ejecucion.</p>
{% endif %}

<footer>Servidor activo · {{ now }} · {{ data_dir }}</footer>
</body></html>
"""

# ── Helpers ─────────────────────────────────────────────────────────────────

def human_size(n):
    for u in ["B","KB","MB","GB"]:
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} TB"

def list_files(directory, exts=None):
    if not os.path.isdir(directory): return []
    out = []
    for fp in sorted(os.listdir(directory), reverse=True):
        full = os.path.join(directory, fp)
        if not os.path.isfile(full): continue
        if exts and not any(fp.lower().endswith(e) for e in exts): continue
        s = os.stat(full)
        out.append({"name":fp, "size":human_size(s.st_size),
                    "date":datetime.fromtimestamp(s.st_mtime).strftime("%Y-%m-%d %H:%M")})
    return out

def get_folders_data():
    dirs = sorted(glob.glob(os.path.join(DATA_DIR, "salida_*")), reverse=True)
    folders, total = [], 0
    for d in dirs:
        files = []
        for fp in sorted(os.listdir(d)):
            full = os.path.join(d, fp)
            if os.path.isfile(full):
                s = os.stat(full)
                files.append({"name":fp, "size":human_size(s.st_size),
                               "date":datetime.fromtimestamp(s.st_mtime).strftime("%Y-%m-%d %H:%M")})
                total += 1
        folders.append({"name":os.path.basename(d), "files":files})
    last = folders[0]["name"].replace("salida_","") if folders else "—"
    return folders, total, last

def render(msg_ok=None, msg_err=None):
    folders, _, last = get_folders_data()
    root_files   = list_files(DATA_DIR, ['.csv','.xlsx'])
    report_files = list_files(os.path.join(DATA_DIR,'reportes'), ['.pdf'])
    return render_template_string(HTML,
        folders=folders, total_folders=len(folders),
        root_files=root_files, root_count=len(root_files),
        report_files=report_files, report_count=len(report_files),
        last_update=last, now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        data_dir=DATA_DIR, msg_ok=msg_ok, msg_err=msg_err)

# ── Rutas ────────────────────────────────────────────────────────────────────

@app.route("/")
@app.route("/files")
def index():
    return render()

@app.route("/upload", methods=["POST"])
def upload():
    if request.form.get("token","") != UPLOAD_TOKEN:
        return render(msg_err="Token incorrecto. Acceso denegado.")
    if "archivo" not in request.files:
        return render(msg_err="No se selecciono ningun archivo.")
    f = request.files["archivo"]
    if not f or f.filename == "":
        return render(msg_err="Archivo vacio.")
    filename = secure_filename(f.filename)
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXT:
        return render(msg_err=f"Extension no permitida: {ext}. Solo .csv .xlsx .pdf")
    destino  = request.form.get("destino","raiz")
    dest_dir = os.path.join(DATA_DIR, "reportes") if destino == "reportes" else DATA_DIR
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, filename)
    f.save(dest_path)
    size = human_size(os.path.getsize(dest_path))
    return render(msg_ok=f"'{filename}' subido correctamente ({size})")

@app.route("/download/<folder>/<filename>")
def download_file(folder, filename):
    if not folder.startswith("salida_"): abort(403)
    fp = os.path.join(DATA_DIR, folder, filename)
    if not os.path.isfile(fp): abort(404)
    return send_file(fp, as_attachment=True, download_name=filename)

@app.route("/download-root/<filename>")
def download_root(filename):
    filename = secure_filename(filename)
    fp = os.path.join(DATA_DIR, filename)
    if not os.path.isfile(fp): abort(404)
    return send_file(fp, as_attachment=True, download_name=filename)

@app.route("/download-report/<filename>")
def download_report(filename):
    filename = secure_filename(filename)
    fp = os.path.join(DATA_DIR, "reportes", filename)
    if not os.path.isfile(fp): abort(404)
    return send_file(fp, as_attachment=True, download_name=filename)

@app.route("/health")
def health():
    return jsonify({"status":"ok","time":datetime.now().isoformat()})

# ── Email ─────────────────────────────────────────────────────────────────────

def _sg(api_key, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request("https://api.sendgrid.com/v3/mail/send", data=data,
        headers={"Authorization":f"Bearer {api_key}","Content-Type":"application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r: return r.status, r.read().decode()
    except urllib.error.HTTPError as e: return e.code, e.read().decode()

def send_email_report(folder_path, folder_name):
    ak = os.environ.get("SENDGRID_API_KEY")
    fe = os.environ.get("SENDGRID_FROM") or os.environ.get("GMAIL_USER")
    to = os.environ.get("EMAIL_TO", fe)
    if not ak or not fe: print("EMAIL: Sin credenciales"); return
    atts = [os.path.join(folder_path,f) for f in os.listdir(folder_path)
            if "final" in f and "progreso" not in f and (f.endswith(".xlsx") or f.endswith(".csv"))]
    if not atts: print("EMAIL: Sin archivos finales"); return
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN","cne-precios-production.up.railway.app")
    lista  = "\n".join("  - "+os.path.basename(a) for a in atts)
    sg_atts = []
    for fp in atts:
        with open(fp,"rb") as fh: enc = base64.b64encode(fh.read()).decode()
        fn   = os.path.basename(fp)
        mime = ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                if fn.endswith(".xlsx") else "text/csv")
        sg_atts.append({"content":enc,"filename":fn,"type":mime,"disposition":"attachment"})
    payload = {
        "personalizations":[{"to":[{"email":e.strip()} for e in to.split(",")]}],
        "from":{"email":fe},
        "subject":f"CNE Precios - Reporte {folder_name}",
        "content":[{"type":"text/plain","value":
            f"Scraper CNE terminado.\n\nCarpeta: {folder_name}\n\n{lista}\n\n"
            f"Portal: https://{domain}/files"}],
        "attachments":sg_atts,
    }
    s,_ = _sg(ak, payload)
    print(f"Email {'OK' if s in (200,202) else 'ERROR'}: {s}")

def send_email_async(folder_path, folder_name):
    threading.Thread(target=send_email_report, args=(folder_path,folder_name), daemon=True).start()

def send_test_email():
    ak = os.environ.get("SENDGRID_API_KEY")
    fe = os.environ.get("SENDGRID_FROM") or os.environ.get("GMAIL_USER")
    to = os.environ.get("EMAIL_TO", fe)
    if not ak or not fe: return
    domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN","cne-precios-production.up.railway.app")
    payload = {
        "personalizations":[{"to":[{"email":e.strip()} for e in to.split(",")]}],
        "from":{"email":fe},
        "subject":"CNE Precios - Servidor activo",
        "content":[{"type":"text/plain","value":
            f"Sistema CNE Precios activo.\n"
            f"Portal: https://{domain}/files\n"
            f"Subir historicos: https://{domain}/files (seccion Upload)\n"
            f"Token de upload: configurado en variable UPLOAD_TOKEN"}],
    }
    s,_ = _sg(ak, payload)
    print(f"Test email {'OK' if s in (200,202) else 'ERROR'}: {s}")

def auto_shutdown():
    import time, signal
    time.sleep(5.5*3600)
    print("Auto-shutdown")
    os.kill(os.getpid(), signal.SIGTERM)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"File server puerto {port} | datos: {DATA_DIR}")
    t = threading.Thread(target=send_test_email, daemon=False); t.start(); t.join(timeout=20)
    threading.Thread(target=auto_shutdown, daemon=True).start()
    app.run(host="0.0.0.0", port=port, debug=False)
