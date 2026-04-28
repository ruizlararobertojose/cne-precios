"""
file_server.py — Servidor web para descargar reportes CNE
Se ejecuta junto al scraper en el mismo servicio Railway.
"""

import os
import glob
import smtplib
import threading
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime
from flask import Flask, render_template_string, send_file, abort, jsonify

app = Flask(__name__)

DATA_DIR = os.environ.get("DATA_DIR", "/app/data")

HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>CNE — Reportes de Precios</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
    :root { --bg:#0d1117;--surface:#161b22;--border:#30363d;--accent:#58a6ff;--green:#3fb950;--text:#e6edf3;--muted:#8b949e; }
    *{box-sizing:border-box;margin:0;padding:0}
    body{background:var(--bg);color:var(--text);font-family:'IBM Plex Sans',sans-serif;min-height:100vh;padding:2rem}
    header{border-bottom:1px solid var(--border);padding-bottom:1.5rem;margin-bottom:2rem}
    .logo{font-family:'IBM Plex Mono',monospace;font-size:.75rem;color:var(--accent);letter-spacing:.2em;text-transform:uppercase;margin-bottom:.5rem}
    h1{font-size:1.6rem;font-weight:600}
    .subtitle{color:var(--muted);font-size:.9rem;margin-top:.3rem}
    .stats{display:flex;gap:2rem;margin-bottom:2rem;flex-wrap:wrap}
    .stat{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:1rem 1.5rem;min-width:160px}
    .stat-label{font-size:.75rem;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;font-family:'IBM Plex Mono',monospace}
    .stat-value{font-size:1.4rem;font-weight:600;color:var(--accent);margin-top:.25rem}
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
  <div class="logo">CNE · Comisión Nacional de Energía</div>
  <h1>Reportes de Precios de Combustibles</h1>
  <div class="subtitle">Archivos generados automáticamente · Actualización periódica</div>
</header>
<div class="stats">
  <div class="stat"><div class="stat-label">Carpetas</div><div class="stat-value">{{ total_folders }}</div></div>
  <div class="stat"><div class="stat-label">Archivos totales</div><div class="stat-value">{{ total_files }}</div></div>
  <div class="stat"><div class="stat-label">Última actualización</div><div class="stat-value" style="font-size:.9rem">{{ last_update }}</div></div>
</div>
{% for folder in folders %}
<div class="folder-section">
  <div class="folder-title {% if loop.first %}latest{% endif %}">
    {{ folder.name }}{% if loop.first %} — MÁS RECIENTE{% endif %}
  </div>
  {% if folder.files %}
  <table>
    <thead><tr><th>Archivo</th><th>Tamaño</th><th>Fecha</th><th>Acción</th></tr></thead>
    <tbody>
      {% for f in folder.files %}
      <tr>
        <td class="filename">{{ f.name }}</td>
        <td class="filesize">{{ f.size }}</td>
        <td class="filesize">{{ f.date }}</td>
        <td><a class="btn" href="/download/{{ folder.name }}/{{ f.name }}">↓ Descargar</a></td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
  <p class="empty">Sin archivos en esta carpeta.</p>
  {% endif %}
</div>
{% endfor %}
{% if not folders %}
<p class="empty">No hay reportes disponibles todavía. El scraper los generará en la próxima ejecución.</p>
{% endif %}
<footer>Servidor activo · {{ now }} · {{ data_dir }}</footer>
</body>
</html>
"""

def human_size(num_bytes):
    for unit in ["B","KB","MB","GB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"

def get_folders_data():
    pattern = os.path.join(DATA_DIR, "salida_*")
    dirs = sorted(glob.glob(pattern), reverse=True)
    folders = []
    total_files = 0
    for d in dirs:
        folder_name = os.path.basename(d)
        files = []
        for fp in sorted(os.listdir(d)):
            full = os.path.join(d, fp)
            if os.path.isfile(full):
                stat = os.stat(full)
                files.append({
                    "name": fp,
                    "size": human_size(stat.st_size),
                    "date": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                })
                total_files += 1
        folders.append({"name": folder_name, "files": files})
    last_update = folders[0]["name"].replace("salida_","") if folders else "—"
    return folders, total_files, last_update

@app.route("/")
@app.route("/files")
def index():
    folders, total_files, last_update = get_folders_data()
    return render_template_string(HTML,
        folders=folders, total_folders=len(folders),
        total_files=total_files, last_update=last_update,
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), data_dir=DATA_DIR)

@app.route("/download/<folder>/<filename>")
def download_file(folder, filename):
    if not folder.startswith("salida_"):
        abort(403)
    filepath = os.path.join(DATA_DIR, folder, filename)
    if not os.path.isfile(filepath):
        abort(404)
    return send_file(filepath, as_attachment=True, download_name=filename)

@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})

# ─────────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────────

def send_email_report(folder_path: str, folder_name: str):
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_pass = os.environ.get("GMAIL_APP_PASS")
    email_to   = os.environ.get("EMAIL_TO", gmail_user)

    if not gmail_user or not gmail_pass:
        print("⚠️  EMAIL: Variables GMAIL_USER / GMAIL_APP_PASS no configuradas.")
        return

    attachments = []
    for f in os.listdir(folder_path):
        fp = os.path.join(folder_path, f)
        if os.path.isfile(fp) and "final" in f and "progreso" not in f:
            if f.endswith(".xlsx") or f.endswith(".csv"):
                attachments.append(fp)

    if not attachments:
        print("⚠️  EMAIL: No se encontraron archivos finales.")
        return

    public_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "tu-servicio.railway.app")

    msg = MIMEMultipart()
    msg["From"]    = gmail_user
    msg["To"]      = email_to
    msg["Subject"] = f"📊 CNE Precios — Reporte {folder_name}"
    body = f"""Hola,

El scraper de precios CNE terminó correctamente.

📁 Carpeta: {folder_name}
📎 Archivos adjuntos: {len(attachments)}

Archivos incluidos:
{chr(10).join('  • ' + os.path.basename(a) for a in attachments)}

También puedes descargar todos los reportes desde:
https://{public_domain}/files

---
Mensaje automático · Sistema CNE Precios""".strip()

    msg.attach(MIMEText(body, "plain"))

    for filepath in attachments:
        with open(filepath, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(filepath)}"')
        msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, email_to.split(","), msg.as_string())
        print(f"✅ EMAIL: Reporte enviado a {email_to}")
    except Exception as e:
        print(f"❌ EMAIL: Error al enviar — {e}")

def send_email_async(folder_path: str, folder_name: str):
    t = threading.Thread(target=send_email_report, args=(folder_path, folder_name), daemon=True)
    t.start()


def send_test_email():
    """Envía un email de prueba al arrancar el servidor para verificar configuración."""
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_pass = os.environ.get("GMAIL_APP_PASS")
    email_to   = os.environ.get("EMAIL_TO", gmail_user)

    if not gmail_user or not gmail_pass:
        print("⚠️  TEST EMAIL: Variables no configuradas, omitiendo.")
        return

    msg = MIMEMultipart()
    msg["From"]    = gmail_user
    msg["To"]      = email_to
    msg["Subject"] = "✅ CNE Precios — Servidor activo y email funcionando"

    body = f"""Hola,

Este es un email de prueba automático del sistema CNE Precios.

Si recibes este mensaje, el envío de correos está funcionando correctamente.

🌐 Portal de descargas:
https://{os.environ.get('RAILWAY_PUBLIC_DOMAIN', 'cne-precios-production.up.railway.app')}/files

⏰ Hora de inicio del servidor: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Recibirás un correo con los archivos adjuntos cada vez que el scraper termine su ejecución.

---
Mensaje automático · Sistema CNE Precios""".strip()

    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, email_to.split(","), msg.as_string())
        print(f"✅ TEST EMAIL: Email de prueba enviado a {email_to}")
    except Exception as e:
        print(f"❌ TEST EMAIL: Error — {e}")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"🌐 File server en puerto {port} | datos: {DATA_DIR}")
    # Enviar email de prueba al arrancar (en background)
    t = threading.Thread(target=send_test_email, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=port, debug=False)
