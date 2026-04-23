from flask import Flask, send_file
import os
from pathlib import Path

app = Flask(__name__)

BASE_DIR = Path("/app/data")


def get_latest_file(extension=".xlsx"):
    files = list(BASE_DIR.rglob(f"*{extension}"))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


@app.route("/")
def home():
    return "Servidor activo"


@app.route("/download/latest")
def download_latest():
    file_path = get_latest_file(".xlsx")
    if not file_path:
        return "No hay archivos aún"
    return send_file(file_path, as_attachment=True)


@app.route("/download/csv")
def download_csv():
    file_path = get_latest_file(".csv")
    if not file_path:
        return "No hay CSV aún"
    return send_file(file_path, as_attachment=True)
