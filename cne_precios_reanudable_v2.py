#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CNE precios nacional con:
- Corrida NUEVA por defecto en cada ejecución
- Reanudación opcional de una corrida interrumpida con --resume-dir
- Guardado incremental en SQLite
- Exportación automática a Excel y CSV
- Pausa cada 1000 municipios por 5 minutos
- Envío opcional por correo al finalizar
"""

import argparse
import os
import sqlite3
import time
import smtplib
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

import pandas as pd
import requests

PAUSA_CADA = 1000
TIEMPO_DESCANSO = 5 * 60
MAX_REINTENTOS = 3
TIMEOUT_SEG = 15
PAUSA_CORTA_SEG = 0.6

URL_ENTIDADES = "https://api-catalogo.cne.gob.mx/api/utiles/entidadesfederativas"
URL_MUNICIPIOS = "https://api-catalogo.cne.gob.mx/api/utiles/municipios"
URL_PETROLIFEROS = "https://api-reportediario.cne.gob.mx/api/EstacionServicio/Petroliferos"

HEADERS = {
    "accept": "*/*",
    "referer": "https://www.cne.gob.mx/",
    "user-agent": "Mozilla/5.0"
}

DB_NAME = "cne_resume.db"


def get_json(session, url, params=None):
    for intento in range(1, MAX_REINTENTOS + 1):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT_SEG)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"⚠️ Error intento {intento}: {e}")
            time.sleep(3 * intento)
    print("❌ Falló definitivamente")
    return None


def init_db(path):
    conn = sqlite3.connect(path)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS precios (
        fecha TEXT,
        hora TEXT,
        entidad_id TEXT,
        entidad TEXT,
        municipio_id TEXT,
        municipio TEXT,
        permiso TEXT,
        nombre TEXT,
        direccion TEXT,
        producto TEXT,
        subproducto TEXT,
        precio REAL,
        UNIQUE(fecha, hora, entidad_id, municipio_id, permiso, producto, subproducto, precio)
    )
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS progreso (
        entidad_id TEXT,
        entidad TEXT,
        municipio_id TEXT,
        municipio TEXT,
        total_registros INTEGER,
        estado TEXT,
        fecha_proceso TEXT,
        UNIQUE(entidad_id, municipio_id)
    )
    """)
    conn.commit()
    return conn


def insert_precio(db, fecha, hora, entidad_id, entidad, municipio_id, municipio, item):
    db.execute("""
    INSERT OR IGNORE INTO precios VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        fecha,
        hora,
        entidad_id,
        entidad,
        municipio_id,
        municipio,
        item.get("Numero"),
        item.get("Nombre"),
        item.get("Direccion"),
        item.get("Producto"),
        item.get("SubProducto"),
        item.get("PrecioVigente"),
    ))


def upsert_progreso(db, entidad_id, entidad, municipio_id, municipio, total_registros, estado):
    db.execute("""
    INSERT INTO progreso VALUES (?,?,?,?,?,?,?)
    ON CONFLICT(entidad_id, municipio_id) DO UPDATE SET
        entidad=excluded.entidad,
        municipio=excluded.municipio,
        total_registros=excluded.total_registros,
        estado=excluded.estado,
        fecha_proceso=excluded.fecha_proceso
    """, (
        entidad_id,
        entidad,
        municipio_id,
        municipio,
        total_registros,
        estado,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ))


def ya_procesado(db, entidad_id, municipio_id):
    row = db.execute(
        "SELECT 1 FROM progreso WHERE entidad_id=? AND municipio_id=? LIMIT 1",
        (entidad_id, municipio_id)
    ).fetchone()
    return row is not None


def exportar_excel_y_csv(db, outdir, sello, etiqueta="final"):
    precios = pd.read_sql_query("SELECT * FROM precios", db)
    progreso = pd.read_sql_query("SELECT * FROM progreso", db)

    excel_path = outdir / f"precios_cne_{etiqueta}_{sello}.xlsx"
    csv_precios = outdir / f"precios_cne_{etiqueta}_{sello}.csv"
    csv_progreso = outdir / f"progreso_cne_{etiqueta}_{sello}.csv"

    if not precios.empty:
        precios.to_csv(csv_precios, index=False, encoding="utf-8-sig")
    if not progreso.empty:
        progreso.to_csv(csv_progreso, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        precios.to_excel(writer, index=False, sheet_name="precios")
        progreso.to_excel(writer, index=False, sheet_name="progreso")

    print(f"\n📦 Exportación automática ({etiqueta}) generada:")
    print(f"   Excel: {excel_path}")
    print(f"   CSV precios: {csv_precios}")
    print(f"   CSV progreso: {csv_progreso}")

    return excel_path, csv_precios, csv_progreso


def preparar_salida(resume_dir=None):
    base_root = Path(os.getenv("RAILWAY_VOLUME_MOUNT_PATH", Path.cwd()))

    if resume_dir:
        outdir = Path(resume_dir)
        if not outdir.exists():
            raise FileNotFoundError(f"No existe la carpeta para reanudar: {outdir}")
        db_path = outdir / DB_NAME
        if not db_path.exists():
            raise FileNotFoundError(f"No existe la base para reanudar: {db_path}")
        sello = outdir.name.replace("salida_", "")
        print(f"🔁 Reanudando corrida en: {outdir}")
        return outdir, db_path, sello

    now = datetime.now()
    sello = now.strftime("%Y%m%d_%H%M%S")
    outdir = base_root / f"salida_{sello}"
    outdir.mkdir(exist_ok=True)
    db_path = outdir / DB_NAME
    print(f"🆕 Nueva corrida en: {outdir}")
    return outdir, db_path, sello


def enviar_correo(archivos):
    email_user = os.getenv("EMAIL_USER")
    email_pass = os.getenv("EMAIL_PASS")
    destinatario = os.getenv("EMAIL_TO", "ruizlara.roberto@gmail.com")

    if not email_user or not email_pass:
        print("❌ No se enviará correo: faltan EMAIL_USER o EMAIL_PASS")
        return

    msg = EmailMessage()
    msg["Subject"] = "✅ CNE precios terminado"
    msg["From"] = email_user
    msg["To"] = destinatario
    msg.set_content(
        "Proceso terminado correctamente.\n\n"
        "Se adjuntan los archivos generados."
    )

    archivos_adjuntos = 0
    for archivo in archivos:
        archivo = Path(archivo)
        if archivo.exists() and archivo.is_file():
            with open(archivo, "rb") as f:
                msg.add_attachment(
                    f.read(),
                    maintype="application",
                    subtype="octet-stream",
                    filename=archivo.name
                )
            archivos_adjuntos += 1
        else:
            print(f"⚠️ No se encontró archivo para adjuntar: {archivo}")

    if archivos_adjuntos == 0:
        print("⚠️ No se adjuntó ningún archivo; se omite envío de correo")
        return

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
            smtp.login(email_user, email_pass)
            smtp.send_message(msg)
        print("📧 Correo enviado correctamente")
    except Exception as e:
        print(f"❌ Error al enviar correo: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume-dir", help="Ruta de la carpeta salida_... para reanudarla")
    args = parser.parse_args()

    outdir, db_path, sello = preparar_salida(args.resume_dir)
    db = init_db(db_path)

    now = datetime.now()
    fecha = now.strftime("%Y-%m-%d")
    hora = now.strftime("%H:%M:%S")

    session = requests.Session()
    session.headers.update(HEADERS)

    contador_sesion = 0

    entidades = get_json(session, URL_ENTIDADES)
    if not entidades:
        print("❌ No se pudieron obtener entidades.")
        db.close()
        return

    for e in entidades:
        eid = e["EntidadFederativaId"]
        ename = e["Nombre"]

        municipios = get_json(session, URL_MUNICIPIOS, {"EntidadFederativaId": eid})
        if not municipios:
            print(f"⚠️ No se pudieron obtener municipios de {ename}")
            continue

        for m in municipios:
            mid = m["MunicipioId"]
            mname = m["Nombre"]

            if ya_procesado(db, eid, mid):
                print(f"⏭️ Ya procesado: {ename} - {mname}")
                continue

            contador_sesion += 1
            print(f"\n📍 {contador_sesion} | {ename} - {mname}")

            data = get_json(session, URL_PETROLIFEROS, {
                "entidadId": eid,
                "municipioId": mid
            })

            if not data:
                upsert_progreso(db, eid, ename, mid, mname, 0, "error")
                db.commit()
                continue

            registros = data.get("Value", [])
            print(f"   → {len(registros)} registros")

            for item in registros:
                insert_precio(db, fecha, hora, eid, ename, mid, mname, item)

            estado = "ok" if len(registros) > 0 else "cero"
            upsert_progreso(db, eid, ename, mid, mname, len(registros), estado)
            db.commit()

            if contador_sesion % PAUSA_CADA == 0:
                exportar_excel_y_csv(db, outdir, sello, etiqueta=f"snapshot_{contador_sesion}")
                print(f"\n🛑 Pausa larga de {TIEMPO_DESCANSO / 60:.0f} minutos...")
                time.sleep(TIEMPO_DESCANSO)
            else:
                time.sleep(PAUSA_CORTA_SEG)

    excel_path, csv_precios, csv_progreso = exportar_excel_y_csv(db, outdir, sello, etiqueta="final")

    total = db.execute("SELECT COUNT(*) FROM precios").fetchone()[0]
    total_municipios = db.execute("SELECT COUNT(*) FROM progreso").fetchone()[0]

    print("\n✅ PROCESO TERMINADO")
    print(f"   Registros en tabla precios: {total}")
    print(f"   Municipios procesados acumulados en esta corrida: {total_municipios}")
    print(f"   Carpeta de salida: {outdir}")
    print(f"   Base SQLite: {db_path}")

    db.close()

    enviar_correo([
        str(excel_path),
        str(csv_precios),
        str(csv_progreso),
    ])


if __name__ == "__main__":
    main()
