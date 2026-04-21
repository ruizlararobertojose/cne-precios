import os
import smtplib
from email.message import EmailMessage


# =========================
# FUNCIÓN PARA ENVIAR CORREO
# =========================
def enviar_correo_con_adjuntos(destinatario, asunto, cuerpo, archivos):
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")

    if not EMAIL_USER or not EMAIL_PASS:
        raise ValueError("Faltan variables EMAIL_USER o EMAIL_PASS")

    msg = EmailMessage()
    msg["From"] = EMAIL_USER
    msg["To"] = destinatario
    msg["Subject"] = asunto
    msg.set_content(cuerpo)

    for ruta in archivos:
        if not os.path.exists(ruta):
            print(f"⚠️ Archivo no encontrado: {ruta}")
            continue

        with open(ruta, "rb") as f:
            contenido = f.read()
            nombre = os.path.basename(ruta)

        msg.add_attachment(
            contenido,
            maintype="application",
            subtype="octet-stream",
            filename=nombre
        )

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)

    print("📧 Correo enviado correctamente")


# =========================
# TU PROCESO (SIMULADO)
# =========================
def main():
    print("🚀 Iniciando proceso...")

    # Aquí va TODO tu código actual de descarga
    # ----------------------------------------
    # NO BORRES tu lógica, solo déjala aquí
    # ----------------------------------------

    # Ejemplo:
    # descargar_datos()
    # generar_excel()
    # guardar_archivos()

    print("✅ PROCESO TERMINADO")

    # =========================
    # BUSCAR ARCHIVOS GENERADOS
    # =========================
    ruta_base = "/app/data"

    archivo_excel = None
    archivo_csv = None
    archivo_progreso = None

    for root, dirs, files in os.walk(ruta_base):
        for f in files:
            ruta_completa = os.path.join(root, f)

            if f.endswith(".xlsx"):
                archivo_excel = ruta_completa

            elif f.endswith(".csv") and "precios" in f:
                archivo_csv = ruta_completa

            elif f.endswith(".csv") and "progreso" in f:
                archivo_progreso = ruta_completa

    archivos = [a for a in [archivo_excel, archivo_csv, archivo_progreso] if a]

    print("📂 Archivos detectados:")
    for a in archivos:
        print(a)

    # =========================
    # ENVIAR CORREO
    # =========================
    enviar_correo_con_adjuntos(
        destinatario="ruizlara.roberto@gmail.com",
        asunto="✅ CNE precios - proceso terminado",
        cuerpo="""
Proceso terminado correctamente.

Se adjuntan los archivos generados.
""",
        archivos=archivos
    )


# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    main()
