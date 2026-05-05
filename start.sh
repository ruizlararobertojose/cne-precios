#!/bin/bash
# start.sh inteligente
# Detecta si es cron o web server

echo "=== Variables de entorno Railway ==="
echo "RAILWAY_CRON_JOB_ID: ${RAILWAY_CRON_JOB_ID:-NO_DEFINIDA}"
echo "PORT: ${PORT:-NO_DEFINIDA}"
echo "===================================="

# Si NO hay PORT definido → es una corrida de cron
# Railway siempre asigna PORT al web service, pero NO al cron
if [ -z "$PORT" ]; then
    echo "🕐 Modo CRON detectado (sin PORT) — ejecutando scraper..."
    python cne_precios_reanudable_v2.py
    echo "✅ Scraper terminado. Saliendo."
    exit 0
else
    echo "🌐 Modo WEB detectado (PORT=$PORT) — iniciando file server..."
    python file_server.py
fi
