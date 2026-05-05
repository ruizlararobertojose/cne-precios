#!/bin/bash
# start.sh — detecta modo via variable RUN_MODE

echo "RUN_MODE: ${RUN_MODE:-NO_DEFINIDA}"

if [ "$RUN_MODE" = "cron" ]; then
    echo "🕐 Modo CRON — ejecutando scraper..."
    python cne_precios_reanudable_v2.py
    echo "✅ Scraper terminado."
    exit 0
else
    echo "🌐 Modo WEB — iniciando file server..."
    python file_server.py
fi
