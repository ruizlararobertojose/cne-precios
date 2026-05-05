#!/bin/bash
# start.sh definitivo
# SIEMPRE levanta el web server
# Si RUN_MODE=cron, corre el scraper primero y luego levanta el web server

if [ "$RUN_MODE" = "cron" ]; then
    echo "🕐 Modo CRON — ejecutando scraper primero..."
    python cne_precios_reanudable_v2.py
    echo "✅ Scraper terminado."
fi

echo "🌐 Iniciando web server..."
python file_server.py
