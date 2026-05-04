#!/bin/bash
# start.sh inteligente
# - Si Railway lo llama como CRON → corre solo el scraper y termina
# - Si Railway lo llama como WEB  → corre Flask permanentemente

# Railway pone RAILWAY_CRON_JOB_ID cuando es una corrida de cron
if [ -n "$RAILWAY_CRON_JOB_ID" ]; then
    echo "🕐 Modo CRON detectado — ejecutando scraper..."
    python cne_precios_reanudable_v2.py
    echo "✅ Scraper terminado. Saliendo."
    exit 0
else
    echo "🌐 Modo WEB detectado — iniciando file server..."
    python file_server.py
fi
