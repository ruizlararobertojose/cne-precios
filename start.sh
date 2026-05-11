#!/bin/bash
# start.sh — Railway entrypoint
# Modos:
#   RUN_MODE=cron      → corre scraper (8am y 6pm Mexico)
#   RUN_MODE=weekly    → corre reporte semanal McKinsey (jueves 19:30 Mexico)
#   (default)          → levanta servidor web Flask

set -e
echo "=== CNE Precios — $(date) ==="
echo "RUN_MODE=${RUN_MODE:-web}"

if [ "$RUN_MODE" = "cron" ]; then
    echo "Modo CRON — ejecutando scraper..."
    python cne_precios_reanudable_v2.py
    echo "Scraper terminado."

elif [ "$RUN_MODE" = "weekly" ]; then
    echo "Modo WEEKLY — generando reporte semanal McKinsey..."
    python reporte_semanal.py
    echo "Reporte semanal terminado."
fi

echo "Iniciando web server..."
python file_server.py
