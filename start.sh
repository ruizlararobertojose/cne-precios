#!/bin/bash
# start.sh — Arranca el file server y luego el scraper

echo "🚀 Iniciando sistema CNE Precios..."

# 1. Lanzar servidor web en background
echo "🌐 Arrancando file server..."
python file_server.py &
WEB_PID=$!
sleep 2

if kill -0 $WEB_PID 2>/dev/null; then
    echo "✅ Web server activo (PID: $WEB_PID)"
else
    echo "❌ Web server no pudo iniciar"
fi

# 2. Ejecutar scraper
echo ""
echo "⚙️  Ejecutando scraper CNE..."
python cne_precios_reanudable_v2.py

echo ""
echo "📋 Scraper terminado. Web server sigue activo."

# 3. Mantener el web server vivo
wait $WEB_PID
