#!/usr/bin/env python3
"""
reporte_semanal.py
Genera el reporte semanal de inteligencia de mercado de combustibles CNE.
- Corre los jueves después del corte de las 18hrs (via start.sh)
- Descarga contexto macro: tipo de cambio Banxico, precio WTI, noticias Google News
- Usa Claude API para redactar la narrativa McKinsey
- Genera PDF profesional y lo envía por correo vía SendGrid
"""

import os, sys, glob, json, re, io, datetime, logging, time
import urllib.request, urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

import anthropic  # pip install anthropic

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, HRFlowable, Image as RLImage, PageBreak)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────
DATA_DIR     = Path(os.getenv('DATA_DIR', '/app/data'))
FIGS_DIR     = DATA_DIR / 'figs'
REPORTS_DIR  = DATA_DIR / 'reportes'
ANTHROPIC_KEY= os.getenv('ANTHROPIC_API_KEY', '')
SENDGRID_KEY = os.getenv('SENDGRID_API_KEY', '')
SENDGRID_FROM= os.getenv('SENDGRID_FROM', '')
EMAIL_TO     = os.getenv('EMAIL_TO', '')
AUTOR        = 'José Roberto Ruiz Lara'

MLIM = 24.00
DLIM = 27.00

FRONTERA_M = {
    'Ciudad Juárez','Juárez','Ojinaga','Janos','Piedras Negras','Acuña','Nava',
    'Nuevo Laredo','Matamoros','Reynosa','Río Bravo','Valle Hermoso','Miguel Alemán',
    'Nogales','Agua Prieta','San Luis Río Colorado','Caborca',
    'General Plutarco Elías Calles','Mexicali','Tijuana','Ensenada','Tecate',
}

for d in [FIGS_DIR, REPORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════
# 1. CARGA Y DEPURACIÓN DE DATOS
# ══════════════════════════════════════════════════════════════════════════

def load_and_clean(csv_path: Path):
    """Carga CSV CNE y devuelve magna y diesel depurados (sin frontera ni fantasmas)."""
    df = pd.read_csv(csv_path)
    magna  = df[df['subproducto'].str.contains('87', na=False)].copy()
    diesel = df[df['subproducto'].str.contains('Ultra Bajo', na=False)].copy()
    magna['frontera']  = magna['municipio'].isin(FRONTERA_M)
    diesel['frontera'] = diesel['municipio'].isin(FRONTERA_M)
    ghost = set(magna[(magna['frontera']) & (magna['precio'] < 22)]['permiso'].unique())
    mc = magna[~magna['permiso'].isin(ghost) & ~magna['frontera']]
    dc = diesel[~diesel['frontera']]
    return mc, dc, ghost, magna[magna['frontera']], diesel[diesel['frontera']]


def get_last_n_cortes(n=4):
    """Devuelve los últimos N archivos CSV del DATA_DIR, ordenados por fecha."""
    csvs = sorted(DATA_DIR.glob('precios_cne_final_*.csv'), reverse=True)
    # También incluye snapshots con otro nombre
    csvs += sorted(DATA_DIR.glob('precios_cne_snapshot_*.csv'), reverse=True)
    csvs = sorted(set(csvs), key=lambda x: x.name, reverse=True)
    return csvs[:n]


def build_trend(cortes):
    """Devuelve dict {label: {magna_avg, diesel_avg}} para cada corte."""
    trend = {}
    for path in reversed(cortes):
        mc, dc, _, _, _ = load_and_clean(path)
        # Extraer fecha del nombre de archivo
        m = re.search(r'(\d{8})', path.name)
        if m:
            d = m.group(1)
            label = f"{d[6:8]}/{d[4:6]}/{d[0:4]}"
        else:
            label = path.stem
        trend[label] = {
            'magna': round(float(mc['precio'].mean()), 2),
            'diesel': round(float(dc['precio'].mean()), 2),
            'n_magna': len(mc),
            'n_diesel': len(dc),
        }
    return trend


# ══════════════════════════════════════════════════════════════════════════
# 2. CONTEXTO MACRO
# ══════════════════════════════════════════════════════════════════════════

def get_tipo_cambio():
    """Obtiene tipo de cambio MXN/USD desde Banxico (API pública, sin key)."""
    try:
        # Serie SF43718 = tipo de cambio FIX diario
        url = ('https://www.banxico.org.mx/SieAPIRest/service/v1/series/'
               'SF43718/datos/oportuno?token=')
        token = os.getenv('BANXICO_TOKEN', '')  # opcional, funciona sin token para datos recientes
        req = urllib.request.Request(url + token, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
        series = data['bmx']['series'][0]['datos']
        latest = series[-1]
        return float(latest['dato'].replace(',', '.')), latest['fecha']
    except Exception as e:
        log.warning(f"Banxico error: {e}")
        # Fallback: scrape de fixer o retornar None
        return None, None


def get_wti_price():
    """Obtiene precio WTI desde EIA (API pública, sin key para datos semanales)."""
    try:
        url = ('https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key='
               + os.getenv('EIA_API_KEY', 'DEMO_KEY')
               + '&frequency=weekly&data[0]=value&facets[product][]=EPCWTI'
               + '&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=2')
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        rows = data['response']['data']
        if rows:
            return float(rows[0]['value']), rows[0]['period']
    except Exception as e:
        log.warning(f"EIA error: {e}")
    return None, None


def get_google_news(queries, max_per_query=3):
    """Busca noticias en Google News RSS. Sin API key."""
    noticias = []
    for query in queries:
        try:
            q = urllib.parse.quote(query)
            url = f'https://news.google.com/rss/search?q={q}&hl=es-419&gl=MX&ceid=MX:es-419'
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (compatible; CNEBot/1.0)'
            })
            with urllib.request.urlopen(req, timeout=15) as r:
                xml_data = r.read()
            root = ET.fromstring(xml_data)
            items = root.findall('.//item')[:max_per_query]
            for item in items:
                title = item.findtext('title', '').strip()
                pub   = item.findtext('pubDate', '').strip()
                link  = item.findtext('link', '').strip()
                source= item.findtext('source', '').strip()
                if title:
                    noticias.append({
                        'titulo': title,
                        'fuente': source,
                        'fecha':  pub[:16] if pub else '',
                        'link':   link,
                        'query':  query,
                    })
        except Exception as e:
            log.warning(f"Google News error ({query}): {e}")
    # Deduplicar por título
    seen = set()
    unique = []
    for n in noticias:
        key = n['titulo'][:60]
        if key not in seen:
            seen.add(key)
            unique.append(n)
    return unique[:12]


def get_macro_context():
    """Reúne todo el contexto macro de la semana."""
    log.info("Obteniendo contexto macro...")
    tc, tc_fecha   = get_tipo_cambio()
    wti, wti_fecha = get_wti_price()

    noticias = get_google_news([
        'gasolina precio Mexico 2026',
        'combustibles SENER CRE Mexico 2026',
        'pacto voluntario gasolina Mexico',
        'diesel precio Mexico 2026',
        'tipo de cambio peso dolar energia Mexico',
        'Pemex precios referencia combustibles',
    ])

    return {
        'tipo_cambio': tc,
        'tipo_cambio_fecha': tc_fecha,
        'wti': wti,
        'wti_fecha': wti_fecha,
        'noticias': noticias,
    }


# ══════════════════════════════════════════════════════════════════════════
# 3. NARRATIVA McKINSEY CON CLAUDE API
# ══════════════════════════════════════════════════════════════════════════

def build_claude_prompt(stats_actual, trend, macro, corte_fecha):
    """Construye el prompt para Claude con todos los datos del periodo."""

    noticias_txt = '\n'.join([
        f"- [{n['fuente']}] {n['titulo']} ({n['fecha']})"
        for n in macro['noticias']
    ]) or 'No se obtuvieron noticias esta semana.'

    trend_txt = '\n'.join([
        f"  {fecha}: Magna ${v['magna']:.2f}/l | Diésel ${v['diesel']:.2f}/l"
        for fecha, v in trend.items()
    ])

    m = stats_actual['magna']
    d = stats_actual['diesel']

    prompt = f"""Eres un analista senior de mercados energéticos en México, estilo McKinsey & Company.
Tu tarea es redactar la sección de análisis narrativo para un reporte semanal de inteligencia 
de precios de combustibles, dirigido a tomadores de decisiones en energía, regulación y negocios.

El análisis debe ser perspicaz, directo y con opinión propia — no solo descripción de números.
Usa lenguaje ejecutivo. Conecta los datos con el contexto macro y las noticias de la semana.
Identifica patrones, riesgos y oportunidades que no son obvios en los números solos.

═══ DATOS DEL CORTE: {corte_fecha} ═══

GASOLINA MAGNA (límite pacto voluntario: $24.00/l):
- Promedio nacional (mercado interior limpio): ${m['nat']:.2f}/l
- Margen vs. límite: {'+' if m['nat']>=24 else '-'}${abs(m['nat']-24):.2f}/l
- Estados que cumplen: {m['comply']}/32
- Estado más caro: {m['worst_state']} (${m['worst_price']:.2f}/l, +${m['worst_price']-24:.2f})
- Estado más barato: {m['best_state']} (${m['best_price']:.2f}/l)
- Municipio extremo: {m['worst_mun']} (${m['worst_mun_price']:.2f}/l)

DIÉSEL DUBA (límite pacto voluntario: $27.00/l):
- Promedio nacional (mercado interior limpio): ${d['nat']:.2f}/l  
- Brecha vs. límite: +${d['nat']-27:.2f}/l (TODOS los estados incumplen)
- Estados que cumplen: 0/{d['n_states']}
- Estado menos alejado: {d['best_state']} (${d['best_price']:.2f}/l, +${d['best_price']-27:.2f})
- Estado más alejado: {d['worst_state']} (${d['worst_price']:.2f}/l, +${d['worst_price']-27:.2f})
- Municipio extremo: {d['worst_mun']} (${d['worst_mun_price']:.2f}/l)

TENDENCIA (últimas semanas, mercado interior limpio):
{trend_txt}

═══ CONTEXTO MACRO ═══
- Tipo de cambio MXN/USD: {'$'+str(macro['tipo_cambio'])+' ('+str(macro['tipo_cambio_fecha'])+')' if macro['tipo_cambio'] else 'No disponible'}
- Precio WTI: {'$'+str(macro['wti'])+' USD/barril ('+str(macro['wti_fecha'])+')' if macro['wti'] else 'No disponible'}

NOTICIAS DE LA SEMANA:
{noticias_txt}

═══ INSTRUCCIONES ═══

Redacta el análisis en ESPAÑOL con estas 5 secciones exactas. Usa XML tags para delimitarlas:

<resumen_ejecutivo>
4 párrafos estilo McKinsey. Párrafo 1: la historia central del periodo (qué está pasando realmente 
en el mercado, más allá de los promedios). Párrafo 2: el factor geográfico y sus implicaciones 
de política. Párrafo 3: la lectura del diésel y qué dice sobre la política energética. 
Párrafo 4: la señal de los datos y qué debe hacer la autoridad.
No uses viñetas. Prosa fluida y directa. Máximo 350 palabras en total.
</resumen_ejecutivo>

<contexto_semana>
2-3 párrafos conectando las noticias de la semana con el comportamiento de los precios.
¿Qué pasó en los mercados internacionales o en la política energética que explica (o contradice)
lo que vemos en los datos? Sé específico, cita las noticias si son relevantes. 
Si no hay noticias relevantes, di qué debería estar mirando el lector la siguiente semana.
Máximo 200 palabras.
</contexto_semana>

<hallazgo_semana>
Un solo hallazgo contraintuitivo o poco obvio que emerge de los datos de esta semana.
Algo que sorprende, que contradice la narrativa oficial, o que anticipa un riesgo.
Formato: título en negrita + 3-4 líneas de explicación. Máximo 80 palabras.
</hallazgo_semana>

<perspectiva>
¿Qué esperar la siguiente semana? Basado en la tendencia, el tipo de cambio y el contexto macro,
¿hacia dónde van los precios? ¿Qué evento o variable podría cambiar el escenario?
2 párrafos cortos. Máximo 120 palabras.
</perspectiva>

<nota_metodologica>
1 párrafo breve explicando la depuración de datos (250 fantasmas, 396 fronterizos) y por qué
los promedios de este reporte son distintos a los que publica la CNE directamente.
Máximo 80 palabras.
</nota_metodologica>
"""
    return prompt


def get_claude_narrative(stats_actual, trend, macro, corte_fecha):
    """Llama a Claude API y devuelve las secciones narrativas."""
    if not ANTHROPIC_KEY:
        log.warning("Sin ANTHROPIC_API_KEY — usando narrativa placeholder")
        return get_placeholder_narrative()

    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    prompt = build_claude_prompt(stats_actual, trend, macro, corte_fecha)

    log.info("Llamando a Claude API para generar narrativa...")
    try:
        msg = client.messages.create(
            model='claude-sonnet-4-20250514',
            max_tokens=2000,
            messages=[{'role': 'user', 'content': prompt}]
        )
        text = msg.content[0].text
        log.info(f"Claude respondió ({len(text)} chars)")
        return parse_narrative(text)
    except Exception as e:
        log.error(f"Claude API error: {e}")
        return get_placeholder_narrative()


def parse_narrative(text):
    """Extrae las secciones XML del texto de Claude."""
    sections = {}
    tags = ['resumen_ejecutivo','contexto_semana','hallazgo_semana','perspectiva','nota_metodologica']
    for tag in tags:
        m = re.search(rf'<{tag}>(.*?)</{tag}>', text, re.DOTALL)
        sections[tag] = m.group(1).strip() if m else ''
    return sections


def get_placeholder_narrative():
    """Narrativa de fallback si Claude no está disponible."""
    return {
        'resumen_ejecutivo': 'Narrativa no disponible — verificar ANTHROPIC_API_KEY.',
        'contexto_semana': 'Contexto no disponible.',
        'hallazgo_semana': 'Hallazgo no disponible.',
        'perspectiva': 'Perspectiva no disponible.',
        'nota_metodologica': 'Ver metodología en página 6.',
    }


def compute_stats(mc, dc):
    """Calcula estadísticas clave para el prompt de Claude."""
    m_by_state = mc.groupby('entidad')['precio'].mean().round(2)
    d_by_state = dc.groupby('entidad')['precio'].mean().round(2)

    m_worst = m_by_state.idxmax(); m_best = m_by_state.idxmin()
    d_worst = d_by_state.idxmax(); d_best = d_by_state.idxmin()

    mun_m = mc.groupby(['entidad','municipio'])['precio'].mean().round(2)
    mun_d = dc.groupby(['entidad','municipio'])['precio'].mean().round(2)

    return {
        'magna': {
            'nat': round(float(mc['precio'].mean()), 2),
            'comply': int((m_by_state <= MLIM).sum()),
            'worst_state': m_worst, 'worst_price': float(m_by_state[m_worst]),
            'best_state':  m_best,  'best_price':  float(m_by_state[m_best]),
            'worst_mun':   mun_m.idxmax()[1], 'worst_mun_price': float(mun_m.max()),
        },
        'diesel': {
            'nat': round(float(dc['precio'].mean()), 2),
            'n_states': len(d_by_state),
            'worst_state': d_worst, 'worst_price': float(d_by_state[d_worst]),
            'best_state':  d_best,  'best_price':  float(d_by_state[d_best]),
            'worst_mun':   mun_d.idxmax()[1], 'worst_mun_price': float(mun_d.max()),
        },
    }


# ══════════════════════════════════════════════════════════════════════════
# 4. GENERACIÓN DE GRÁFICAS
# ══════════════════════════════════════════════════════════════════════════

GREEN='#2E7D32'; TEAL='#00796B'; RED='#C62828'; AMBER='#F57F17'
BLUE='#1565C0'; DGRAY='#757575'; WHITE='#FFFFFF'; LGRAY='#FAFAFA'; DARK='#1a1a2e'

def make_trend_chart(trend):
    labels = list(trend.keys())
    magna_vals  = [v['magna']  for v in trend.values()]
    diesel_vals = [v['diesel'] for v in trend.values()]

    fig, (ax1,ax2) = plt.subplots(1,2,figsize=(11,4))
    for ax,vals,color,limit,title in [
        (ax1,magna_vals, BLUE, MLIM,'Magna ($/l)'),
        (ax2,diesel_vals,TEAL, DLIM,'Diésel DUBA ($/l)'),
    ]:
        ax.plot(labels,vals,'o-',color=color,lw=2.5,ms=9,zorder=3)
        for x,y in zip(labels,vals):
            ax.annotate(f'${y:.2f}',(x,y),textcoords='offset points',
                        xytext=(0,12),ha='center',fontsize=10,fontweight='bold',color=DARK)
        ax.axhline(limit,color=AMBER,lw=1.8,ls='--',zorder=2,label=f'Límite ${limit}')
        ypad=0.5; ymin=min(vals)-ypad; ymax=max(vals)+ypad+0.4
        ax.set_ylim(ymin,ymax)
        ax.fill_between(labels,limit,max(ymax,limit+0.1),color='#FFEBEE',alpha=0.3,zorder=1)
        ax.fill_between(labels,ymin,limit,color='#E8F5E9',alpha=0.3,zorder=1)
        ax.set_title(title,fontsize=11,fontweight='bold',color=DARK,pad=8)
        ax.set_facecolor(LGRAY); ax.tick_params(labelsize=8,rotation=15)
        ax.grid(axis='y',alpha=0.3); ax.set_ylabel('$/litro',fontsize=9,color=DGRAY)
        ax.legend(fontsize=8)
    fig.suptitle('Evolución precio nacional — mercado interior limpio',
                 fontsize=11,fontweight='bold',color=DARK,y=1.01)
    fig.patch.set_facecolor(WHITE)
    plt.tight_layout()
    out = FIGS_DIR / 'trend.png'
    plt.savefig(out,dpi=160,bbox_inches='tight'); plt.close()
    return out


def make_bars(state_df, limit, nat, title, c_ok, c_over, fname):
    fig,ax = plt.subplots(figsize=(9,7.5))
    labels = state_df['estado'].str.replace(' de Ignacio de la Llave','').str.replace(
        ' de Zaragoza','').str.replace(' de Ocampo','')
    cols = [c_ok if v<=limit else c_over for v in state_df['avg']]
    bars = ax.barh(labels, state_df['avg'], color=cols, height=0.72, zorder=2)
    ax.axvline(limit,color=AMBER,lw=1.8,ls='--',zorder=3,label=f'Límite ${limit}')
    ax.axvline(nat,color=BLUE,lw=1.2,ls=':',zorder=3,label=f'Prom. nac. ${nat:.2f}')
    ax.set_xlim(state_df['avg'].min()-0.3, state_df['avg'].max()+0.5)
    for bar,val in zip(bars,state_df['avg']):
        ax.text(val+0.02,bar.get_y()+bar.get_height()/2,f'${val:.2f}',
                va='center',fontsize=7.5,color=DARK)
    ax.set_title(title,fontsize=11,fontweight='bold',color=DARK,pad=10)
    ax.tick_params(labelsize=8.5); ax.grid(axis='x',alpha=0.3,zorder=0)
    ax.set_facecolor(LGRAY); fig.patch.set_facecolor(WHITE)
    leg=[mpatches.Patch(color=c_ok,label='Cumple'),
         mpatches.Patch(color=c_over,label='Sobre límite'),
         plt.Line2D([0],[0],color=AMBER,ls='--',lw=1.5,label=f'Límite ${limit}'),
         plt.Line2D([0],[0],color=BLUE,ls=':',lw=1.2,label=f'Prom. nac. ${nat:.2f}')]
    ax.legend(handles=leg,fontsize=8,loc='lower right')
    plt.tight_layout()
    out = FIGS_DIR / fname
    plt.savefig(out,dpi=155,bbox_inches='tight'); plt.close()
    return out


# ══════════════════════════════════════════════════════════════════════════
# 5. GENERACIÓN DEL PDF
# ══════════════════════════════════════════════════════════════════════════

def build_pdf(mc, dc, trend, narrative, macro, corte_fecha, out_path):
    W, H = A4
    TW = W - 30*mm

    C = {
        'dark':  colors.HexColor('#1a1a2e'),
        'blue':  colors.HexColor('#1565C0'),
        'teal':  colors.HexColor('#00796B'),
        'green': colors.HexColor('#2E7D32'),
        'amber': colors.HexColor('#F57F17'),
        'red':   colors.HexColor('#C62828'),
        'lgray': colors.HexColor('#F5F5F5'),
        'mgray': colors.HexColor('#E0E0E0'),
        'dgray': colors.HexColor('#757575'),
        'lgreen':colors.HexColor('#E8F5E9'),
        'lred':  colors.HexColor('#FFEBEE'),
        'lblue': colors.HexColor('#E3F2FD'),
        'lamber':colors.HexColor('#FFF8E1'),
        'lteal': colors.HexColor('#E0F2F1'),
        'white': colors.white,
    }

    _sc = {}
    def S(name,size=10,color='dark',bold=False,align=TA_LEFT,sb=0,sa=4,ld=None):
        col = C.get(color, color) if isinstance(color,str) else color
        key = (name,size,str(col),bold,align,sb,sa,ld)
        if key not in _sc:
            _sc[key] = ParagraphStyle(f's{len(_sc)}',fontSize=size,textColor=col,
                fontName='Helvetica-Bold' if bold else 'Helvetica',
                alignment=align,spaceBefore=sb,spaceAfter=sa,
                leading=ld if ld else size*1.35)
        return _sc[key]

    def hr(c='mgray',t=0.5,s=4):
        return HRFlowable(width='100%',thickness=t,color=C[c],spaceAfter=s,spaceBefore=s)

    def im(path,w,h): return RLImage(str(path),width=w,height=h)

    def hbar(text, bg='dark', size=9):
        t=Table([[Paragraph(f'<font color="white"><b>{text}</b></font>',S('hb',size,'white',True))]],
                colWidths=[TW])
        t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),C[bg]),
            ('LEFTPADDING',(0,0),(-1,-1),10),('TOPPADDING',(0,0),(-1,-1),5),
            ('BOTTOMPADDING',(0,0),(-1,-1),5),('ROUNDEDCORNERS',[4])]))
        return t

    def kpis(items):
        n=len(items); cw=TW/n
        rv=[Paragraph(v,S(f'kv{i}',14,'dark',True,TA_CENTER)) for i,(v,l,bg) in enumerate(items)]
        rl=[Paragraph(l.replace('\n','<br/>'),S(f'kl{i}',7.5,'dgray',False,TA_CENTER)) for i,(v,l,bg) in enumerate(items)]
        t=Table([rv,rl],colWidths=[cw]*n,rowHeights=[20,18])
        ts=TableStyle([('ALIGN',(0,0),(-1,-1),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
                       ('TOPPADDING',(0,0),(-1,0),6),('BOTTOMPADDING',(0,1),(-1,1),6)])
        for i,(_,_,bg) in enumerate(items): ts.add('BACKGROUND',(i,0),(i,-1),C[bg])
        t.setStyle(ts); return t

    def callout(title,body,border='blue',bg='lblue'):
        t=Table([[Paragraph(f'<b>{title}</b>',S('ct'+title[:4],9,border,True))],
                 [Paragraph(body,S('cb'+title[:4],9,'dark',False,TA_JUSTIFY,ld=13))]],colWidths=[TW])
        t.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(0,0),C[bg]),('BACKGROUND',(0,1),(0,1),C[bg]),
            ('LINEBEFORE',(0,0),(0,-1),3,C[border]),('BOX',(0,0),(-1,-1),0.3,C['mgray']),
            ('LEFTPADDING',(0,0),(-1,-1),10),('RIGHTPADDING',(0,0),(-1,-1),10),
            ('TOPPADDING',(0,0),(0,0),7),('BOTTOMPADDING',(0,0),(0,0),4),
            ('TOPPADDING',(0,1),(0,1),4),('BOTTOMPADDING',(0,1),(0,1),7),
            ('ROUNDEDCORNERS',[3])]))
        return t

    def narrative_block(text, indent=False):
        """Convierte texto con párrafos separados por \n\n en flowables."""
        items = []
        for para in text.strip().split('\n\n'):
            para = para.strip()
            if not para: continue
            # Negrita para líneas que empiezan con **
            para = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', para)
            align = TA_JUSTIFY
            lm = 10 if indent else 0
            items.append(Paragraph(para, S(f'nb{para[:5]}',9.5,'dark',False,align,sb=0,sa=6,ld=14)))
        return items

    def state_table(state_df, limit, out_dict, tag):
        thdr=[Paragraph(c,S(f'th{tag}{i}',8,'white',True,TA_CENTER))
              for i,c in enumerate(['Estado','Min','Prom','Max','vs límite','Peor municipio','Exceso'])]
        rows=[thdr]
        for _,r in state_df.iterrows():
            diff=round(r['avg']-limit,2)
            e=r['estado']
            mun,mval=out_dict.get(e,('--','--'))
            exc=('+$'+f'{round(mval-limit,2):.2f}' if mval!='--' else '--')
            en=e.replace(' de Ignacio de la Llave','').replace(' de Zaragoza','').replace(' de Ocampo','')
            dc_row='red' if diff>0 else 'green'
            rows.append([
                Paragraph(en,S(f'e{tag}{en[:3]}',8,'dark')),
                Paragraph('$'+f'{r["min"]:.2f}',S(f'mi{tag}{en[:3]}',8,'dgray',False,TA_CENTER)),
                Paragraph('$'+f'{r["avg"]:.2f}',S(f'av{tag}{en[:3]}',8,'dark',True,TA_CENTER)),
                Paragraph('$'+f'{r["max"]:.2f}',S(f'mx{tag}{en[:3]}',8,'dgray',False,TA_CENTER)),
                Paragraph(('+' if diff>=0 else '')+'$'+f'{diff:.2f}',
                          S(f'df{tag}{en[:3]}',8,dc_row,True,TA_CENTER)),
                Paragraph(mun[:26] if mun!='--' else '--',S(f'mu{tag}{en[:3]}',7.5,'dgray',False)),
                Paragraph(exc,S(f'ex{tag}{en[:3]}',8,'red' if mun!='--' else 'dgray',False,TA_CENTER)),
            ])
        cws=[50*mm,17*mm,17*mm,17*mm,18*mm,40*mm,18*mm]
        t=Table(rows,colWidths=cws,repeatRows=1)
        ts=TableStyle([
            ('BACKGROUND',(0,0),(-1,0),C['dark']),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[C['white'],C['lgray']]),
            ('GRID',(0,0),(-1,-1),0.2,C['mgray']),
            ('ALIGN',(1,0),(-1,-1),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3),
            ('LEFTPADDING',(0,0),(-1,-1),5),('RIGHTPADDING',(0,0),(-1,-1),5),
            ('ROUNDEDCORNERS',[3]),
        ])
        for i,(_,r) in enumerate(state_df.iterrows(),1):
            if r['avg']>limit:
                ts.add('BACKGROUND',(4,i),(4,i),C['lred'])
        t.setStyle(ts); return t

    # ── Datos ────────────────────────────────────────────────────────────
    m_nat=round(float(mc['precio'].mean()),2); d_nat=round(float(dc['precio'].mean()),2)
    m_s=mc.groupby('entidad')['precio'].agg(['min','mean','max']).round(2).reset_index()
    m_s.columns=['estado','min','avg','max']; m_s=m_s.sort_values('avg',ascending=False)
    d_s=dc.groupby('entidad')['precio'].agg(['min','mean','max']).round(2).reset_index()
    d_s.columns=['estado','min','avg','max']; d_s=d_s.sort_values('avg',ascending=False)

    def worst_mun(clean_df, limit):
        mun=clean_df.groupby(['entidad','municipio'])['precio'].mean().round(2).reset_index()
        mun.columns=['estado','municipio','avg']; out={}
        for e,sub in mun.groupby('estado'):
            over=sub[sub['avg']>limit].nlargest(1,'avg')
            out[e]=(over.iloc[0]['municipio'],over.iloc[0]['avg']) if len(over)>0 else ('--','--')
        return out

    out_m=worst_mun(mc,MLIM); out_d=worst_mun(dc,DLIM)
    m_comply=int((m_s['avg']<=MLIM).sum()); d_comply=int((d_s['avg']<=DLIM).sum())

    # ── Gráficas ──────────────────────────────────────────────────────────
    trend_chart = make_trend_chart(trend)
    bars_magna  = make_bars(m_s.sort_values('avg'),MLIM,m_nat,
        f'Magna — Promedio por estado · {corte_fecha}',GREEN,RED,'barras_magna.png')
    bars_diesel = make_bars(d_s.sort_values('avg'),DLIM,d_nat,
        f'Diesel DUBA — Promedio por estado · {corte_fecha} · limite $27.00',TEAL,RED,'barras_diesel.png')

    # ── PDF ───────────────────────────────────────────────────────────────
    doc=SimpleDocTemplate(str(out_path),pagesize=A4,
        leftMargin=15*mm,rightMargin=15*mm,topMargin=13*mm,bottomMargin=13*mm)
    story=[]

    # PG 1: PORTADA
    story.append(hbar(f'INTELIGENCIA DE MERCADO — COMBUSTIBLES MEXICO — {corte_fecha.upper()}','dark',10))
    story.append(Spacer(1,5*mm))
    story.append(Paragraph('Análisis Semanal de Precios de Combustibles',S('t1',19,'dark',True,ld=23)))
    story.append(Paragraph('Gasolina Magna y Diésel DUBA · Mercado interior limpio · Fuente: CNE',S('t2',10,'dgray')))
    story.append(Spacer(1,1*mm))
    story.append(hr('blue',2,2))
    story.append(Paragraph(f'Análisis elaborado por {AUTOR} · Publicación: jueves posterior al corte de 18:00 hrs',
                            S('t3',8,'dgray')))
    story.append(Spacer(1,4*mm))

    tc_str = f'${macro["tipo_cambio"]:.4f}' if macro.get('tipo_cambio') else 'N/D'
    wti_str = f'${macro["wti"]:.2f} USD/bbl' if macro.get('wti') else 'N/D'

    story.append(kpis([
        (f'${m_nat:.2f}','Magna · Prom. nac.\ninterior limpio','lblue'),
        (f'${MLIM:.2f}','Límite Magna\npacto voluntario','lamber'),
        (f'${d_nat:.2f}','Diésel · Prom. nac.\ninterior limpio','lred'),
        (f'${DLIM:.2f}','Límite Diésel\npacto voluntario','lamber'),
        (f'{m_comply}/32','Magna: estados\nque cumplen','lgreen' if m_comply>=28 else 'lred'),
        (f'{d_comply}/30','Diésel: estados\nque cumplen','lred'),
    ]))
    story.append(Spacer(1,3*mm))

    # Macro context bar
    macro_items = [
        [Paragraph(f'<b>Tipo de cambio:</b> {tc_str}',S('mc1',9,'dark',False)),
         Paragraph(f'<b>Precio WTI:</b> {wti_str}',S('mc2',9,'dark',False)),
         Paragraph(f'<b>Registros procesados:</b> {len(mc)+len(dc):,}',S('mc3',9,'dark',False)),
         Paragraph(f'<b>Corte:</b> {corte_fecha}',S('mc4',9,'dark',False))]
    ]
    mt=Table(macro_items,colWidths=[TW/4]*4)
    mt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),C['lgray']),
        ('ALIGN',(0,0),(-1,-1),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),
        ('BOX',(0,0),(-1,-1),0.5,C['mgray']),('ROUNDEDCORNERS',[4])]))
    story.append(mt)
    story.append(Spacer(1,4*mm))

    story.append(Paragraph('Evolución del precio nacional — mercado interior limpio',
                            S('sh',8,'dgray',True,sb=0,sa=2)))
    story.append(im(trend_chart,TW,TW*0.38))
    story.append(Paragraph('Verde = bajo límite · Rojo = sobre límite · Línea punteada = límite del acuerdo voluntario',
                            S('cap',7.5,'dgray',False,TA_CENTER)))
    story.append(PageBreak())

    # PG 2: RESUMEN EJECUTIVO (narrativa Claude)
    story.append(hbar('RESUMEN EJECUTIVO — LECTURA ESTRATÉGICA','blue'))
    story.append(Spacer(1,4*mm))
    story.extend(narrative_block(narrative.get('resumen_ejecutivo','')))
    story.append(Spacer(1,4*mm))

    story.append(hbar('CONTEXTO DE LA SEMANA','teal'))
    story.append(Spacer(1,3*mm))
    story.extend(narrative_block(narrative.get('contexto_semana','')))
    story.append(Spacer(1,4*mm))

    # Hallazgo destacado
    hallazgo = narrative.get('hallazgo_semana','')
    if hallazgo:
        story.append(callout('HALLAZGO DE LA SEMANA',hallazgo,'amber','lamber'))
    story.append(Spacer(1,4*mm))

    story.append(hbar('PERSPECTIVA — PRÓXIMA SEMANA','dark'))
    story.append(Spacer(1,3*mm))
    story.extend(narrative_block(narrative.get('perspectiva','')))
    story.append(PageBreak())

    # PG 3: NOTICIAS DE LA SEMANA
    if macro.get('noticias'):
        story.append(hbar('NOTICIAS RELEVANTES DE LA SEMANA','dark'))
        story.append(Spacer(1,3*mm))
        news_rows=[[
            Paragraph('<b>Titular</b>',S('nh0',8,'white',True)),
            Paragraph('<b>Fuente</b>',S('nh1',8,'white',True)),
            Paragraph('<b>Fecha</b>',S('nh2',8,'white',True,TA_CENTER)),
        ]]
        for n in macro['noticias'][:10]:
            news_rows.append([
                Paragraph(n['titulo'][:110],S(f'nt{n["titulo"][:4]}',7.5,'dark')),
                Paragraph(n['fuente'][:25],S(f'nf{n["titulo"][:4]}',7.5,'dgray')),
                Paragraph(n['fecha'][:12],S(f'nd{n["titulo"][:4]}',7.5,'dgray',False,TA_CENTER)),
            ])
        nt=Table(news_rows,colWidths=[TW*0.66,TW*0.20,TW*0.14],repeatRows=1)
        nt.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),C['dark']),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[C['white'],C['lgray']]),
            ('GRID',(0,0),(-1,-1),0.2,C['mgray']),
            ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
            ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
            ('LEFTPADDING',(0,0),(-1,-1),6),('RIGHTPADDING',(0,0),(-1,-1),6),
            ('ROUNDEDCORNERS',[3]),
        ]))
        story.append(nt)
        story.append(PageBreak())

    # PG 4: MAGNA POR ESTADO
    story.append(hbar('GASOLINA MAGNA — ANÁLISIS POR ESTADO · LÍMITE $24.00/l','blue'))
    story.append(Spacer(1,3*mm))
    story.append(im(bars_magna,TW,TW*0.76))
    story.append(Paragraph(f'{m_comply}/32 estados cumplen · {corte_fecha}',
                            S('cap',7.5,'dgray',False,TA_CENTER)))
    story.append(Spacer(1,3*mm))
    story.append(state_table(m_s,MLIM,out_m,'m'))
    story.append(PageBreak())

    # PG 5: DIESEL POR ESTADO
    story.append(hbar(f'DIÉSEL DUBA — ANÁLISIS POR ESTADO · LÍMITE $27.00/l · {d_comply}/30 ESTADOS CUMPLEN','red'))
    story.append(Spacer(1,3*mm))
    story.append(im(bars_diesel,TW,TW*0.76))
    story.append(Paragraph(f'{d_comply}/30 estados cumplen · {corte_fecha}',
                            S('cap',7.5,'red',False,TA_CENTER)))
    story.append(Spacer(1,3*mm))
    story.append(state_table(d_s,DLIM,out_d,'d'))
    story.append(PageBreak())

    # PG 6: METODOLOGÍA
    story.append(hbar('METODOLOGÍA — DEPURACIÓN Y CLASIFICACIÓN DE DATOS','red'))
    story.append(Spacer(1,4*mm))
    story.extend(narrative_block(narrative.get('nota_metodologica','')))
    story.append(Spacer(1,4*mm))

    rows_m=[
        [Paragraph(c,S(f'mth{i}',8,'white',True)) for i,c in
         enumerate(['Categoría','Registros','Criterio','Tratamiento'])],
        [Paragraph('Mercado interior',S('c1',8,'teal',True)),
         Paragraph('~5,500',S('c2',8,'dark',True,TA_CENTER)),
         Paragraph('Municipio no fronterizo · precio variable entre cortes',S('c3',8,'dark')),
         Paragraph('Análisis principal vs. acuerdo voluntario',S('c4',8,'dark'))],
        [Paragraph('Fronterizo activo',S('c5',8,'blue',True)),
         Paragraph('~400',S('c6',8,'dark',True,TA_CENTER)),
         Paragraph('22 municipios en franja fronteriza con variación real de precio',S('c7',8,'dark')),
         Paragraph('Índice separado. Referencia: paridad USD',S('c8',8,'dark'))],
        [Paragraph('Registros fantasma',S('c9',8,'red',True)),
         Paragraph('~250',S('c10',8,'dark',True,TA_CENTER)),
         Paragraph('Precio idéntico en 3+ cortes consecutivos · precio < $22/l',S('c11',8,'dark')),
         Paragraph('Excluidos. Distorsionan mínimo nacional en $3.49/l',S('c12',8,'dark'))],
    ]
    mt2=Table(rows_m,colWidths=[35*mm,22*mm,TW*0.40,TW*0.28])
    mt2.setStyle(TableStyle([
        ('BACKGROUND',(0,0),(-1,0),C['dark']),
        ('BACKGROUND',(0,1),(-1,1),C['lgreen']),('BACKGROUND',(0,2),(-1,2),C['lblue']),
        ('BACKGROUND',(0,3),(-1,3),C['lred']),
        ('GRID',(0,0),(-1,-1),0.2,C['mgray']),('ALIGN',(1,0),(1,-1),'CENTER'),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),
        ('LEFTPADDING',(0,0),(-1,-1),6),('RIGHTPADDING',(0,0),(-1,-1),6),
        ('ROUNDEDCORNERS',[3])]))
    story.append(mt2)
    story.append(Spacer(1,5*mm))
    story.append(hr())
    story.append(Paragraph(
        f'Análisis elaborado por {AUTOR} · '
        'Datos: CNE api-reportediario.cne.gob.mx · '
        'Procesamiento: scraper Python automatizado (Railway) · '
        'Publicación: jueves posterior al corte de 18:00 hrs · '
        'Metodología disponible bajo solicitud',
        S('foot',7.5,'dgray',False,TA_CENTER)))

    doc.build(story)
    log.info(f"PDF generado: {out_path} ({out_path.stat().st_size//1024} KB)")
    return out_path


# ══════════════════════════════════════════════════════════════════════════
# 6. ENVÍO POR CORREO (SendGrid)
# ══════════════════════════════════════════════════════════════════════════

def send_report_email(pdf_path, corte_fecha, stats):
    """Envía el PDF por correo vía SendGrid API."""
    import base64

    if not SENDGRID_KEY or not EMAIL_TO:
        log.warning("Sin credenciales SendGrid — no se envía email")
        return

    with open(pdf_path,'rb') as f:
        pdf_b64 = base64.b64encode(f.read()).decode()

    m = stats['magna']; d = stats['diesel']
    subject = f"Análisis Semanal Combustibles CNE — {corte_fecha}"
    body = f"""Análisis semanal de precios de combustibles CNE — {corte_fecha}
Elaborado por {AUTOR}

═══ MAGNA ═══
Promedio nacional: ${m['nat']:.2f}/l (límite $24.00)
Margen: {'+' if m['nat']>=24 else '-'}${abs(m['nat']-24):.2f}/l
Estados que cumplen: {m['comply']}/32

═══ DIÉSEL DUBA ═══
Promedio nacional: ${d['nat']:.2f}/l (límite $27.00)
Brecha: +${d['nat']-27:.2f}/l
Estados que cumplen: 0/{d['n_states']}

Adjunto el reporte completo en PDF con análisis de mercado,
narrativa estratégica, mapas de calor y tablas por estado.

---
Este reporte se genera automáticamente cada jueves después del corte de 18hrs.
"""

    payload = json.dumps({
        'personalizations': [{'to': [{'email': EMAIL_TO}]}],
        'from': {'email': SENDGRID_FROM, 'name': AUTOR},
        'subject': subject,
        'content': [{'type': 'text/plain', 'value': body}],
        'attachments': [{
            'content': pdf_b64,
            'type': 'application/pdf',
            'filename': pdf_path.name,
        }]
    }).encode()

    req = urllib.request.Request(
        'https://api.sendgrid.com/v3/mail/send',
        data=payload,
        headers={
            'Authorization': f'Bearer {SENDGRID_KEY}',
            'Content-Type': 'application/json',
        }
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            log.info(f"Email enviado: {r.status}")
    except Exception as e:
        log.error(f"Error enviando email: {e}")


# ══════════════════════════════════════════════════════════════════════════
# 7. MAIN
# ══════════════════════════════════════════════════════════════════════════

def main():
    log.info("=== REPORTE SEMANAL CNE — INICIO ===")

    # 7.1 Encontrar cortes disponibles
    cortes = get_last_n_cortes(5)
    if not cortes:
        log.error("No se encontraron archivos CSV en DATA_DIR. Abortando.")
        sys.exit(1)

    corte_actual = cortes[0]
    log.info(f"Corte más reciente: {corte_actual.name}")

    # Extraer fecha del archivo
    m = re.search(r'(\d{8})', corte_actual.name)
    if m:
        d = m.group(1)
        corte_fecha = f"{d[6:8]}/{d[4:6]}/{d[0:4]}"
    else:
        corte_fecha = datetime.date.today().strftime('%d/%m/%Y')

    # 7.2 Cargar y depurar datos
    mc, dc, ghost, frontera_m, frontera_d = load_and_clean(corte_actual)
    log.info(f"Interior limpio: magna={len(mc)}, diesel={len(dc)}")
    log.info(f"Frontera: {len(frontera_m)} | Fantasmas: {len(ghost)}")

    # 7.3 Tendencia histórica
    trend = build_trend(cortes)
    log.info(f"Tendencia: {trend}")

    # 7.4 Estadísticas para Claude
    stats = compute_stats(mc, dc)

    # 7.5 Contexto macro
    macro = get_macro_context()
    log.info(f"Tipo de cambio: {macro['tipo_cambio']} | WTI: {macro['wti']}")
    log.info(f"Noticias encontradas: {len(macro['noticias'])}")

    # 7.6 Narrativa McKinsey con Claude
    narrative = get_claude_narrative(stats, trend, macro, corte_fecha)

    # 7.7 Generar PDF
    fecha_str = datetime.date.today().strftime('%Y%m%d')
    pdf_name = f"reporte_cne_{fecha_str}.pdf"
    pdf_path = REPORTS_DIR / pdf_name
    build_pdf(mc, dc, trend, narrative, macro, corte_fecha, pdf_path)

    # 7.8 Enviar correo
    send_report_email(pdf_path, corte_fecha, stats)

    log.info("=== REPORTE SEMANAL CNE — FIN ===")


if __name__ == '__main__':
    main()
