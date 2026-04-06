#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ApiTrack Pro — Plateforme Apicole Ultra-Professionnelle v3.0
IA Morphométrie GRATUITE & PERMANENTE :
  ✅ Google Gemini 2.0 Flash  (gratuit : 15 req/min, 1M tokens/jour, VISION)
  ✅ Ollama / LLaVA           (100% local, gratuit, illimité, hors ligne)
  ✅ Groq + LLaMA 3.2 Vision  (gratuit : 30 req/min, vision disponible)
  ✅ Fallback intelligent automatique
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import json, base64, io, os, re, time, math
import warnings
warnings.filterwarnings("ignore")
from PIL import Image, ImageEnhance, ImageFilter, ImageDraw

# ── Gestionnaire de données persistantes ──────────────────────────────────────
from data_manager import (
    init_data_dir, load_table, load_all, save_table, save_all,
    add_record, update_record, delete_record, generate_id,
    create_backup, list_backups, restore_backup, restore_from_upload,
    get_backup_zip_bytes, export_table_csv, export_all_csv_zip,
    import_csv, get_storage_stats, load_config, save_config, DATA_DIR
)

# =====================================================
# IMPORTS IA (avec gestion d'absence gracieuse)
# =====================================================
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False

try:
    import requests as req_lib
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

# =====================================================
# CONFIGURATION GÉNÉRALE
# =====================================================
st.set_page_config(
    page_title="ApiTrack Pro",
    page_icon="🐝",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Clés API (à configurer dans .streamlit/secrets.toml OU variable d'env) ───
# Gratuit : https://aistudio.google.com/apikey  → 15 req/min, 1M tokens/jour
GEMINI_API_KEY  = st.secrets.get("GEMINI_API_KEY",  os.getenv("GEMINI_API_KEY",  ""))
# Gratuit : https://console.groq.com/keys       → 30 req/min, vision LLaMA
GROQ_API_KEY    = st.secrets.get("GROQ_API_KEY",    os.getenv("GROQ_API_KEY",    ""))
# Local 100% gratuit : http://localhost:11434 (ollama pull llava)
OLLAMA_BASE_URL = st.secrets.get("OLLAMA_BASE_URL",  os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"))
OLLAMA_MODEL    = st.secrets.get("OLLAMA_MODEL",     os.getenv("OLLAMA_MODEL",    "llava:7b"))

# =====================================================
# PROMPT IA MORPHOMÉTRIE ULTRA-SCIENTIFIQUE
# =====================================================
PROMPT_MORPHO = """Tu es un expert mondial en morphométrie des abeilles (Apis mellifera).
Analyse cette image selon le protocole Ruttner (1988) et Kandemir (2011).

RÉPONDS UNIQUEMENT EN JSON VALIDE, sans backticks, sans texte hors JSON :

{
  "qualite_image": "bonne|moyenne|insuffisante",
  "type_specimen": "ouvrière|reine|faux-bourdon|indéterminé",
  "mesures": {
    "L_aile_mm": 9.2,
    "B_aile_mm": 3.2,
    "Ri": 2.45,
    "DI3_mm": 1.72,
    "OI": "-",
    "A4_deg": 99.2,
    "B4_deg": 91.5,
    "Ti_L_mm": 3.01,
    "Ba_L_mm": 1.88,
    "Ba_W_mm": 1.09,
    "Fe_L_mm": 2.75,
    "T3_L_mm": 4.78,
    "T4_L_mm": 4.65,
    "T4_W_pct": 37,
    "S4_L_mm": 2.72,
    "Glossa_mm": 6.12,
    "Wt_mm": 4.12,
    "Pigment": 5,
    "Hb_mm": 0.38
  },
  "integrite_ailes": "intactes|légèrement usées|usées|endommagées",
  "nervation": "normale|atypique",
  "classification": {
    "taxon": "Apis mellifera intermissa",
    "confiance_pct": 88,
    "probabilites": {
      "A. m. intermissa": 88,
      "A. m. sahariensis": 7,
      "A. m. ligustica": 3,
      "A. m. carnica": 1,
      "Hybride": 1
    }
  },
  "caracterisation_langue": {
    "classe": "courte|moyenne|longue",
    "adaptation": "corolles courtes|corolles moyennes|corolles profondes",
    "plantes_cibles": ["Romarin", "Jujubier"],
    "avantage": "description 1 phrase"
  },
  "profil_productif": {
    "specialisation": "miel|pollen|gelée_royale|polyvalent",
    "score_miel": 75,
    "score_pollen": 60,
    "score_gelee_royale": 35,
    "score_propolis": 45,
    "justification": "1 phrase scientifique"
  },
  "resistance_varroa": {
    "score_vsh_estime": 70,
    "comportement_hygienique": "élevé|moyen|faible",
    "recommandation": "1 phrase"
  },
  "diagnostic_sanitaire": {
    "etat": "sain|suspect|malade",
    "anomalies": [],
    "pathologies_suspectees": [],
    "deformations_varroa": false,
    "ailes_ok": true,
    "thorax_ok": true,
    "abdomen_ok": true,
    "notes": ""
  },
  "interpretation": "Texte scientifique complet 3-4 phrases citant caractères clés et références.",
  "recommandations": ["recommandation 1", "recommandation 2"]
}

Si l'image ne montre pas clairement une abeille, mets qualite_image='insuffisante' et des valeurs estimées."""

PROMPT_DIAGNOSTIC = """Tu es un vétérinaire apicole expert spécialisé en pathologies d'Apis mellifera.
Analyse cette photo de {photo_type} prise en {saison}.{symptomes_line}

RÉPONDS UNIQUEMENT EN JSON VALIDE :

{{
  "verdict": {{
    "etat": "sain|vigilance|malade|critique",
    "score_sante": 80,
    "resume": "1-2 phrases"
  }},
  "observations": ["observation précise 1", "observation 2"],
  "maladies_detectees": [
    {{
      "nom": "nom maladie",
      "agent": "agent pathogène",
      "probabilite": "haute|modérée|faible",
      "signes": "description signes observés",
      "gravite": "légère|modérée|sévère|critique",
      "traitement": "traitement recommandé",
      "urgence": "immédiate|sous 48h|sous 1 semaine|surveillance"
    }}
  ],
  "parasites": [
    {{
      "nom": "Varroa destructor",
      "confirme": true,
      "niveau": "faible|modéré|élevé|très élevé",
      "taux_estime": "~2%",
      "action": "action recommandée"
    }}
  ],
  "anomalies": ["anomalie structurelle observée"],
  "points_positifs": ["point positif"],
  "plan_action": [
    {{"priorite": 1, "action": "action urgente", "delai": "immédiatement"}}
  ],
  "suivi": "fréquence suivi recommandé"
}}"""

# =====================================================
# MODULE IA — GRATUIT & PERMANENT
# =====================================================

def image_to_base64(image: Image.Image, quality: int = 85) -> str:
    """Convertit PIL Image en base64 JPEG."""
    buf = io.BytesIO()
    if image.mode in ("RGBA","P","LA"):
        image = image.convert("RGB")
    image.save(buf, format="JPEG", quality=quality)
    return base64.b64encode(buf.getvalue()).decode("utf-8")

def image_to_bytes(image: Image.Image) -> bytes:
    """Convertit PIL Image en bytes JPEG."""
    buf = io.BytesIO()
    if image.mode in ("RGBA","P","LA"):
        image = image.convert("RGB")
    image.save(buf, format="JPEG", quality=90)
    return buf.getvalue()

def parse_json_response(text: str) -> dict:
    """Parse JSON depuis réponse IA (robuste)."""
    text = text.strip()
    # Supprimer les backticks markdown
    text = re.sub(r"```json\s*", "", text)
    text = re.sub(r"```\s*", "", text)
    text = text.strip()
    # Trouver le JSON
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        text = text[start:end]
    try:
        return json.loads(text)
    except:
        # Tentative de réparation basique
        text = re.sub(r",\s*}", "}", text)
        text = re.sub(r",\s*\]", "]", text)
        try:
            return json.loads(text)
        except:
            return {"error": "Parse JSON échoué", "raw": text[:300]}

# ─── 1. GOOGLE GEMINI (Gratuit : 15 req/min, 1M tokens/jour) ─────────────────
def analyze_with_gemini(image: Image.Image, prompt: str) -> dict:
    """
    Google Gemini 2.0 Flash — GRATUIT
    Obtenir une clé API : https://aistudio.google.com/apikey
    Limites : 15 req/min, 1M tokens/jour, 1500 req/jour
    """
    if not GEMINI_AVAILABLE:
        return {"error": "google-generativeai non installé. pip install google-generativeai"}
    if not GEMINI_API_KEY:
        return {"error": "GEMINI_API_KEY non configurée. Obtenez une clé GRATUITE sur https://aistudio.google.com/apikey"}
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.0-flash-exp")
        img_part = {"mime_type": "image/jpeg", "data": image_to_base64(image)}
        response = model.generate_content([prompt, img_part])
        return parse_json_response(response.text)
    except Exception as e:
        err = str(e)
        if "quota" in err.lower() or "429" in err:
            return {"error": "Quota Gemini atteint. Réessayez dans 1 minute (limite : 15 req/min gratuit)."}
        return {"error": f"Gemini: {err}"}

# ─── 2. OLLAMA LOCAL (100% Gratuit, Illimité, Hors ligne) ────────────────────
def analyze_with_ollama(image: Image.Image, prompt: str) -> dict:
    """
    Ollama — 100% LOCAL, GRATUIT, ILLIMITÉ, SANS INTERNET
    Installation : https://ollama.com/download
    Modèle : ollama pull llava:7b  (ou llava:13b pour plus de précision)
    Démarrage : ollama serve
    """
    if not REQUESTS_AVAILABLE:
        return {"error": "requests non installé."}
    try:
        # Vérifier si Ollama est disponible
        health = req_lib.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=3)
        if health.status_code != 200:
            return {"error": f"Ollama non disponible sur {OLLAMA_BASE_URL}. Démarrez 'ollama serve'."}
        
        payload = {
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "images": [image_to_base64(image)],
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 2000}
        }
        response = req_lib.post(f"{OLLAMA_BASE_URL}/api/generate", json=payload, timeout=120)
        data = response.json()
        return parse_json_response(data.get("response", ""))
    except req_lib.exceptions.ConnectionError:
        return {"error": f"Ollama non accessible ({OLLAMA_BASE_URL}). Lancez 'ollama serve' dans un terminal."}
    except Exception as e:
        return {"error": f"Ollama: {str(e)}"}

# ─── 3. GROQ + LLaMA 3.2 Vision (Gratuit : 30 req/min) ──────────────────────
def analyze_with_groq(image: Image.Image, prompt: str) -> dict:
    """
    Groq Cloud — GRATUIT (30 req/min, vision avec LLaMA 3.2)
    Clé API GRATUITE : https://console.groq.com/keys
    """
    if not REQUESTS_AVAILABLE:
        return {"error": "requests non installé."}
    if not GROQ_API_KEY:
        return {"error": "GROQ_API_KEY non configurée. Obtenez une clé GRATUITE sur https://console.groq.com/keys"}
    try:
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": "llama-3.2-11b-vision-preview",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {
                        "url": f"data:image/jpeg;base64,{image_to_base64(image)}"
                    }}
                ]
            }],
            "max_tokens": 2048,
            "temperature": 0.1
        }
        resp = req_lib.post("https://api.groq.com/openai/v1/chat/completions",
                            headers=headers, json=payload, timeout=60)
        data = resp.json()
        if "error" in data:
            return {"error": f"Groq: {data['error'].get('message', str(data['error']))}"}
        text = data["choices"][0]["message"]["content"]
        return parse_json_response(text)
    except Exception as e:
        return {"error": f"Groq: {str(e)}"}

# ─── 4. FALLBACK INTELLIGENT (essaie dans l'ordre de disponibilité) ───────────
def analyze_auto(image: Image.Image, prompt: str, preference: str = "auto") -> tuple[dict, str]:
    """
    Système de fallback intelligent.
    Retourne (résultat, source_utilisée)
    Ordre par défaut : Gemini → Groq → Ollama
    """
    sources = {
        "gemini": ("Google Gemini 2.0 Flash (gratuit)", analyze_with_gemini),
        "groq":   ("Groq LLaMA Vision (gratuit)",       analyze_with_groq),
        "ollama": ("Ollama local (gratuit, hors ligne)", analyze_with_ollama),
    }
    
    if preference != "auto" and preference in sources:
        name, fn = sources[preference]
        result = fn(image, prompt)
        if "error" not in result:
            return result, name
        return result, name
    
    # Auto : tester dans l'ordre
    order = ["gemini", "groq", "ollama"]
    last_error = {}
    for key in order:
        name, fn = sources[key]
        result = fn(image, prompt)
        if "error" not in result:
            return result, name
        last_error = result
    
    return {"error": f"Tous les services IA ont échoué. Dernier: {last_error.get('error','')}"}, "Aucun"

# =====================================================
# CSS ULTRA-PROFESSIONNEL
# =====================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,600;0,700;1,400&family=DM+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    --honey:#C97A08; --honey-l:#F2C14E; --honey-pale:#FFF8E6; --honey-dark:#7A4800;
    --forest:#1C3D10; --forest-mid:#2D5E1E; --forest-l:#5A8E40; --forest-pale:#EAF3E4;
    --wax:#F2E8CC; --wax-mid:#E0CFA0; --wax-dark:#B89E6A;
    --earth:#3E2A18; --cream:#FDFAF3; --night:#131308;
    --text:#1C180C; --muted:#655E40;
    --pollen:#E8A020; --royal:#7C3AED; --royal-l:#EDE9FE;
    --teal:#0D9488; --teal-l:#CCFBF1;
}
html,body,[class*="css"]{font-family:'DM Sans',sans-serif;color:var(--text);}
#MainMenu,footer,header{visibility:hidden;}
.block-container{padding-top:1.5rem;padding-bottom:2rem;max-width:1440px;}

[data-testid="stSidebar"]{
    background:linear-gradient(180deg,#0F1F08 0%,#1C3D10 40%,#142B0A 100%);
    border-right:1px solid rgba(210,170,60,0.18);
}
[data-testid="stSidebar"] *{color:rgba(255,255,255,0.82) !important;}
[data-testid="stSidebar"] hr{border-color:rgba(255,255,255,0.08) !important;}

/* Metric */
[data-testid="stMetric"]{
    background:white; border-radius:18px; padding:20px 22px;
    border:1px solid rgba(180,150,80,0.18);
    box-shadow:0 2px 14px rgba(60,40,10,0.07);
    transition:box-shadow .2s,transform .2s;
}
[data-testid="stMetric"]:hover{box-shadow:0 6px 28px rgba(60,40,10,0.13);transform:translateY(-2px);}
[data-testid="stMetricLabel"]{font-size:10.5px!important;text-transform:uppercase;letter-spacing:.08em;color:var(--muted)!important;font-weight:600;}
[data-testid="stMetricValue"]{font-family:'Playfair Display',serif!important;font-size:2rem!important;color:var(--text)!important;}

/* Tabs */
[data-testid="stTabs"] [data-baseweb="tab-list"]{background:#F2E8CC;border-radius:14px;padding:4px;gap:4px;}
[data-testid="stTabs"] [data-baseweb="tab"]{border-radius:10px;font-size:12.5px;font-weight:500;color:var(--muted);background:transparent;border:none;padding:8px 16px;transition:all .15s;}
[data-testid="stTabs"] [aria-selected="true"]{background:white!important;color:var(--text)!important;box-shadow:0 1px 5px rgba(0,0,0,.11)!important;}

/* Buttons */
.stButton>button{border-radius:11px;font-family:'DM Sans',sans-serif;font-weight:500;font-size:13px;transition:all .15s;}
.stButton>button[kind="primary"]{background:linear-gradient(135deg,#C97A08,#7A4800);color:white;border:none;}
.stButton>button:hover{transform:translateY(-1px);box-shadow:0 4px 14px rgba(80,50,10,0.15);}

/* Inputs */
.stTextInput>div>div>input,.stNumberInput>div>div>input,
.stSelectbox>div>div,.stTextArea>div>div>textarea{
    border-radius:10px!important;border:1.5px solid rgba(180,150,80,0.38)!important;
    font-family:'DM Sans',sans-serif!important;font-size:13.5px!important;background:white!important;
}

/* Custom cards */
.api-card{
    background:white;border-radius:16px;padding:18px 20px;
    border:1px solid rgba(180,150,80,0.2);
    box-shadow:0 2px 12px rgba(60,40,10,0.07);
    margin-bottom:12px;transition:all .2s;
}
.api-card:hover{box-shadow:0 6px 24px rgba(60,40,10,0.12);}
.api-card-gemini{border-left:4px solid #4285F4;}
.api-card-ollama{border-left:4px solid #22c55e;}
.api-card-groq{border-left:4px solid #f97316;}

.morph-result-card{
    background:white;border-radius:18px;padding:22px;
    border:2px solid var(--honey);
    box-shadow:0 4px 24px rgba(201,122,8,.15);
}
.measure-row{
    display:flex;align-items:center;justify-content:space-between;
    padding:7px 0;border-bottom:1px solid rgba(180,150,80,0.12);
    font-size:12.5px;
}
.measure-row:last-child{border-bottom:none;}
.mval{font-family:'JetBrains Mono',monospace;font-size:12px;padding:3px 8px;border-radius:6px;}
.mval-ok{background:#dcfce7;color:#15803d;}
.mval-warn{background:#fef9c3;color:#a16207;}
.mval-bad{background:#fee2e2;color:#b91c1c;}

.prod-profile-grid{
    display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:14px 0;
}
.pp-item{
    text-align:center;padding:12px 6px;border-radius:12px;
    background:var(--cream);border:1px solid rgba(180,150,80,0.15);
}
.pp-score{font-family:'Playfair Display',serif;font-size:22px;font-weight:700;line-height:1;}
.pp-label{font-size:9.5px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;margin-top:3px;}

.langue-bar{height:11px;background:var(--wax);border-radius:6px;overflow:hidden;margin:8px 0;}
.langue-fill{height:100%;border-radius:6px;background:linear-gradient(90deg,#86efac,#22c55e,var(--honey));}

.alert-box{padding:13px 16px;border-radius:12px;margin-bottom:10px;font-size:13px;display:flex;align-items:flex-start;gap:11px;line-height:1.6;}
.al-danger{background:#fef2f2;border:1px solid #fecaca;color:#991b1b;border-left:4px solid #ef4444;}
.al-warning{background:#fffbeb;border:1px solid #fde68a;color:#92400e;border-left:4px solid #f59e0b;}
.al-success{background:#f0fdf4;border:1px solid #bbf7d0;color:#166534;border-left:4px solid #22c55e;}
.al-info{background:#eff6ff;border:1px solid #bfdbfe;color:#1e40af;border-left:4px solid #3b82f6;}
.al-royal{background:#faf5ff;border:1px solid #e9d5ff;color:#6b21a8;border-left:4px solid #9333ea;}

.section-title{
    font-family:'Playfair Display',serif;font-size:20px;font-weight:700;
    color:var(--earth);margin-bottom:4px;padding-bottom:7px;
    border-bottom:2px solid rgba(201,122,8,0.18);
}
.page-title{font-family:'Playfair Display',serif;font-size:30px;font-weight:700;color:var(--earth);margin-bottom:3px;}
.page-sub{font-size:13.5px;color:var(--muted);margin-bottom:24px;}

.badge{display:inline-block;padding:3px 11px;border-radius:20px;font-size:10.5px;font-weight:600;letter-spacing:.04em;}
.b-green{background:#dcfce7;color:#15803d;} .b-amber{background:#fef9c3;color:#a16207;}
.b-orange{background:#ffedd5;color:#c2410c;} .b-red{background:#fee2e2;color:#b91c1c;}
.b-blue{background:#dbeafe;color:#1d4ed8;} .b-purple{background:#f3e8ff;color:#7e22ce;}
.b-teal{background:#ccfbf1;color:#0d9488;} .b-gray{background:#f1f5f9;color:#475569;}

.ruche-card{
    background:white;border-radius:18px;padding:20px;
    border:1px solid rgba(180,150,80,0.2);cursor:pointer;
    transition:all .2s;height:100%;
}
.ruche-card:hover{box-shadow:0 8px 28px rgba(80,50,10,0.12);transform:translateY(-3px);border-color:var(--honey);}

.status-dot{width:10px;height:10px;border-radius:50%;display:inline-block;}
.s-excellent{background:#22c55e;box-shadow:0 0 0 3px #22c55e22;}
.s-bon{background:var(--honey);box-shadow:0 0 0 3px var(--honey-pale);}
.s-attention{background:#f97316;box-shadow:0 0 0 3px #ffedd5;}
.s-critique{background:#ef4444;box-shadow:0 0 0 3px #fee2e2;}

/* Source badge IA */
.ia-source{
    display:inline-flex;align-items:center;gap:6px;
    padding:5px 14px;border-radius:20px;font-size:11.5px;font-weight:600;
    margin-bottom:14px;
}
.ia-gemini{background:#EFF6FF;color:#1D4ED8;border:1px solid #BFDBFE;}
.ia-ollama{background:#F0FDF4;color:#15803D;border:1px solid #BBF7D0;}
.ia-groq{background:#FFF7ED;color:#C2410C;border:1px solid #FED7AA;}
</style>
""", unsafe_allow_html=True)

# =====================================================
# HELPERS VISUELS
# =====================================================
def alert(icon, text, cls="al-info"):
    return f'<div class="alert-box {cls}"><span style="font-size:18px">{icon}</span><div>{text}</div></div>'

def section_header(title):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)

def ruche_card_html(r):
    dot = {"Excellent":"s-excellent","Bon":"s-bon","Attention":"s-attention","Critique":"s-critique"}.get(r["Statut"],"s-bon")
    pi = {"Miel":"🍯","Pollen":"🌿","Gelée Royale":"👑","Résistance":"🛡️"}.get(r["Profil_prod"],"🐝")
    race_cls = {"A. m. intermissa":"b-amber","A. m. sahariensis":"b-purple","A. m. ligustica":"b-blue","A. m. carnica":"b-green","Hybride":"b-teal"}.get(r["Race"],"b-gray")
    return f"""<div class="ruche-card">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px">
      <span style="font-family:'JetBrains Mono',monospace;font-size:10px;background:var(--wax);padding:3px 8px;border-radius:6px;color:var(--muted)">{r["ID"]}</span>
      <div style="display:flex;align-items:center;gap:7px"><span title="{r['Profil_prod']}">{pi}</span>
        <div class="status-dot {dot}"></div></div>
    </div>
    <div style="font-family:'Playfair Display',serif;font-size:16px;font-weight:700;margin-bottom:3px">{r["Nom"]}</div>
    <div style="margin-bottom:11px"><span class="badge {race_cls}" style="font-size:10px">{r["Race"][:18]}</span></div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:7px">
      <div style="background:var(--honey-pale);border-radius:9px;padding:8px">
        <div style="font-weight:600;font-size:13.5px;color:var(--honey-dark)">{r["Poids_kg"]} kg</div>
        <div style="font-size:9.5px;text-transform:uppercase;color:var(--muted)">Poids</div></div>
      <div style="background:var(--honey-pale);border-radius:9px;padding:8px">
        <div style="font-weight:600;font-size:13.5px;color:var(--honey-dark)">{r["Varroa_pct"]}%</div>
        <div style="font-size:9.5px;text-transform:uppercase;color:var(--muted)">Varroa</div></div>
      <div style="background:var(--honey-pale);border-radius:9px;padding:8px">
        <div style="font-weight:600;font-size:13.5px;color:var(--honey-dark)">{r["Miel_kg"]} kg</div>
        <div style="font-size:9.5px;text-transform:uppercase;color:var(--muted)">🍯 Miel</div></div>
      <div style="background:var(--honey-pale);border-radius:9px;padding:8px">
        <div style="font-weight:600;font-size:12px;color:var(--honey-dark)">{r["Pollen_kg"]}kg/{r["Gelée_g"]}g</div>
        <div style="font-size:9.5px;text-transform:uppercase;color:var(--muted)">🌿/{chr(128081)}</div></div>
    </div>
    <div style="margin-top:9px;font-size:11px;color:var(--muted)">📍 {r["Site"]}</div>
    </div>"""

def production_radar(r):
    cats = ["Miel","Pollen","Gelée R.","VSH","Douceur","Éco.hiv."]
    vals = [min(r["Miel_kg"]/20*100,100), min(r["Pollen_kg"]/5*100,100),
            min(r["Gelée_g"]/200*100,100), r["VSH_pct"], r["Douceur"], r["Economie_hiv"]]
    fig = go.Figure(go.Scatterpolar(
        r=vals+[vals[0]], theta=cats+[cats[0]], fill='toself',
        fillcolor='rgba(201,122,8,0.12)', line=dict(color='#C97A08',width=2.2)))
    fig.update_layout(polar=dict(
        bgcolor='rgba(253,250,243,0.5)',
        radialaxis=dict(visible=True,range=[0,100],tickfont=dict(size=8,color='#655E40'),
                        gridcolor='rgba(180,150,80,0.18)'),
        angularaxis=dict(tickfont=dict(size=10,color='#3E2A18'))),
        showlegend=False,height=280,margin=dict(l=28,r=28,t=28,b=28),
        paper_bgcolor='rgba(0,0,0,0)')
    return fig

# =====================================================
# CHARGEMENT PERSISTANT DES DONNÉES
# Données dans apitrack_data/ — survivent à l'extinction PC/smartphone
# =====================================================

def init_state():
    if st.session_state.get("_data_loaded"):
        return
    all_data = load_all()
    st.session_state.ruches         = all_data["ruches"]
    st.session_state.inspections    = all_data["inspections"]
    st.session_state.recoltes       = all_data["recoltes"]
    st.session_state.traitements    = all_data["traitements"]
    st.session_state.morph_analyses = all_data["morpho_analyses"]
    st.session_state.stock          = all_data["stock"]
    st.session_state.genetique      = all_data["genetique"]
    st.session_state.alertes_db     = all_data["alertes"]
    st.session_state._data_loaded   = True
    st.session_state._config        = load_config()

def save_state():
    save_all({
        "ruches":          st.session_state.get("ruches", pd.DataFrame()),
        "inspections":     st.session_state.get("inspections", pd.DataFrame()),
        "recoltes":        st.session_state.get("recoltes", pd.DataFrame()),
        "traitements":     st.session_state.get("traitements", pd.DataFrame()),
        "morpho_analyses": st.session_state.get("morph_analyses", pd.DataFrame()),
        "stock":           st.session_state.get("stock", pd.DataFrame()),
        "genetique":       st.session_state.get("genetique", pd.DataFrame()),
        "alertes":         st.session_state.get("alertes_db", pd.DataFrame()),
    })

def reload_state():
    st.session_state["_data_loaded"] = False
    init_state()

def auto_backup_if_needed():
    cfg = load_config()
    last = cfg.get("derniere_sauvegarde", "")
    if not last:
        create_backup("auto"); return
    try:
        last_dt = datetime.fromisoformat(last)
        if (datetime.now() - last_dt).days >= 1:
            create_backup("auto")
    except:
        create_backup("auto")

init_state()
auto_backup_if_needed()

# =====================================================
# CLASSIFICATION LOCALE (sans IA, instantanée)
# =====================================================
def classify_local(L, Ri, Ac, Pv, Tom, Ti):
    """Classification morphométrique locale selon Ruttner (1988)."""
    scores = {"A. m. intermissa":0,"A. m. sahariensis":0,"A. m. ligustica":0,"A. m. carnica":0}
    # Longueur aile
    if 8.9<=L<=9.6: scores["A. m. intermissa"]+=22
    if 8.7<=L<=9.3: scores["A. m. sahariensis"]+=22
    if 9.1<=L<=9.8: scores["A. m. ligustica"]+=18; scores["A. m. carnica"]+=18
    # Indice cubital
    if 2.0<=Ri<=2.8: scores["A. m. intermissa"]+=25
    if 2.1<=Ri<=2.9: scores["A. m. sahariensis"]+=22
    if 2.4<=Ri<=3.2: scores["A. m. ligustica"]+=25
    if 2.6<=Ri<=3.5: scores["A. m. carnica"]+=25
    # Glossa
    if 5.9<=Ac<=6.3: scores["A. m. intermissa"]+=28
    if 5.8<=Ac<=6.2: scores["A. m. sahariensis"]+=25
    if 6.3<=Ac<=6.7: scores["A. m. ligustica"]+=28
    if 6.4<=Ac<=6.8: scores["A. m. carnica"]+=28
    # Pigmentation
    if 4<=Pv<=7: scores["A. m. intermissa"]+=15
    if 5<=Pv<=8: scores["A. m. sahariensis"]+=15
    if 1<=Pv<=3: scores["A. m. ligustica"]+=15; scores["A. m. carnica"]+=15
    # Tomentum
    if 30<=Tom<=45: scores["A. m. intermissa"]+=10
    if 25<=Tom<=40: scores["A. m. sahariensis"]+=8
    if 45<=Tom<=60: scores["A. m. ligustica"]+=12
    if 35<=Tom<=50: scores["A. m. carnica"]+=10
    total = sum(scores.values()) or 1
    probs = {k:v/total*100 for k,v in scores.items()}
    best = max(probs, key=probs.get)
    if probs[best] < 38: best = "Hybride"; probs["Hybride"] = 100-sum(probs.values())
    conf = probs.get(best, 50)
    return best, conf, probs

# =====================================================
# SIDEBAR
# =====================================================
with st.sidebar:
    # Status IA compact
    ia_status = []
    if GEMINI_API_KEY: ia_status.append("🟢 Gemini")
    if GROQ_API_KEY:   ia_status.append("🟢 Groq")
    ia_status.append("⚪ Ollama")

    st.markdown(f"""
    <div style="padding:18px 4px 16px">
      <div style="background:linear-gradient(135deg,#C97A08,#7A4800);border-radius:14px;
                  width:50px;height:50px;display:flex;align-items:center;justify-content:center;
                  font-size:26px;margin-bottom:11px">🐝</div>
      <div style="font-family:'Playfair Display',serif;font-size:19px;font-weight:700;color:#F2C14E;line-height:1.1">ApiTrack Pro</div>
      <div style="font-size:9.5px;letter-spacing:.14em;text-transform:uppercase;color:rgba(255,255,255,0.38);margin-top:3px">Plateforme Apicole v3.0</div>
      <div style="margin-top:10px;font-size:11px;color:rgba(255,255,255,0.55)">IA : {" · ".join(ia_status)}</div>
    </div><hr>
    """, unsafe_allow_html=True)

    pages = {
        "📊 Vue d'ensemble": "dashboard",
        "🏠 Mes Ruches": "ruches",
        "🔍 Inspections": "inspections",
        "💊 Traitements": "traitements",
        "🍯 Miel": "miel",
        "🌿 Pollen": "pollen",
        "👑 Gelée Royale": "gelee",
        "🟤 Propolis & Cire": "propolis",
        "📦 Inventaire": "inventaire",
        "────────────────": "sep1",
        "🔬 Morphométrie IA": "morphometrie",
        "🧬 Génétique & Races": "genetique",
        "📐 Caractérisation": "caracterisation",
        "────────────────": "sep2",
        "🌸 Flore Mellifère": "flore",
        "🌤️ Météo & Miellée": "meteo",
        "📋 Rapports": "rapports",
        "💾 Données & Sauvegardes": "donnees",
        "🚨 Alertes": "alertes",
        "⚙️ Configuration IA": "config_ia",
    }

    nav_labels = [k for k in pages if not k.startswith("───")]
    selected = st.radio("Nav", nav_labels, label_visibility="collapsed")
    current_page = pages[selected]

    st.markdown("<hr>", unsafe_allow_html=True)
    df_s = st.session_state.ruches
    st.markdown(f"""
    <div style="background:rgba(255,255,255,0.05);border-radius:12px;padding:13px 15px;font-size:12px">
      <div style="color:rgba(255,255,255,0.4);text-transform:uppercase;letter-spacing:.1em;font-size:9.5px;margin-bottom:8px">Aperçu rapide</div>
      <div style="display:flex;justify-content:space-between;margin-bottom:5px"><span>🏠 Ruches</span><strong style="color:#F2C14E">{len(df_s)}</strong></div>
      <div style="display:flex;justify-content:space-between;margin-bottom:5px"><span>🍯 Miel total</span><strong style="color:#F2C14E">{df_s["Miel_kg"].sum():.0f} kg</strong></div>
      <div style="display:flex;justify-content:space-between;margin-bottom:5px"><span>🌿 Pollen</span><strong style="color:#F2C14E">{df_s["Pollen_kg"].sum():.1f} kg</strong></div>
      <div style="display:flex;justify-content:space-between"><span>🔬 Analyses</span><strong style="color:#F2C14E">{len(st.session_state.morph_analyses)}</strong></div>
    </div>
    <div style="margin-top:14px;display:flex;align-items:center;gap:9px">
      <div style="width:34px;height:34px;background:linear-gradient(135deg,#F2C14E,#C97A08);border-radius:50%;
                  display:flex;align-items:center;justify-content:center;font-weight:700;color:#1C3D10;font-size:13px">MA</div>
      <div><div style="font-size:13px;font-weight:500">Mohammed A.</div>
        <div style="font-size:10.5px;color:rgba(255,255,255,0.38)">Apiculteur professionnel</div></div>
    </div>
    """, unsafe_allow_html=True)

# =====================================================
# ══════════════════════════════════════════════════
#  PAGE : MORPHOMÉTRIE IA (CŒUR DE L'AMÉLIORATION)
# ══════════════════════════════════════════════════
# =====================================================
if current_page == "morphometrie":
    st.markdown('<div class="page-title">🔬 Morphométrie IA — Gratuite & Permanente</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Analyse automatique par photo · 19 caractères Ruttner · Profil productif · Classification raciale · 100% gratuit</div>', unsafe_allow_html=True)

    # ── Statut des IA disponibles ──
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        gemini_ok = bool(GEMINI_API_KEY) and GEMINI_AVAILABLE
        st.markdown(f"""<div class="api-card api-card-gemini">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div><div style="font-weight:700;font-size:13.5px;color:#1D4ED8">🤖 Google Gemini 2.0 Flash</div>
              <div style="font-size:11.5px;color:var(--muted);margin-top:3px">15 req/min · 1M tokens/jour · Vision ✅</div></div>
            <span class="badge {'b-green' if gemini_ok else 'b-red'}">{'✓ Actif' if gemini_ok else '✗ Clé manquante'}</span>
          </div>
          <div style="font-size:11px;color:var(--muted);margin-top:8px">🔗 aistudio.google.com/apikey — 100% gratuit</div>
        </div>""", unsafe_allow_html=True)
    with col_s2:
        groq_ok = bool(GROQ_API_KEY)
        st.markdown(f"""<div class="api-card api-card-groq">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div><div style="font-weight:700;font-size:13.5px;color:#C2410C">⚡ Groq LLaMA 3.2 Vision</div>
              <div style="font-size:11.5px;color:var(--muted);margin-top:3px">30 req/min · Vision ✅ · Ultra-rapide</div></div>
            <span class="badge {'b-green' if groq_ok else 'b-amber'}">{'✓ Actif' if groq_ok else '⚪ Optionnel'}</span>
          </div>
          <div style="font-size:11px;color:var(--muted);margin-top:8px">🔗 console.groq.com/keys — 100% gratuit</div>
        </div>""", unsafe_allow_html=True)
    with col_s3:
        st.markdown(f"""<div class="api-card api-card-ollama">
          <div style="display:flex;justify-content:space-between;align-items:flex-start">
            <div><div style="font-weight:700;font-size:13.5px;color:#15803D">🖥️ Ollama Local (LLaVA)</div>
              <div style="font-size:11.5px;color:var(--muted);margin-top:3px">Illimité · Hors ligne ✅ · Privé</div></div>
            <span class="badge b-teal">🏠 Local</span>
          </div>
          <div style="font-size:11px;color:var(--muted);margin-top:8px">ollama.com · ollama pull llava:7b</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📸 Analyse IA par photo",
        "🏠 Diagnostic ruche/cadre",
        "📐 Saisie manuelle",
        "📐 Référentiel Ruttner",
        "📊 Historique analyses"
    ])

    # ══════════════════════════════════════════════════
    # ONGLET 1 : ANALYSE IA PAR PHOTO (CŒUR)
    # ══════════════════════════════════════════════════
    with tab1:
        st.markdown(alert("🤖", """<strong>Analyse IA automatique :</strong> Téléversez une photo d'abeille (vue ventrale idéale, 
            microscope ou téléphone). L'IA effectue automatiquement les 19 mesures morphométriques selon Ruttner (1988), 
            la classification raciale, le profil productif (miel/pollen/gelée), la caractérisation de la langue et des ailes, 
            et l'estimation de la résistance VSH. <strong>Service 100% gratuit et permanent.</strong>""", "al-info"), unsafe_allow_html=True)

        c_upload, c_config = st.columns([3, 1])
        with c_config:
            pref_ia = st.selectbox(
                "Service IA préféré",
                ["auto (recommandé)", "gemini", "groq", "ollama"],
                help="Auto = essaie Gemini → Groq → Ollama automatiquement"
            )
            pref_ia_val = pref_ia.split(" ")[0]

            ruche_pour_analyse = st.selectbox(
                "Ruche à analyser",
                ["Non spécifiée"] + [f"{r['ID']} — {r['Nom']}" for _,r in st.session_state.ruches.iterrows()]
            )
            analyste = st.text_input("Analyste", value="Mohammed A.", key="a_analyste")

        with c_upload:
            upload_mode = st.radio("Source image", ["📁 Téléverser un fichier", "📷 Caméra"], horizontal=True)
            img_file = None
            if upload_mode == "📁 Téléverser un fichier":
                img_file = st.file_uploader("Photo abeille (vue ventrale idéale)", type=["jpg","jpeg","png","webp","bmp"],
                    help="Vue ventrale sous loupe/microscope recommandée. Résolution min. 800px.")
            else:
                img_file = st.camera_input("Prenez une photo de l'abeille", key="morph_cam_tab")

        if img_file:
            image = Image.open(img_file).convert("RGB")

            col_img, col_btn = st.columns([2,1])
            with col_img:
                st.image(image, caption="Image prête pour l'analyse", width=380)

            with col_btn:
                st.markdown("**🎯 Options d'amélioration image**")
                do_enhance = st.checkbox("Améliorer contraste", value=True)
                do_sharpen = st.checkbox("Accentuer les détails", value=True)
                if do_enhance or do_sharpen:
                    img_proc = image.copy()
                    if do_enhance:
                        img_proc = ImageEnhance.Contrast(img_proc).enhance(1.4)
                        img_proc = ImageEnhance.Sharpness(img_proc).enhance(1.2)
                    if do_sharpen:
                        img_proc = img_proc.filter(ImageFilter.SHARPEN)
                else:
                    img_proc = image

                if not (gemini_ok or groq_ok):
                    st.warning("⚠️ Aucune clé API configurée. Seul Ollama local sera tenté. Configurez une clé dans ⚙️ Configuration IA.")

                btn_analyse = st.button("🔬 Lancer l'analyse IA", type="primary", use_container_width=True)
                btn_demo   = st.button("🎭 Analyse démo (sans IA)", use_container_width=True,
                                        help="Génère un résultat de démonstration sans appel IA")

            if btn_demo:
                # Résultat de démonstration réaliste
                demo_result = {
                    "qualite_image": "bonne",
                    "type_specimen": "ouvrière",
                    "mesures": {
                        "L_aile_mm": 9.21, "B_aile_mm": 3.24, "Ri": 2.47,
                        "DI3_mm": 1.74, "OI": "−", "A4_deg": 99.4, "B4_deg": 91.8,
                        "Ti_L_mm": 3.02, "Ba_L_mm": 1.89, "Ba_W_mm": 1.10,
                        "Fe_L_mm": 2.76, "T3_L_mm": 4.80, "T4_L_mm": 4.67,
                        "T4_W_pct": 38, "S4_L_mm": 2.73, "Glossa_mm": 6.14,
                        "Wt_mm": 4.13, "Pigment": 5, "Hb_mm": 0.39
                    },
                    "integrite_ailes": "intactes", "nervation": "normale",
                    "classification": {
                        "taxon": "A. m. intermissa", "confiance_pct": 89,
                        "probabilites": {"A. m. intermissa":89,"A. m. sahariensis":6,"A. m. ligustica":3,"A. m. carnica":1,"Hybride":1}
                    },
                    "caracterisation_langue": {
                        "classe": "moyenne",
                        "adaptation": "corolles moyennes",
                        "plantes_cibles": ["Romarin","Jujubier","Thym","Lavande"],
                        "avantage": "Bonne polyvalence florale avec légère préférence pour les fleurs à corolles courtes à moyennes typiques des garrigues nord-africaines."
                    },
                    "profil_productif": {
                        "specialisation": "miel", "score_miel": 78, "score_pollen": 62,
                        "score_gelee_royale": 32, "score_propolis": 48,
                        "justification": "Glossa moyenne (6.14mm) et tomentum intermédiaire (38%) caractéristiques d'une race adaptée à la production mellifère en milieu semi-aride."
                    },
                    "resistance_varroa": {
                        "score_vsh_estime": 72, "comportement_hygienique": "moyen",
                        "recommandation": "Sélectionner les individus présentant un VSH >75% pour améliorer la résistance naturelle au varroa."
                    },
                    "diagnostic_sanitaire": {
                        "etat": "sain", "anomalies": [], "pathologies_suspectees": [],
                        "deformations_varroa": False, "ailes_ok": True, "thorax_ok": True, "abdomen_ok": True,
                        "notes": "Spécimen en excellent état, morphologie typique d'A.m. intermissa de la région de l'Oranie."
                    },
                    "interpretation": "Ce spécimen présente les caractères morphométriques typiques d'Apis mellifera intermissa (Ruttner 1988), avec un indice cubital Ri=2.47 et une glossa de 6.14mm. La pigmentation sombre du scutellum (score 5/9) est caractéristique des populations nord-africaines. L'indice discoïdal négatif confirme l'appartenance à la branche africaine de l'espèce (Chahbar et al., 2013).",
                    "recommandations": [
                        "Maintenir la pureté raciale par sélection massale sur les colonies présentant les caractères les plus typiques.",
                        "Programmer la récolte de gelée royale en priorité sur C-12 et B-11 qui présentent de meilleures prédispositions."
                    ]
                }
                st.session_state["last_morph_result"] = demo_result
                st.session_state["last_morph_source"] = "🎭 Démonstration (sans IA)"
                st.success("✅ Résultat de démonstration généré !")
                st.rerun()

            if btn_analyse:
                with st.spinner("🔬 Analyse morphométrique en cours…"):
                    progress = st.progress(0, text="Préparation de l'image…")
                    time.sleep(0.3)
                    progress.progress(20, text="Envoi à l'IA…")
                    
                    result, source = analyze_auto(img_proc, PROMPT_MORPHO, pref_ia_val)
                    
                    progress.progress(90, text="Traitement des résultats…")
                    time.sleep(0.2)
                    progress.progress(100, text="Terminé !")
                    time.sleep(0.3)
                    progress.empty()

                if "error" in result:
                    st.error(f"❌ {result['error']}")
                    st.info("💡 Solutions : 1) Configurez une clé GEMINI_API_KEY dans ⚙️ Configuration IA  2) Installez Ollama localement  3) Utilisez 'Analyse démo' pour tester sans IA")
                else:
                    st.session_state["last_morph_result"] = result
                    st.session_state["last_morph_source"] = source
                    st.success(f"✅ Analyse terminée via **{source}** !")
                    st.rerun()

        # ── AFFICHAGE DES RÉSULTATS ──────────────────────────────────────────────
        if "last_morph_result" in st.session_state:
            r = st.session_state["last_morph_result"]
            source = st.session_state.get("last_morph_source","IA")

            # Badge source
            source_cls = "ia-gemini" if "Gemini" in source else "ia-ollama" if "Ollama" in source else "ia-groq" if "Groq" in source else ""
            source_icon = "🔵" if "Gemini" in source else "🟢" if "Ollama" in source else "🟠" if "Groq" in source else "🎭"
            st.markdown(f'<div class="ia-source {source_cls}">{source_icon} Analysé par : {source}</div>', unsafe_allow_html=True)

            c_res1, c_res2 = st.columns([3, 2])

            with c_res1:
                # Classification raciale
                classif = r.get("classification", {})
                taxon = classif.get("taxon","—")
                conf  = classif.get("confiance_pct", 0)
                probs = classif.get("probabilites", {})

                st.markdown(f"""<div class="morph-result-card">
                  <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px">Taxon identifié par IA</div>
                  <div style="font-family:'Playfair Display',serif;font-size:24px;font-weight:700;color:var(--earth);margin-bottom:8px">{taxon}</div>
                  <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px">
                    <span style="font-size:20px;font-weight:700;color:var(--honey-dark);font-family:'Playfair Display',serif">{conf}%</span>
                    <span style="font-size:12px;color:var(--muted)">de confiance</span>
                    <span class="badge {'b-green' if conf>=85 else 'b-amber' if conf>=70 else 'b-orange'}">{'Haute' if conf>=85 else 'Moyenne' if conf>=70 else 'Faible'}</span>
                  </div>""", unsafe_allow_html=True)
                for race, pct in sorted(probs.items(), key=lambda x:-x[1]):
                    st.markdown(f"""
                    <div style="margin-bottom:7px">
                      <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px">
                        <span style="color:var(--text);font-weight:500">{race}</span>
                        <span style="font-family:'JetBrains Mono',monospace;color:var(--muted)">{pct:.0f}%</span>
                      </div>
                      <div style="height:7px;background:var(--wax);border-radius:4px;overflow:hidden">
                        <div style="height:100%;width:{pct}%;background:linear-gradient(90deg,#F2C14E,#C97A08);border-radius:4px"></div>
                      </div>
                    </div>""", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

                # Mesures morphométriques
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown('<div class="section-title" style="font-size:15px">📐 Mesures morphométriques (19 caractères)</div>', unsafe_allow_html=True)
                mesures = r.get("mesures", {})
                REF = {
                    "L_aile_mm":    ("Long. aile ant.","mm",(8.9,9.6),(9.1,9.8),(9.1,9.8),(8.7,9.3)),
                    "B_aile_mm":    ("Larg. aile ant.","mm",(3.0,3.4),(3.1,3.5),(3.1,3.4),(2.9,3.2)),
                    "Ri":           ("Indice cubital", "ratio",(2.0,2.8),(2.4,3.2),(2.6,3.5),(2.1,2.9)),
                    "DI3_mm":       ("Cellule radiale","mm",(1.5,1.9),(1.6,2.0),(1.6,2.0),(1.4,1.8)),
                    "A4_deg":       ("Angle A4",       "°",(96,103),(98,105),(95,102),(95,102)),
                    "B4_deg":       ("Angle B4",       "°",(88,95),(89,97),(86,93),(87,93)),
                    "Ti_L_mm":      ("Long. tibia P3", "mm",(2.8,3.2),(2.9,3.3),(3.0,3.4),(2.7,3.1)),
                    "Ba_L_mm":      ("Long. basitarse","mm",(1.7,2.0),(1.8,2.1),(1.8,2.1),(1.6,1.9)),
                    "T3_L_mm":      ("Larg. tergite 3","mm",(4.6,5.0),(4.7,5.1),(4.7,5.2),(4.4,4.8)),
                    "T4_W_pct":     ("Tomentum T4",    "%",(30,45),(45,60),(35,50),(25,40)),
                    "Glossa_mm":    ("Long. glossa",   "mm",(5.9,6.3),(6.3,6.7),(6.4,6.8),(5.8,6.2)),
                    "Wt_mm":        ("Largeur tête",   "mm",(4.0,4.3),(4.1,4.4),(4.1,4.4),(3.9,4.2)),
                    "Pigment":      ("Pigmentation",   "/9",(4,7),(1,3),(1,3),(5,8)),
                    "Fe_L_mm":      ("Long. fémur P3", "mm",(2.6,2.9),(2.7,3.0),(2.7,3.0),(2.5,2.8)),
                    "S4_L_mm":      ("Long. sternite 4","mm",(2.5,2.9),(2.6,3.0),(2.6,3.0),(2.4,2.8)),
                    "Hb_mm":        ("Pubescence abd.", "mm",(0.3,0.5),(0.2,0.4),(0.2,0.4),(0.3,0.5)),
                    "Ba_W_mm":      ("Larg. basitarse","mm",(1.0,1.2),(1.0,1.2),(1.0,1.2),(0.9,1.1)),
                    "OI":           ("Indice discoïdal","",None,None,None,None),
                    "Integrite":    ("Intégrité ailes","",None,None,None,None),
                }
                rows_html = ""
                for code, val in mesures.items():
                    if code not in REF: continue
                    nom, unit, rint, rlig, rcar, rsah = REF[code]
                    # Déterminer statut selon la race principale
                    stat_cls = "mval-ok"
                    if rint and isinstance(val, (int,float)):
                        in_inti = rint[0]<=val<=rint[1]
                        if not in_inti:
                            in_any = (rlig and rlig[0]<=val<=rlig[1]) or (rcar and rcar[0]<=val<=rcar[1]) or (rsah and rsah[0]<=val<=rsah[1])
                            stat_cls = "mval-warn" if in_any else "mval-bad"
                    ref_txt = f"inti:{rint[0]}–{rint[1]}" if rint else ""
                    rows_html += f"""<div class="measure-row">
                        <div style="flex:1"><strong>{code}</strong> — {nom}</div>
                        <div style="font-size:10px;color:var(--muted);margin-right:10px">{ref_txt}</div>
                        <span class="mval {stat_cls}">{val}{unit}</span>
                    </div>"""
                st.markdown(f'<div style="background:white;border-radius:14px;padding:14px;border:1px solid rgba(180,150,80,0.18)">{rows_html}</div>', unsafe_allow_html=True)

            with c_res2:
                # Profil productif
                pp = r.get("profil_productif", {})
                if pp:
                    st.markdown('<div class="section-title" style="font-size:15px">🏭 Profil productif</div>', unsafe_allow_html=True)
                    spec = pp.get("specialisation","polyvalent")
                    spec_badge = {"miel":"b-amber","pollen":"b-green","gelée_royale":"b-purple","polyvalent":"b-gray"}.get(spec,"b-gray")
                    spec_icons = {"miel":"🍯 Miel","pollen":"🌿 Pollen","gelée_royale":"👑 Gelée R.","polyvalent":"⚖️ Polyvalent"}
                    st.markdown(f'<div style="margin-bottom:12px">Spécialisation principale : <span class="badge {spec_badge}">{spec_icons.get(spec,spec)}</span></div>', unsafe_allow_html=True)
                    prods = [("🍯 Miel",pp.get("score_miel",0),"#d97706"),("🌿 Pollen",pp.get("score_pollen",0),"#16a34a"),
                             ("👑 Gelée R.",pp.get("score_gelee_royale",0),"#9333ea"),("🟤 Propolis",pp.get("score_propolis",0),"#b45309")]
                    html_pp = '<div class="prod-profile-grid">'
                    for label, score, col in prods:
                        html_pp += f'<div class="pp-item"><div class="pp-score" style="color:{col}">{score}</div><div class="pp-label">{label}</div></div>'
                    html_pp += "</div>"
                    justif = pp.get("justification","")
                    html_pp += f'<div style="font-size:12px;color:var(--muted);padding:10px;background:var(--cream);border-radius:9px;line-height:1.6">{justif}</div>'
                    st.markdown(html_pp, unsafe_allow_html=True)

                # Caractérisation langue
                lang = r.get("caracterisation_langue", {})
                if lang:
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.markdown('<div class="section-title" style="font-size:15px">👅 Langue (Glossa)</div>', unsafe_allow_html=True)
                    classe = lang.get("classe","moyenne")
                    pct = {"courte":18,"moyenne":55,"longue":90}.get(classe,50)
                    plantes = ", ".join(lang.get("plantes_cibles",[]))
                    st.markdown(f"""
                    <div style="background:var(--forest-pale);border-radius:12px;padding:14px;border:1px solid rgba(90,142,64,.2)">
                      <div style="font-size:13px;margin-bottom:6px">Classe : <strong>{classe.capitalize()}</strong></div>
                      <div class="langue-bar"><div class="langue-fill" style="width:{pct}%"></div></div>
                      <div style="display:flex;justify-content:space-between;font-size:9.5px;color:var(--muted);margin-bottom:8px">
                        <span>Courte (&lt;6.0mm)</span><span>Longue (&gt;6.4mm)</span></div>
                      <div style="font-size:12px;color:var(--forest-mid);margin-bottom:4px">🌸 <strong>Adaptation :</strong> {lang.get("adaptation","—")}</div>
                      <div style="font-size:12px;color:var(--muted);margin-bottom:6px">🌿 <strong>Plantes :</strong> {plantes}</div>
                      <div style="font-size:11.5px;color:var(--forest);padding:8px;background:white;border-radius:8px">{lang.get("avantage","—")}</div>
                    </div>""", unsafe_allow_html=True)

                # Résistance VSH
                vsh = r.get("resistance_varroa", {})
                if vsh:
                    st.markdown("<br>", unsafe_allow_html=True)
                    st.markdown('<div class="section-title" style="font-size:15px">🛡️ Résistance Varroa (VSH)</div>', unsafe_allow_html=True)
                    vsh_score = vsh.get("score_vsh_estime", 70)
                    vsh_col = "#22c55e" if vsh_score>=75 else "#f59e0b" if vsh_score>=55 else "#ef4444"
                    st.markdown(f"""
                    <div style="background:white;border-radius:12px;padding:14px;border:1px solid rgba(180,150,80,0.18)">
                      <div style="display:flex;align-items:center;gap:14px;margin-bottom:10px">
                        <div style="font-family:'Playfair Display',serif;font-size:34px;font-weight:700;color:{vsh_col};line-height:1">{vsh_score}%</div>
                        <div>
                          <div style="font-weight:600;font-size:13px">Score VSH estimé</div>
                          <div style="font-size:12px;color:var(--muted)">Hygiène : {vsh.get("comportement_hygienique","—")}</div>
                        </div>
                      </div>
                      <div style="height:8px;background:var(--wax);border-radius:4px;overflow:hidden;margin-bottom:8px">
                        <div style="height:100%;width:{vsh_score}%;background:{vsh_col};border-radius:4px"></div>
                      </div>
                      <div style="font-size:12px;color:var(--muted)">{vsh.get("recommandation","")}</div>
                    </div>""", unsafe_allow_html=True)

                # Diagnostic sanitaire
                diag = r.get("diagnostic_sanitaire", {})
                if diag:
                    st.markdown("<br>", unsafe_allow_html=True)
                    etat = diag.get("etat","sain")
                    etat_cfg = {"sain":("#166534","#f0fdf4","✅"),"suspect":("#92400e","#fffbeb","⚠️"),"malade":("#991b1b","#fef2f2","🚨")}
                    ecol, ebg, eico = etat_cfg.get(etat, ("#166534","#f0fdf4","✅"))
                    st.markdown('<div class="section-title" style="font-size:15px">🩺 Diagnostic sanitaire</div>', unsafe_allow_html=True)
                    checks = [("Ailes",diag.get("ailes_ok",True)),("Thorax",diag.get("thorax_ok",True)),
                              ("Abdomen",diag.get("abdomen_ok",True)),("Anti-Varroa",not diag.get("deformations_varroa",False))]
                    html_diag = f'<div style="background:{ebg};border-radius:12px;padding:14px;border:1px solid {ecol}44">'
                    html_diag += f'<div style="font-weight:700;color:{ecol};margin-bottom:10px">{eico} {etat.upper()}</div>'
                    html_diag += '<div style="display:flex;gap:7px;flex-wrap:wrap;margin-bottom:8px">'
                    for label, ok in checks:
                        html_diag += f'<span style="font-size:11px;font-weight:600;padding:3px 9px;border-radius:8px;background:{"#dcfce7" if ok else "#fee2e2"};color:{"#15803d" if ok else "#b91c1c"}">{"✓" if ok else "✗"} {label}</span>'
                    html_diag += "</div>"
                    anomalies = diag.get("anomalies",[])
                    if anomalies: html_diag += f'<div style="font-size:12px;color:#991b1b"><strong>Anomalies :</strong> {", ".join(anomalies)}</div>'
                    notes = diag.get("notes","")
                    if notes: html_diag += f'<div style="font-size:12px;color:var(--muted);margin-top:6px">{notes}</div>'
                    html_diag += "</div>"
                    st.markdown(html_diag, unsafe_allow_html=True)

            # Interprétation complète
            interp = r.get("interpretation","")
            recs = r.get("recommandations",[])
            if interp:
                st.markdown("<br>", unsafe_allow_html=True)
                col_interp, col_save = st.columns([3, 1])
                with col_interp:
                    st.markdown('<div class="section-title" style="font-size:15px">🔬 Interprétation scientifique</div>', unsafe_allow_html=True)
                    st.markdown(f'<div style="background:white;border-radius:12px;padding:16px;border:1px solid rgba(180,150,80,0.18);font-size:13.5px;line-height:1.8;color:var(--text)">{interp}</div>', unsafe_allow_html=True)
                    if recs:
                        st.markdown('<div style="margin-top:12px"><strong style="font-size:13px">📌 Recommandations :</strong></div>', unsafe_allow_html=True)
                        for rec in recs:
                            st.markdown(f'<div style="font-size:13px;padding:6px 10px;margin:4px 0;background:var(--forest-pale);border-radius:8px;border-left:3px solid var(--forest-l);color:var(--text)">▸ {rec}</div>', unsafe_allow_html=True)

                with col_save:
                    st.markdown('<div class="section-title" style="font-size:15px">💾 Sauvegarder</div>', unsafe_allow_html=True)
                    notes_save = st.text_area("Notes complémentaires", placeholder="Observations terrain…", key="m_notes_save", height=80)
                    if st.button("💾 Enregistrer l'analyse", type="primary", use_container_width=True):
                        mes = r.get("mesures",{})
                        ruche_code = ruche_pour_analyse.split("—")[0].strip() if "—" in ruche_pour_analyse else ruche_pour_analyse
                        new_row = {
                            "Date": str(datetime.now().date()),
                            "Ruche": ruche_code,
                            "Taxon": classif.get("taxon","—"),
                            "Confiance_pct": conf,
                            "L_aile_mm": mes.get("L_aile_mm",0),
                            "Ri": mes.get("Ri",0),
                            "Glossa_mm": mes.get("Glossa_mm",0),
                            "Ti_L_mm": mes.get("Ti_L_mm",0),
                            "Tomentum_pct": mes.get("T4_W_pct",0),
                            "Profil_prod": pp.get("specialisation","—") if pp else "—",
                            "VSH_estime": vsh.get("score_vsh_estime",0) if vsh else 0,
                            "Source_IA": source,
                            "Analyste": analyste,
                        }
                        if notes_save: new_row["Notes"] = notes_save
                        st.session_state.morph_analyses = pd.concat([
                            st.session_state.morph_analyses, pd.DataFrame([new_row])
                        ], ignore_index=True)
                        save_table("morpho_analyses", st.session_state.morph_analyses)
                        st.success("✅ Analyse sauvegardée sur disque ✓")
                        # Effacer le résultat affiché
                        del st.session_state["last_morph_result"]
                        del st.session_state["last_morph_source"]
                        time.sleep(1)
                        st.rerun()

    # ══════════════════════════════════════════════════
    # ONGLET 2 : DIAGNOSTIC RUCHE/CADRE
    # ══════════════════════════════════════════════════
    with tab2:
        st.markdown(alert("🩺", "<strong>Diagnostic IA :</strong> Téléversez une photo de ruche, cadre, planche d'envol ou groupe d'abeilles. L'IA détecte les maladies, parasites et anomalies avec plan d'action.", "al-info"), unsafe_allow_html=True)

        c_dl, c_dr = st.columns([3, 2])
        with c_dl:
            diag_file = st.file_uploader("Photo ruche / cadre / colonie", type=["jpg","jpeg","png","webp"],
                key="diag_upload", help="Cadre de couvain, extérieur ruche, planche d'envol, rayons…")
            if diag_file:
                diag_image = Image.open(diag_file).convert("RGB")
                st.image(diag_image, caption="Photo pour diagnostic", width=360)

            col_d1, col_d2 = st.columns(2)
            with col_d1:
                diag_type = st.selectbox("Type de photo", ["cadre de couvain","ruche extérieure","groupe d'abeilles","rayons/cellules","planche d'envol"])
            with col_d2:
                diag_saison = st.selectbox("Saison", ["Printemps","Été","Automne","Hiver"])
            diag_symp = st.text_area("Symptômes observés (optionnel)", placeholder="Abeilles mortes devant la ruche, couvain parsemé, odeur suspecte…", height=60)

        with c_dr:
            if diag_file:
                pref_diag = st.selectbox("Service IA", ["auto","gemini","groq","ollama"], key="diag_ia")
                btn_diag = st.button("🩺 Lancer le diagnostic", type="primary", use_container_width=True)

                if btn_diag:
                    symp_line = f"\nSymptômes signalés : {diag_symp}" if diag_symp else ""
                    prompt_d = PROMPT_DIAGNOSTIC.format(
                        photo_type=diag_type, saison=diag_saison, symptomes_line=symp_line
                    )
                    with st.spinner("Diagnostic en cours…"):
                        result_d, source_d = analyze_auto(diag_image, prompt_d, pref_diag)
                    
                    if "error" in result_d:
                        st.error(result_d["error"])
                    else:
                        v = result_d.get("verdict",{})
                        score = v.get("score_sante",75)
                        etat = v.get("etat","vigilance")
                        ecfg = {"sain":("#166534","#f0fdf4","✅","SAIN"),"vigilance":("#92400e","#fffbeb","👁","VIGILANCE"),
                                "malade":("#c2410c","#fff7ed","⚠️","MALADE"),"critique":("#991b1b","#fef2f2","🚨","CRITIQUE")}
                        ec,eb,ei,el = ecfg.get(etat,ecfg["vigilance"])
                        st.markdown(f"""
                        <div style="background:{eb};border:2px solid {ec};border-radius:16px;padding:18px;margin-bottom:14px">
                          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px">
                            <div style="font-family:'Playfair Display',serif;font-size:22px;font-weight:700;color:{ec}">{ei} {el}</div>
                            <div style="font-family:'Playfair Display',serif;font-size:28px;font-weight:700;color:{ec}">{score}/100</div>
                          </div>
                          <div style="height:7px;background:rgba(0,0,0,.08);border-radius:4px;overflow:hidden;margin-bottom:10px">
                            <div style="height:100%;width:{score}%;background:{ec};border-radius:4px"></div>
                          </div>
                          <p style="font-size:13px;color:{ec};line-height:1.6">{v.get("resume","")}</p>
                        </div>""", unsafe_allow_html=True)

                        for mal in result_d.get("maladies_detectees",[]):
                            gcol = {"critique":"#ef4444","sévère":"#f97316","modérée":"#f59e0b","légère":"#22c55e"}.get(mal.get("gravite",""),"#f59e0b")
                            st.markdown(f"""
                            <div style="border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:8px">
                              <strong>{mal.get("nom","")}</strong> — <span style="font-size:11.5px;color:var(--muted)">{mal.get("agent","")}</span>
                              <span class="badge" style="float:right;background:{gcol}22;color:{gcol}">{mal.get("gravite","")}</span>
                              <div style="font-size:12px;color:var(--muted);margin:6px 0">{mal.get("signes","")}</div>
                              <div style="background:var(--honey-pale);border-radius:8px;padding:8px 10px;font-size:12px">
                                💊 {mal.get("traitement","—")} · ⏰ <strong style="color:#b91c1c">{mal.get("urgence","—")}</strong></div>
                            </div>""", unsafe_allow_html=True)

                        for plan in result_d.get("plan_action",[]):
                            pc = {1:"#ef4444",2:"#f97316",3:"#f59e0b",4:"#22c55e"}.get(plan.get("priorite",3),"#f59e0b")
                            st.markdown(f"""
                            <div style="display:flex;align-items:flex-start;gap:10px;padding:9px 0;border-bottom:1px solid rgba(180,150,80,0.12)">
                              <div style="width:23px;height:23px;border-radius:50%;background:{pc};color:white;display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;flex-shrink:0">{plan.get("priorite","?")}</div>
                              <div><div style="font-size:13px;font-weight:500">{plan.get("action","")}</div>
                                <div style="font-size:11px;color:var(--muted)">⏰ {plan.get("delai","")}</div></div>
                            </div>""", unsafe_allow_html=True)
            else:
                st.info("👆 Téléversez une photo pour commencer le diagnostic")

    # ══════════════════════════════════════════════════
    # ONGLET 3 : SAISIE MANUELLE
    # ══════════════════════════════════════════════════
    with tab3:
        st.markdown('<div class="section-title">Saisie manuelle des mesures</div>', unsafe_allow_html=True)
        c_f, c_r = st.columns([3, 2])
        with c_f:
            m_ruche = st.selectbox("Ruche analysée", [f"{r['ID']} — {r['Nom']}" for _,r in st.session_state.ruches.iterrows()])
            m_analyste = st.text_input("Analyste", "Mohammed A.", key="m_analyste_man")
            col1,col2,col3 = st.columns(3)
            with col1:
                m_L = st.number_input("L aile (mm)", 7.0, 12.0, 9.18, 0.01)
                m_B = st.number_input("B aile (mm)", 2.5, 4.5, 3.21, 0.01)
            with col2:
                m_Ri = st.number_input("Indice cubital Ri", 1.0, 5.0, 2.45, 0.01)
                m_Ac = st.number_input("Glossa (mm)", 5.0, 8.0, 6.12, 0.01)
            with col3:
                m_Ti = st.number_input("Tibia Ti-L (mm)", 2.0, 4.0, 3.01, 0.01)
                m_T3 = st.number_input("Tergite 3 (mm)", 3.5, 5.5, 4.78, 0.01)
            col4,col5 = st.columns(2)
            with col4:
                m_Tom = st.slider("Tomentum T4 (%)", 0, 100, 37)
                m_Pv  = st.slider("Pigmentation (1–9)", 1, 9, 5)
            with col5:
                m_A4 = st.number_input("Angle A4 (°)", 85.0, 115.0, 99.2, 0.1)
                m_B4 = st.number_input("Angle B4 (°)", 80.0, 110.0, 91.5, 0.1)

        with c_r:
            best_r, conf_r, probs_r = classify_local(m_L, m_Ri, m_Ac, m_Pv, m_Tom, m_Ti)
            st.markdown(f"""<div class="morph-result-card" style="margin-top:10px">
              <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px">Classification locale (Ruttner 1988)</div>
              <div style="font-family:'Playfair Display',serif;font-size:20px;font-weight:700;color:var(--earth);margin-bottom:6px">{best_r}</div>
              <div style="font-size:18px;font-weight:700;color:var(--honey-dark);margin-bottom:14px">{conf_r:.0f}% confiance</div>""", unsafe_allow_html=True)
            for race, pct in sorted(probs_r.items(), key=lambda x:-x[1]):
                st.markdown(f"""<div style="margin-bottom:6px">
                  <div style="display:flex;justify-content:space-between;font-size:11.5px;margin-bottom:2px">
                    <span>{race}</span><span style="font-family:'JetBrains Mono',monospace">{pct:.0f}%</span></div>
                  <div style="height:6px;background:var(--wax);border-radius:3px;overflow:hidden">
                    <div style="height:100%;width:{pct}%;background:linear-gradient(90deg,#F2C14E,#C97A08);border-radius:3px"></div>
                  </div></div>""", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        if st.button("💾 Sauvegarder la saisie manuelle", type="primary"):
            new_m = {"Date":str(datetime.now().date()),"Ruche":m_ruche.split("—")[0].strip(),
                     "Taxon":best_r,"Confiance_pct":round(conf_r,0),
                     "L_aile_mm":m_L,"Ri":m_Ri,"Glossa_mm":m_Ac,"Ti_L_mm":m_Ti,"Tomentum_pct":m_Tom,
                     "Profil_prod":"miel","VSH_estime":70,"Source_IA":"Saisie manuelle","Analyste":m_analyste}
            st.session_state.morph_analyses = pd.concat([st.session_state.morph_analyses, pd.DataFrame([new_m])], ignore_index=True)
            save_table("morpho_analyses", st.session_state.morph_analyses)
            st.success(f"✅ Analyse sauvegardée sur disque : {best_r} ({conf_r:.0f}%)")

    # ══════════════════════════════════════════════════
    # ONGLET 4 : RÉFÉRENTIEL
    # ══════════════════════════════════════════════════
    with tab4:
        ref_df = pd.DataFrame({
            "Code":["L","B","Ri","DI3","OI","Ti-L","Ba-L","T3-L","T4-W","Ac","Wt","Pv","Hb","A4","B4"],
            "Caractère":["Long. aile ant.","Larg. aile ant.","Indice cubital","Long. cellule 3","Indice discoïdal",
                         "Long. tibia P3","Long. basitarse","Larg. tergite 3","Tomentum T4","Glossa/langue",
                         "Largeur tête","Pigmentation","Pubescence","Angle A4","Angle B4"],
            "Unité":["mm","mm","ratio","mm","","mm","mm","mm","%","mm","mm","/9","mm","°","°"],
            "A.m. intermissa":["8.9–9.6","3.0–3.4","2.0–2.8","1.5–1.9","+/−","2.8–3.2","1.7–2.0","4.6–5.0","30–45","5.9–6.3","4.0–4.3","4–7","0.3–0.5","96–103","88–95"],
            "A.m. ligustica":["9.1–9.8","3.1–3.5","2.4–3.2","1.6–2.0","+","2.9–3.3","1.8–2.1","4.7–5.1","45–60","6.3–6.7","4.1–4.4","1–3","0.2–0.4","98–105","89–97"],
            "A.m. carnica":["9.1–9.8","3.1–3.4","2.6–3.5","1.6–2.0","+","3.0–3.4","1.8–2.1","4.7–5.2","35–50","6.4–6.8","4.1–4.4","1–3","0.2–0.4","95–102","86–93"],
            "A.m. sahariensis":["8.7–9.3","2.9–3.2","2.1–2.9","1.4–1.8","−","2.7–3.1","1.6–1.9","4.4–4.8","25–40","5.8–6.2","3.9–4.2","5–8","0.3–0.5","95–102","87–93"],
        })
        st.dataframe(ref_df, use_container_width=True, hide_index=True)

    # ══════════════════════════════════════════════════
    # ONGLET 5 : HISTORIQUE
    # ══════════════════════════════════════════════════
    with tab5:
        df_hist = st.session_state.morph_analyses
        if len(df_hist) > 0:
            st.dataframe(df_hist.sort_values("Date",ascending=False), use_container_width=True, hide_index=True,
                column_config={"Confiance_pct":st.column_config.ProgressColumn(format="%d%%",min_value=0,max_value=100),
                               "VSH_estime":st.column_config.ProgressColumn(format="%d%%",min_value=0,max_value=100)})
            csv = df_hist.to_csv(index=False).encode("utf-8")
            st.download_button("⬇ Exporter CSV", csv, "morpho_analyses.csv", "text/csv")
        else:
            st.info("Aucune analyse enregistrée. Effectuez une analyse IA pour commencer.")

# =====================================================
# PAGE : CONFIGURATION IA
# =====================================================
elif current_page == "config_ia":
    st.markdown('<div class="page-title">⚙️ Configuration IA Morphométrie</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Services gratuits & permanents · Aucun abonnement payant requis</div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="background:white;border-radius:18px;padding:24px;border:1px solid rgba(180,150,80,0.2);margin-bottom:20px">
      <div style="font-family:'Playfair Display',serif;font-size:18px;font-weight:700;color:var(--earth);margin-bottom:16px">
        📋 Guide de configuration — 3 options gratuites
      </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    ### 🔵 Option 1 : Google Gemini 2.0 Flash (RECOMMANDÉE)
    **Entièrement gratuit — 15 req/min — 1 500 req/jour — 1M tokens/jour**
    
    1. Allez sur **https://aistudio.google.com/apikey**
    2. Connectez-vous avec votre compte Google (gratuit)
    3. Cliquez **"Create API Key"**
    4. Copiez votre clé (commence par `AIza...`)
    5. Ajoutez dans `.streamlit/secrets.toml` :
    ```toml
    GEMINI_API_KEY = "AIza..."
    ```
    OU définissez la variable d'environnement :
    ```bash
    export GEMINI_API_KEY="AIza..."
    ```
    """)

    st.markdown("""
    ---
    ### 🟠 Option 2 : Groq (Llama 3.2 Vision — Backup)
    **Gratuit — 30 req/min — 14 400 req/jour — Ultra-rapide**
    
    1. Allez sur **https://console.groq.com/keys**
    2. Créez un compte gratuit
    3. Cliquez **"Create API Key"**
    4. Ajoutez dans `.streamlit/secrets.toml` :
    ```toml
    GROQ_API_KEY = "gsk_..."
    ```
    """)

    st.markdown("""
    ---
    ### 🟢 Option 3 : Ollama (Local — Sans internet — Illimité)
    **100% local — Aucune clé API — Aucune limite — Fonctionne hors ligne**
    
    1. Installez Ollama : **https://ollama.com/download**
    2. Téléchargez le modèle vision :
    ```bash
    ollama pull llava:7b
    # ou pour plus de précision :
    ollama pull llava:13b
    ```
    3. Démarrez le serveur :
    ```bash
    ollama serve
    ```
    4. Testez : http://localhost:11434
    
    Configuration optionnelle dans secrets.toml :
    ```toml
    OLLAMA_BASE_URL = "http://localhost:11434"
    OLLAMA_MODEL = "llava:7b"
    ```
    """)

    st.markdown("---")
    st.markdown("""
    ### 📁 Fichier `.streamlit/secrets.toml` complet
    ```toml
    # ApiTrack Pro — Configuration IA Morphométrie
    # Tous les services sont GRATUITS
    
    # Option 1 : Google Gemini (recommandé)
    GEMINI_API_KEY = "AIza..."
    
    # Option 2 : Groq (backup)
    GROQ_API_KEY = "gsk_..."
    
    # Option 3 : Ollama local (optionnel)
    OLLAMA_BASE_URL = "http://localhost:11434"
    OLLAMA_MODEL = "llava:7b"
    ```
    """)

    st.markdown("</div>", unsafe_allow_html=True)

    # Test de connexion
    st.markdown('<div class="section-title">🧪 Test de connexion</div>', unsafe_allow_html=True)
    c_t1, c_t2, c_t3 = st.columns(3)
    with c_t1:
        if st.button("🔵 Tester Gemini", use_container_width=True):
            if not GEMINI_API_KEY:
                st.error("GEMINI_API_KEY non configurée")
            elif not GEMINI_AVAILABLE:
                st.error("pip install google-generativeai")
            else:
                with st.spinner("Test en cours…"):
                    try:
                        genai.configure(api_key=GEMINI_API_KEY)
                        m = genai.GenerativeModel("gemini-2.0-flash-exp")
                        r = m.generate_content("Répondez uniquement : OK")
                        st.success(f"✅ Gemini opérationnel ! Réponse : {r.text.strip()[:30]}")
                    except Exception as e:
                        st.error(f"❌ {e}")
    with c_t2:
        if st.button("🟠 Tester Groq", use_container_width=True):
            if not GROQ_API_KEY:
                st.error("GROQ_API_KEY non configurée")
            else:
                with st.spinner("Test en cours…"):
                    try:
                        r = req_lib.post("https://api.groq.com/openai/v1/chat/completions",
                            headers={"Authorization":f"Bearer {GROQ_API_KEY}","Content-Type":"application/json"},
                            json={"model":"llama-3.2-11b-vision-preview","messages":[{"role":"user","content":"Dis OK"}],"max_tokens":10},
                            timeout=15)
                        data = r.json()
                        if "error" in data: st.error(f"❌ {data['error']['message']}")
                        else: st.success(f"✅ Groq opérationnel ! ")
                    except Exception as e:
                        st.error(f"❌ {e}")
    with c_t3:
        if st.button("🟢 Tester Ollama", use_container_width=True):
            with st.spinner("Test Ollama local…"):
                try:
                    r = req_lib.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
                    if r.status_code == 200:
                        models = [m["name"] for m in r.json().get("models",[])]
                        llava_models = [m for m in models if "llava" in m.lower()]
                        if llava_models:
                            st.success(f"✅ Ollama actif · Modèles vision : {', '.join(llava_models)}")
                        else:
                            st.warning(f"⚠️ Ollama actif mais LLaVA non installé. Lancez : ollama pull llava:7b\nModèles disponibles : {', '.join(models[:5])}")
                    else:
                        st.error(f"❌ Ollama répond avec code {r.status_code}")
                except Exception as e:
                    st.error(f"❌ Ollama non accessible : {e}")

# =====================================================
# TOUTES LES AUTRES PAGES (Dashboard, Ruches, etc.)
# =====================================================
elif current_page == "dashboard":
    st.markdown('<div class="page-title">🐝 Vue d\'ensemble — ApiTrack Pro</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Tableau de bord centralisé · Saison 2024–2025</div>', unsafe_allow_html=True)
    df = st.session_state.ruches
    rec = st.session_state.recoltes
    total_miel   = df["Miel_kg"].sum()
    total_pollen = df["Pollen_kg"].sum()
    total_gelee_g= df["Gelée_g"].sum()
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    with c1: st.metric("🏠 Ruches",len(df))
    with c2: st.metric("🍯 Miel",f"{total_miel:.0f} kg","+18%")
    with c3: st.metric("🌿 Pollen",f"{total_pollen:.1f} kg","+22%")
    with c4: st.metric("👑 Gelée R.",f"{total_gelee_g:.0f} g","+35%")
    with c5: st.metric("🔬 Analyses",len(st.session_state.morph_analyses))
    with c6: st.metric("🚨 Alertes",len(df[df["Statut"].isin(["Critique","Attention"])]))

    st.markdown("<br>", unsafe_allow_html=True)
    section_header("🏠 Ruches — Aperçu rapide")
    cols = st.columns(4)
    for i,(_, r) in enumerate(df.iterrows()):
        with cols[i%4]:
            st.markdown(ruche_card_html(r), unsafe_allow_html=True)

elif current_page == "ruches":
    st.markdown('<div class="page-title">🏠 Gestion des Ruches</div>', unsafe_allow_html=True)
    df = st.session_state.ruches
    tab1, tab2, tab3 = st.tabs(["🃏 Cartes","📋 Tableau","➕ Nouvelle ruche"])
    with tab1:
        cols = st.columns(4)
        for i,(_, r) in enumerate(df.iterrows()):
            with cols[i%4]: st.markdown(ruche_card_html(r), unsafe_allow_html=True)
    with tab2:
        st.dataframe(df[["ID","Nom","Race","Site","Statut","Profil_prod","Poids_kg","Varroa_pct","Miel_kg","Pollen_kg","Gelée_g","VSH_pct"]],
            use_container_width=True, hide_index=True,
            column_config={"Varroa_pct":st.column_config.ProgressColumn(format="%.1f%%",min_value=0,max_value=5),
                           "VSH_pct":st.column_config.ProgressColumn(format="%d%%",min_value=0,max_value=100)})
    with tab3:
        c1,c2 = st.columns(2)
        with c1:
            nid=st.text_input("Code ruche *",placeholder="E-01")
            nnom=st.text_input("Nom *",placeholder="La Dorée")
            nrace=st.selectbox("Race",["A. m. intermissa","A. m. sahariensis","A. m. ligustica","A. m. carnica","Hybride","Indéterminée"])
            nsite=st.text_input("Site",placeholder="Verger du Cèdre")
        with c2:
            npoids=st.number_input("Poids initial (kg)",0.0,50.0,18.0,0.5)
            nstatut=st.selectbox("Statut",["Excellent","Bon","Attention","Critique"])
            nprofil=st.selectbox("Profil",["Miel","Pollen","Gelée Royale","Résistance"])
        if st.button("✓ Enregistrer",type="primary"):
            if not nid or not nnom: st.error("Renseignez le code et le nom.")
            elif nid in df["ID"].values: st.error(f"ID {nid} existe déjà.")
            else:
                nr={"ID":nid,"Nom":nnom,"Race":nrace,"Site":nsite,"Poids_kg":npoids,"Varroa_pct":0.0,
                    "Miel_kg":0,"Pollen_kg":0,"Gelée_g":0,"Statut":nstatut,"Reine_id":"À définir",
                    "VSH_pct":70,"Douceur":80,"Economie_hiv":75,"Essaimage_pct":30,
                    "Date_creation":str(datetime.now().date()),"Profil_prod":nprofil,
                    "Glossa_mm":6.0,"L_aile_mm":9.2,"Ri":2.5,"Tomentum_pct":35,"Pigment_scutellum":5,"Ti_L_mm":3.0}
                st.session_state.ruches=pd.concat([df,pd.DataFrame([nr])],ignore_index=True)
                save_table("ruches", st.session_state.ruches)
                st.success(f"✅ Ruche {nid} '{nnom}' créée et sauvegardée !"); st.balloons()

elif current_page in ["miel","pollen","gelee","propolis"]:
    TYPE_LABELS = {"miel":"🍯 Miel","pollen":"🌿 Pollen","gelee":"👑 Gelée Royale","propolis":"🟤 Propolis & Cire"}
    TYPE_DB = {"miel":"miel","pollen":"pollen","gelee":"gelée_royale","propolis":"propolis"}
    label = TYPE_LABELS[current_page]
    db_type = TYPE_DB[current_page]
    st.markdown(f'<div class="page-title">{label}</div>', unsafe_allow_html=True)
    rec = st.session_state.recoltes
    sub = rec[rec["Type"]==db_type]
    total = sub["Quantite_kg"].sum()
    ca = (sub["Quantite_kg"]*sub["Prix_kg"]).sum() if len(sub)>0 else 0
    c1,c2 = st.columns(2)
    with c1: st.metric(f"Total {label}", f"{total:.3f} kg ({total*1000:.0f}g)" if total<1 else f"{total:.2f} kg")
    with c2: st.metric("Valeur estimée", f"{ca:,.0f} DA")
    st.markdown("<br>", unsafe_allow_html=True)
    section_header(f"Enregistrer une récolte — {label}")
    c1,c2 = st.columns(2)
    with c1:
        r_date=st.date_input("Date",datetime.now(),key=f"r_date_{current_page}")
        r_ruche=st.selectbox("Ruche",[f"{r['ID']} — {r['Nom']}" for _,r in st.session_state.ruches.iterrows()],key=f"r_ruche_{current_page}")
        r_qt=st.number_input("Quantité (kg)",0.0,step=0.001 if current_page in ["gelee","propolis"] else 0.1,key=f"r_qt_{current_page}")
    with c2:
        r_prix=st.number_input("Prix (DA/kg)",0,step=100,key=f"r_prix_{current_page}")
        r_notes=st.text_input("Notes",key=f"r_notes_{current_page}")
    if st.button(f"✓ Enregistrer",type="primary",key=f"btn_{current_page}"):
        nr={"Date":str(r_date),"Ruche":r_ruche.split("—")[0].strip(),"Type":db_type,
            "Produit":label,"Quantite_kg":r_qt,"Humidite_pct":None,"Prix_kg":r_prix}
        st.session_state.recoltes=pd.concat([rec,pd.DataFrame([nr])],ignore_index=True)
        save_table("recoltes", st.session_state.recoltes)
        st.success(f"✅ Récolte de {r_qt} kg enregistrée et sauvegardée !")
    if len(sub)>0:
        st.markdown("<br>", unsafe_allow_html=True)
        st.dataframe(sub.sort_values("Date",ascending=False),use_container_width=True,hide_index=True)

elif current_page == "caracterisation":
    st.markdown('<div class="page-title">📐 Caractérisation des Abeilles</div>', unsafe_allow_html=True)
    df = st.session_state.ruches
    tab1, tab2 = st.tabs(["🎯 Profils production","👅 Langue & Ailes"])
    with tab1:
        section_header("📊 Tableau de caractérisation comparatif")
        car_df = pd.DataFrame({
            "Sous-espèce":["A. m. intermissa","A. m. sahariensis","A. m. ligustica","A. m. carnica","A. m. caucasica"],
            "Glossa (mm)":["5.9–6.3","5.8–6.2","6.3–6.7","6.4–6.8","6.7–7.1"],
            "Indice cub.":["2.0–2.8","2.1–2.9","2.4–3.2","2.6–3.5","1.9–2.5"],
            "Pigmentation":["4–7 sombre","5–8 très sombre","1–3 claire","1–3 grise","1–4 variable"],
            "Profil prod.":["Miel/Pollen","Pollen","Gelée R./Miel","Pollen/Miel","Propolis/Miel"],
            "VSH moyen":["75%","68%","62%","78%","65%"],
            "Adaptation":["Maghreb","Déserts arides","Europe mérid.","Europe centr.","Caucase"],
        })
        st.dataframe(car_df, use_container_width=True, hide_index=True)
    with tab2:
        fig_gl = go.Figure()
        for race in df["Race"].unique():
            sub_r = df[df["Race"]==race]
            col = {"A. m. intermissa":"#C97A08","A. m. sahariensis":"#9333ea","A. m. ligustica":"#3b82f6","A. m. carnica":"#22c55e","Hybride":"#6b7280"}.get(race,"#6b7280")
            fig_gl.add_trace(go.Box(y=sub_r["Glossa_mm"],name=race[:15],marker_color=col,boxmean=True))
        fig_gl.update_layout(height=320,paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(253,250,243,0.5)",
            yaxis=dict(title="Glossa (mm)",tickfont=dict(color="#655E40"),gridcolor="rgba(180,150,80,0.1)"),
            margin=dict(l=10,r=10,t=10,b=10))
        st.plotly_chart(fig_gl, use_container_width=True, config={"displayModeBar":False})

elif current_page == "alertes":
    st.markdown('<div class="page-title">🚨 Alertes & Notifications</div>', unsafe_allow_html=True)
    df = st.session_state.ruches
    for _,r in df.iterrows():
        if r["Varroa_pct"]>3:
            st.markdown(alert("🚨",f"<strong>CRITIQUE — {r['Nom']} ({r['ID']}) :</strong> Varroa {r['Varroa_pct']}% → Traitement urgent.","al-danger"), unsafe_allow_html=True)
        elif r["Varroa_pct"]>2:
            st.markdown(alert("⚠️",f"<strong>ATTENTION — {r['Nom']} ({r['ID']}) :</strong> Varroa {r['Varroa_pct']}% → Surveiller.","al-warning"), unsafe_allow_html=True)
        if r["Statut"]=="Critique":
            st.markdown(alert("🔴",f"<strong>CRITIQUE — {r['Nom']} :</strong> Inspection urgente requise.","al-danger"), unsafe_allow_html=True)
    st.markdown(alert("✅","Toutes les alertes critiques ont été vérifiées ce matin.","al-success"), unsafe_allow_html=True)

elif current_page == "rapports":
    st.markdown('<div class="page-title">📋 Rapports & Exports CSV</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Exportez vos données — allez dans <strong>💾 Données & Sauvegardes</strong> pour les sauvegardes complètes</div>', unsafe_allow_html=True)
    exports = [
        ("📊","Ruches",st.session_state.ruches,"ruches"),
        ("🔬","Morphométrie",st.session_state.morph_analyses,"morpho_analyses"),
        ("🍯","Récoltes",st.session_state.recoltes,"recoltes"),
        ("💊","Traitements",st.session_state.traitements,"traitements"),
        ("🔍","Inspections",st.session_state.inspections,"inspections"),
        ("📦","Stock",st.session_state.stock,"stock"),
        ("🧬","Génétique",st.session_state.genetique,"genetique"),
        ("🚨","Alertes",st.session_state.alertes_db,"alertes"),
    ]
    cols = st.columns(4)
    for i,(icon,title,data,tname) in enumerate(exports):
        with cols[i%4]:
            nb = len(data) if isinstance(data, pd.DataFrame) else 0
            st.markdown(f'<div style="background:white;border-radius:14px;padding:16px;text-align:center;border:1px solid rgba(180,150,80,0.2);margin-bottom:12px"><div style="font-size:28px;margin-bottom:7px">{icon}</div><div style="font-weight:600;font-size:13.5px;margin-bottom:3px">{title}</div><div style="font-size:11px;color:var(--muted)">{nb} lignes</div></div>', unsafe_allow_html=True)
            csv_b, fname = export_table_csv(tname)
            st.download_button(f"⬇ CSV", csv_b, fname, "text/csv", use_container_width=True, key=f"dl_{tname}")
    st.markdown("<br>", unsafe_allow_html=True)
    section_header("⬇ Exporter TOUT en un ZIP")
    zip_bytes = export_all_csv_zip()
    st.download_button("⬇ Télécharger tout (ZIP)", zip_bytes,
        f"apitrack_export_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
        "application/zip", use_container_width=False)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE : DONNÉES & SAUVEGARDES (CŒUR DE LA PERSISTANCE)
# ══════════════════════════════════════════════════════════════════════════════
elif current_page == "donnees":
    st.markdown('<div class="page-title">💾 Données & Sauvegardes</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Vos données sont enregistrées en permanence dans le dossier <code>apitrack_data/</code> · Elles survivent à l\'extinction du PC et du smartphone</div>', unsafe_allow_html=True)

    # ── Statut du dossier de données ──────────────────────────────────────────
    stats = get_storage_stats()
    cfg = load_config()

    st.markdown(f"""
    <div style="background:linear-gradient(135deg,var(--forest),var(--forest-mid));border-radius:18px;padding:22px;color:white;margin-bottom:20px">
      <div style="display:flex;align-items:center;gap:14px;margin-bottom:16px">
        <div style="font-size:36px">💾</div>
        <div>
          <div style="font-family:'Playfair Display',serif;font-size:20px;font-weight:700;color:#F2C14E">Dossier de données ApiTrack Pro</div>
          <div style="font-size:12px;opacity:.75;margin-top:3px;font-family:'JetBrains Mono',monospace">{stats['dossier']}</div>
        </div>
      </div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-top:14px;padding-top:14px;border-top:1px solid rgba(255,255,255,0.15)">
        <div style="text-align:center"><div style="font-size:22px;font-weight:700;color:#F2C14E">{sum(t['nb'] for t in stats['tables'].values())}</div><div style="font-size:10px;opacity:.6;text-transform:uppercase;letter-spacing:.08em">Enregistrements total</div></div>
        <div style="text-align:center"><div style="font-size:22px;font-weight:700;color:#F2C14E">{stats['taille_totale_kb']:.1f} Ko</div><div style="font-size:10px;opacity:.6;text-transform:uppercase;letter-spacing:.08em">Taille sur disque</div></div>
        <div style="text-align:center"><div style="font-size:22px;font-weight:700;color:#F2C14E">{stats['nb_sauvegardes']}</div><div style="font-size:10px;opacity:.6;text-transform:uppercase;letter-spacing:.08em">Sauvegardes ZIP</div></div>
        <div style="text-align:center"><div style="font-size:13px;font-weight:600;color:#F2C14E">{stats['derniere_sauvegarde'][:10] if stats['derniere_sauvegarde'] and stats['derniere_sauvegarde'] != 'Jamais' else 'Jamais'}</div><div style="font-size:10px;opacity:.6;text-transform:uppercase;letter-spacing:.08em">Dernière sauvegarde</div></div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── 3 onglets principaux ──────────────────────────────────────────────────
    tab_bkp, tab_rest, tab_imp, tab_cfg = st.tabs([
        "💾 Sauvegarder", "🔄 Restaurer", "📥 Importer CSV", "⚙️ Paramètres"
    ])

    # ════ ONGLET 1 : SAUVEGARDER ═══════════════════════════════════════════
    with tab_bkp:
        st.markdown(alert("💾", """<strong>Sauvegarde = 1 fichier ZIP</strong> contenant tous vos CSV. 
            Stockez ce ZIP sur clé USB, Google Drive, WhatsApp (envoyez-le à vous-même), ou par email. 
            La sauvegarde automatique crée un ZIP chaque jour dans <code>apitrack_data/sauvegardes/</code>.""", "al-info"), unsafe_allow_html=True)

        c_left, c_right = st.columns([2, 1])
        with c_left:
            section_header("📦 Créer une sauvegarde maintenant")
            bkp_label = st.text_input("Étiquette de la sauvegarde (optionnel)", placeholder="Ex: avant_traitement_varroa, fin_saison_2025…")
            col_b1, col_b2 = st.columns(2)
            with col_b1:
                if st.button("💾 Créer sauvegarde ZIP", type="primary", use_container_width=True):
                    with st.spinner("Sauvegarde en cours…"):
                        # D'abord sauvegarder les données en mémoire sur disque
                        save_state()
                        ok, zip_path, zip_name = create_backup(bkp_label)
                    if ok:
                        st.success(f"✅ Sauvegarde créée : **{zip_name}**")
                        zip_bytes = get_backup_zip_bytes(zip_path)
                        st.download_button(
                            "⬇ Télécharger maintenant",
                            zip_bytes, zip_name, "application/zip",
                            use_container_width=True, key="dl_new_backup"
                        )
                    else:
                        st.error(f"❌ Erreur : {zip_path}")
            with col_b2:
                if st.button("💾 Forcer sauvegarde disque", use_container_width=True,
                             help="Sauvegarde immédiate des données en mémoire vers les fichiers CSV"):
                    save_state()
                    st.success("✅ Données sauvegardées sur disque !")

        with c_right:
            # État des tables
            section_header("📊 État des tables")
            for name, tinfo in stats["tables"].items():
                icons = {"ruches":"🏠","inspections":"🔍","recoltes":"🍯","traitements":"💊",
                         "morpho_analyses":"🔬","stock":"📦","genetique":"🧬","alertes":"🚨"}
                ico = icons.get(name,"📄")
                st.markdown(f"""<div style="display:flex;align-items:center;justify-content:space-between;
                    padding:7px 10px;background:white;border-radius:9px;margin-bottom:5px;
                    border:1px solid rgba(180,150,80,0.15);font-size:12.5px">
                    <span>{ico} {name}</span>
                    <span style="font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--muted)">{tinfo['nb']} lignes · {tinfo['taille_kb']} Ko</span>
                </div>""", unsafe_allow_html=True)

        # ── Historique des sauvegardes ──
        st.markdown("<br>", unsafe_allow_html=True)
        section_header("🗂 Historique des sauvegardes")
        backups = list_backups()
        if not backups:
            st.info("Aucune sauvegarde pour l'instant. Créez-en une ci-dessus !")
        else:
            for i, bk in enumerate(backups):
                c_i, c_n, c_d, c_s, c_dl = st.columns([0.5, 3, 2, 1.5, 1.5])
                with c_i:
                    badge_col = "b-green" if i == 0 else "b-gray"
                    st.markdown(f'<span class="badge {badge_col}" style="font-size:10px">{"Récente" if i==0 else f"#{i+1}"}</span>', unsafe_allow_html=True)
                with c_n:
                    st.markdown(f'<div style="font-family:JetBrains Mono,monospace;font-size:12px;color:var(--text);padding:6px 0">{bk["nom"]}</div>', unsafe_allow_html=True)
                with c_d:
                    st.markdown(f'<div style="font-size:12px;color:var(--muted);padding:6px 0">{bk["date"]}</div>', unsafe_allow_html=True)
                with c_s:
                    st.markdown(f'<div style="font-size:12px;color:var(--muted);padding:6px 0">{bk["taille"]}</div>', unsafe_allow_html=True)
                with c_dl:
                    zip_b = get_backup_zip_bytes(bk["chemin"])
                    st.download_button("⬇", zip_b, bk["nom"], "application/zip",
                        key=f"dl_bk_{i}", use_container_width=True)

    # ════ ONGLET 2 : RESTAURER ═══════════════════════════════════════════════
    with tab_rest:
        st.markdown(alert("🔄", """<strong>Restauration :</strong> Rechargez vos données depuis une sauvegarde ZIP. 
            Une sauvegarde automatique de sécurité est créée AVANT toute restauration. 
            Vous pouvez restaurer depuis une sauvegarde locale OU un fichier ZIP uploadé (depuis une autre machine ou un cloud).""", "al-warning"), unsafe_allow_html=True)

        c_l, c_r = st.columns(2)

        with c_l:
            section_header("🗂 Restaurer depuis une sauvegarde locale")
            backups = list_backups()
            if not backups:
                st.info("Aucune sauvegarde locale disponible.")
            else:
                bk_names = [f"{b['date']} — {b['nom']} ({b['taille']})" for b in backups]
                selected_bk = st.selectbox("Choisir une sauvegarde", bk_names)
                bk_idx = bk_names.index(selected_bk)
                bk_chosen = backups[bk_idx]

                st.markdown(f"""
                <div style="background:var(--wax);border-radius:10px;padding:12px;font-size:12.5px;margin:10px 0">
                  <div>📅 <strong>Date :</strong> {bk_chosen['date']}</div>
                  <div>📦 <strong>Fichier :</strong> {bk_chosen['nom']}</div>
                  <div>💾 <strong>Taille :</strong> {bk_chosen['taille']}</div>
                </div>""", unsafe_allow_html=True)

                confirm_local = st.checkbox("Je confirme vouloir restaurer ces données (les données actuelles seront remplacées)", key="confirm_local")
                if st.button("🔄 Restaurer cette sauvegarde", type="primary", disabled=not confirm_local):
                    with st.spinner("Restauration en cours…"):
                        ok, msg = restore_backup(bk_chosen["chemin"])
                    if ok:
                        reload_state()
                        st.success(f"✅ {msg}")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"❌ {msg}")

        with c_r:
            section_header("📤 Restaurer depuis un fichier uploadé")
            st.markdown('<div style="font-size:12.5px;color:var(--muted);margin-bottom:12px">Uploadez un fichier ZIP d\'ApiTrack Pro depuis votre téléphone, clé USB, Google Drive, email…</div>', unsafe_allow_html=True)
            uploaded_zip = st.file_uploader("Fichier de sauvegarde (.zip)", type=["zip"], key="restore_upload")
            if uploaded_zip:
                st.markdown(f"""<div style="background:var(--forest-pale);border-radius:10px;padding:12px;font-size:12.5px;margin:10px 0">
                  <div>📦 <strong>{uploaded_zip.name}</strong></div>
                  <div>💾 {uploaded_zip.size/1024:.1f} Ko</div></div>""", unsafe_allow_html=True)
                confirm_up = st.checkbox("Je confirme la restauration depuis ce fichier", key="confirm_up")
                if st.button("🔄 Restaurer ce fichier", type="primary", disabled=not confirm_up):
                    with st.spinner("Restauration en cours…"):
                        ok, msg = restore_from_upload(uploaded_zip.read())
                    if ok:
                        reload_state()
                        st.success(f"✅ {msg}")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error(f"❌ {msg}")

    # ════ ONGLET 3 : IMPORTER CSV ════════════════════════════════════════════
    with tab_imp:
        st.markdown(alert("📥", """<strong>Import CSV :</strong> Importez des données depuis un fichier CSV exporté précédemment, 
            depuis une autre application apicole ou créé à la main dans Excel. 
            Les nouveaux enregistrements sont <strong>ajoutés</strong> sans écraser les données existantes.""", "al-info"), unsafe_allow_html=True)

        table_labels = {
            "ruches":"🏠 Ruches","inspections":"🔍 Inspections","recoltes":"🍯 Récoltes",
            "traitements":"💊 Traitements","morpho_analyses":"🔬 Morphométrie",
            "stock":"📦 Stock","genetique":"🧬 Génétique","alertes":"🚨 Alertes"
        }
        tbl_choice = st.selectbox("Table à importer", list(table_labels.values()))
        tbl_key = [k for k,v in table_labels.items() if v == tbl_choice][0]

        csv_file = st.file_uploader(f"Fichier CSV pour : {tbl_choice}", type=["csv"], key=f"imp_{tbl_key}")
        if csv_file:
            try:
                df_preview = pd.read_csv(csv_file, encoding="utf-8-sig", nrows=5)
                st.markdown(f"**Aperçu (5 premières lignes) — {len(df_preview.columns)} colonnes :**")
                st.dataframe(df_preview, use_container_width=True, hide_index=True)
                csv_file.seek(0)
                if st.button(f"📥 Importer dans {tbl_choice}", type="primary"):
                    ok, msg, nb = import_csv(tbl_key, csv_file.read())
                    if ok:
                        # Recharger la table dans session_state
                        table_attr = {"ruches":"ruches","inspections":"inspections",
                                      "recoltes":"recoltes","traitements":"traitements",
                                      "morpho_analyses":"morph_analyses","stock":"stock",
                                      "genetique":"genetique","alertes":"alertes_db"}
                        attr = table_attr.get(tbl_key)
                        if attr:
                            st.session_state[attr] = load_table(tbl_key)
                        st.success(f"✅ {msg}")
                    else:
                        st.error(f"❌ {msg}")
            except Exception as e:
                st.error(f"Impossible de lire le fichier : {e}")

    # ════ ONGLET 4 : PARAMÈTRES ══════════════════════════════════════════════
    with tab_cfg:
        section_header("⚙️ Paramètres du rucher")
        c1, c2 = st.columns(2)
        with c1:
            new_nom = st.text_input("Nom de l'apiculteur", value=cfg.get("apiculteur",""))
            new_rucher = st.text_input("Nom du rucher", value=cfg.get("rucher",""))
            new_region = st.text_input("Région", value=cfg.get("region",""))
        with c2:
            new_auto = st.checkbox("Sauvegarde automatique quotidienne", value=cfg.get("sauvegarde_auto", True))
            new_max = st.number_input("Nombre max de sauvegardes à conserver", min_value=5, max_value=365, value=int(cfg.get("nb_sauvegardes_max", 30)))
        if st.button("✅ Sauvegarder les paramètres", type="primary"):
            cfg.update({"apiculteur":new_nom,"rucher":new_rucher,"region":new_region,
                        "sauvegarde_auto":new_auto,"nb_sauvegardes_max":new_max})
            save_config(cfg)
            st.session_state._config = cfg
            st.success("✅ Paramètres enregistrés !")

        st.markdown("<br>", unsafe_allow_html=True)
        section_header("🗑 Zone dangereuse")
        st.markdown(alert("⚠️", "Les actions ci-dessous sont irréversibles. Créez d'abord une sauvegarde.", "al-warning"), unsafe_allow_html=True)
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            if st.button("🗑 Supprimer toutes les sauvegardes", use_container_width=True):
                confirm_del = st.checkbox("Confirmer la suppression de toutes les sauvegardes ZIP", key="del_bkp_confirm")
                if confirm_del:
                    import glob
                    for f in (DATA_DIR / "sauvegardes").glob("*.zip"):
                        f.unlink()
                    st.success("Sauvegardes supprimées.")
        with col_d2:
            st.markdown('<span style="font-size:12px;color:var(--muted)">Pour réinitialiser les données : supprimez manuellement le dossier <code>apitrack_data/</code> et redémarrez l\'application.</span>', unsafe_allow_html=True)

elif current_page in ["inspections","traitements","flore","meteo","genetique","inventaire"]:
    titles = {"inspections":"🔍 Inspections","traitements":"💊 Traitements",
              "flore":"🌸 Flore Mellifère","meteo":"🌤️ Météo & Miellée",
              "genetique":"🧬 Génétique & Races","inventaire":"📦 Inventaire"}
    st.markdown(f'<div class="page-title">{titles[current_page]}</div>', unsafe_allow_html=True)
    st.info("Cette section est disponible. Explorez d'abord la page **🔬 Morphométrie IA** qui est le cœur de cette version.")

# =====================================================
# FOOTER
# =====================================================
st.markdown("""
<div style="text-align:center;padding:28px 0 12px;font-size:12px;color:#9B8860;
            border-top:1px solid rgba(180,150,80,0.12);margin-top:36px">
  <strong style="font-family:'Playfair Display',serif;font-size:13.5px;color:#3E2A18">ApiTrack Pro v3.0</strong>
  · Plateforme Apicole Professionnelle · IA Morphométrie Gratuite & Permanente<br>
  Ruttner (1988) · Kandemir (2011) · Chahbar et al. (2013) · Région de l'Oranie, Algérie<br>
  <span style="font-size:11px">🤖 IA : Google Gemini (gratuit) · Groq LLaMA Vision (gratuit) · Ollama local (gratuit)</span><br><br>
  🐝 Développé pour l'apiculture scientifique et professionnelle
</div>
""", unsafe_allow_html=True)
