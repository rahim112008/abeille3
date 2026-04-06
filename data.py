#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ApiTrack Pro — Gestionnaire de données persistantes
Toutes les données sont sauvegardées dans le dossier 'apitrack_data/'
Survit à l'extinction du PC/smartphone.

Structure du dossier :
  apitrack_data/
  ├── ruches.csv
  ├── inspections.csv
  ├── recoltes.csv
  ├── traitements.csv
  ├── morpho_analyses.csv
  ├── stock.csv
  ├── genetique.csv
  ├── alertes.csv
  ├── config.json
  └── sauvegardes/
      ├── backup_2025-04-06_14h30.zip
      ├── backup_2025-04-05_09h00.zip
      └── ...
"""

import os, json, shutil, zipfile, hashlib
import pandas as pd
from datetime import datetime
from pathlib import Path

# ─── Dossier principal de données ──────────────────────────────────────────────
DATA_DIR    = Path("apitrack_data")
BACKUP_DIR  = DATA_DIR / "sauvegardes"
CONFIG_FILE = DATA_DIR / "config.json"

# ─── Schémas des tables (colonnes + valeurs par défaut) ────────────────────────
SCHEMAS = {
    "ruches": {
        "file": "ruches.csv",
        "cols": ["ID","Nom","Race","Site","Type_ruche","Poids_kg","Varroa_pct",
                 "Miel_kg","Pollen_kg","Gelée_g","Propolis_kg","Cire_kg",
                 "Statut","Reine_id","VSH_pct","Douceur","Economie_hiv",
                 "Essaimage_pct","Date_creation","Profil_prod",
                 "Glossa_mm","L_aile_mm","Ri","Tomentum_pct","Pigment_scutellum","Ti_L_mm",
                 "Notes","Date_modif"],
        "demo": [
            {"ID":"A-03","Nom":"Reine Dorée","Race":"A. m. intermissa","Site":"Verger du Cèdre",
             "Type_ruche":"Dadant 10 cadres","Poids_kg":22.3,"Varroa_pct":1.2,
             "Miel_kg":18.5,"Pollen_kg":2.8,"Gelée_g":145,"Propolis_kg":0.05,"Cire_kg":0.3,
             "Statut":"Excellent","Reine_id":"R-2024-01","VSH_pct":78,"Douceur":88,
             "Economie_hiv":82,"Essaimage_pct":25,"Date_creation":"2022-03-15","Profil_prod":"Miel",
             "Glossa_mm":6.12,"L_aile_mm":9.18,"Ri":2.45,"Tomentum_pct":37,"Pigment_scutellum":5,"Ti_L_mm":3.01,
             "Notes":"Colonie phare du rucher","Date_modif":datetime.now().strftime("%Y-%m-%d")},
            {"ID":"B-07","Nom":"Bergère","Race":"A. m. sahariensis","Site":"Colline des Oliviers",
             "Type_ruche":"Dadant 10 cadres","Poids_kg":16.8,"Varroa_pct":3.8,
             "Miel_kg":11.2,"Pollen_kg":4.5,"Gelée_g":62,"Propolis_kg":0.08,"Cire_kg":0.2,
             "Statut":"Critique","Reine_id":"R-2024-07","VSH_pct":52,"Douceur":60,
             "Economie_hiv":65,"Essaimage_pct":55,"Date_creation":"2022-06-10","Profil_prod":"Pollen",
             "Glossa_mm":5.98,"L_aile_mm":8.92,"Ri":2.22,"Tomentum_pct":28,"Pigment_scutellum":6,"Ti_L_mm":2.85,
             "Notes":"Traitement varroa en cours","Date_modif":datetime.now().strftime("%Y-%m-%d")},
            {"ID":"C-12","Nom":"Atlas","Race":"A. m. intermissa","Site":"Verger du Cèdre",
             "Type_ruche":"Langstroth","Poids_kg":20.1,"Varroa_pct":0.8,
             "Miel_kg":15.0,"Pollen_kg":3.1,"Gelée_g":198,"Propolis_kg":0.06,"Cire_kg":0.25,
             "Statut":"Excellent","Reine_id":"R-2023-14","VSH_pct":81,"Douceur":92,
             "Economie_hiv":88,"Essaimage_pct":18,"Date_creation":"2021-04-20","Profil_prod":"Gelée Royale",
             "Glossa_mm":6.22,"L_aile_mm":9.41,"Ri":2.61,"Tomentum_pct":41,"Pigment_scutellum":4,"Ti_L_mm":3.18,
             "Notes":"Excellente productrice gelée","Date_modif":datetime.now().strftime("%Y-%m-%d")},
            {"ID":"D-02","Nom":"Soleil d'Or","Race":"Hybride","Site":"Plaine des Fleurs",
             "Type_ruche":"Dadant 10 cadres","Poids_kg":25.4,"Varroa_pct":1.5,
             "Miel_kg":16.3,"Pollen_kg":2.2,"Gelée_g":88,"Propolis_kg":0.04,"Cire_kg":0.35,
             "Statut":"Bon","Reine_id":"R-2024-03","VSH_pct":67,"Douceur":75,
             "Economie_hiv":72,"Essaimage_pct":38,"Date_creation":"2023-02-28","Profil_prod":"Miel",
             "Glossa_mm":6.48,"L_aile_mm":9.52,"Ri":2.91,"Tomentum_pct":48,"Pigment_scutellum":2,"Ti_L_mm":3.24,
             "Notes":"Essaimage prévu","Date_modif":datetime.now().strftime("%Y-%m-%d")},
            {"ID":"A-08","Nom":"Jasmine","Race":"A. m. intermissa","Site":"Plaine des Fleurs",
             "Type_ruche":"Dadant 10 cadres","Poids_kg":18.2,"Varroa_pct":2.1,
             "Miel_kg":12.8,"Pollen_kg":3.8,"Gelée_g":112,"Propolis_kg":0.03,"Cire_kg":0.18,
             "Statut":"Attention","Reine_id":"R-2024-05","VSH_pct":71,"Douceur":83,
             "Economie_hiv":76,"Essaimage_pct":32,"Date_creation":"2023-05-12","Profil_prod":"Pollen",
             "Glossa_mm":6.05,"L_aile_mm":9.24,"Ri":2.51,"Tomentum_pct":34,"Pigment_scutellum":5,"Ti_L_mm":3.04,
             "Notes":"Surveiller varroa","Date_modif":datetime.now().strftime("%Y-%m-%d")},
            {"ID":"C-05","Nom":"Nuit Étoilée","Race":"A. m. intermissa","Site":"Verger du Cèdre",
             "Type_ruche":"Warré","Poids_kg":14.5,"Varroa_pct":0.5,
             "Miel_kg":10.5,"Pollen_kg":2.1,"Gelée_g":76,"Propolis_kg":0.07,"Cire_kg":0.15,
             "Statut":"Attention","Reine_id":"R-2023-22","VSH_pct":83,"Douceur":90,
             "Economie_hiv":85,"Essaimage_pct":15,"Date_creation":"2021-07-08","Profil_prod":"Résistance",
             "Glossa_mm":6.08,"L_aile_mm":9.31,"Ri":2.48,"Tomentum_pct":39,"Pigment_scutellum":5,"Ti_L_mm":3.09,
             "Notes":"Bonne résistance VSH","Date_modif":datetime.now().strftime("%Y-%m-%d")},
            {"ID":"B-11","Nom":"Montagne Bleue","Race":"A. m. carnica","Site":"Colline des Oliviers",
             "Type_ruche":"Dadant 10 cadres","Poids_kg":19.8,"Varroa_pct":1.0,
             "Miel_kg":14.2,"Pollen_kg":2.5,"Gelée_g":165,"Propolis_kg":0.05,"Cire_kg":0.22,
             "Statut":"Excellent","Reine_id":"R-2023-08","VSH_pct":76,"Douceur":95,
             "Economie_hiv":90,"Essaimage_pct":22,"Date_creation":"2022-09-01","Profil_prod":"Gelée Royale",
             "Glossa_mm":6.55,"L_aile_mm":9.48,"Ri":2.98,"Tomentum_pct":43,"Pigment_scutellum":2,"Ti_L_mm":3.21,
             "Notes":"Race Carnica, très douce","Date_modif":datetime.now().strftime("%Y-%m-%d")},
            {"ID":"D-09","Nom":"Zephyr","Race":"A. m. ligustica","Site":"Plaine des Fleurs",
             "Type_ruche":"Dadant 10 cadres","Poids_kg":21.5,"Varroa_pct":1.3,
             "Miel_kg":17.1,"Pollen_kg":2.0,"Gelée_g":95,"Propolis_kg":0.04,"Cire_kg":0.28,
             "Statut":"Bon","Reine_id":"R-2024-09","VSH_pct":63,"Douceur":88,
             "Economie_hiv":70,"Essaimage_pct":42,"Date_creation":"2023-08-15","Profil_prod":"Miel",
             "Glossa_mm":6.52,"L_aile_mm":9.61,"Ri":2.85,"Tomentum_pct":52,"Pigment_scutellum":1,"Ti_L_mm":3.28,
             "Notes":"Race Italienne importée","Date_modif":datetime.now().strftime("%Y-%m-%d")},
        ]
    },
    "inspections": {
        "file": "inspections.csv",
        "cols": ["ID","Date","Ruche","Poids_kg","Temperature_int","Cadres_couverts","Cadres_couvain",
                 "Varroa","Reine","Reserves","Comportement","Maladie_signes","Statut_general","Notes","Analyste"],
        "demo": [
            {"ID":"INS-001","Date":"2025-03-25","Ruche":"A-03","Poids_kg":22.3,"Temperature_int":35.1,
             "Cadres_couverts":8,"Cadres_couvain":6,"Varroa":"Faible (<1%)","Reine":"Observée",
             "Reserves":"Excellentes (>15 kg)","Comportement":"Calme","Maladie_signes":"Aucun",
             "Statut_general":"Excellent","Notes":"Colonie forte, ponte active","Analyste":"Mohammed A."},
            {"ID":"INS-002","Date":"2025-03-20","Ruche":"B-07","Poids_kg":16.8,"Temperature_int":34.6,
             "Cadres_couverts":5,"Cadres_couvain":3,"Varroa":"Élevée (>3%)","Reine":"Observée",
             "Reserves":"Faibles (3-8 kg)","Comportement":"Nerveux","Maladie_signes":"Varroa visible",
             "Statut_general":"Critique","Notes":"Traitement acide oxalique initié","Analyste":"Mohammed A."},
            {"ID":"INS-003","Date":"2025-03-15","Ruche":"C-12","Poids_kg":20.1,"Temperature_int":35.3,
             "Cadres_couverts":9,"Cadres_couvain":7,"Varroa":"Aucune","Reine":"Observée",
             "Reserves":"Excellentes (>15 kg)","Comportement":"Calme","Maladie_signes":"Aucun",
             "Statut_general":"Excellent","Notes":"Récolte gelée royale programmée","Analyste":"Mohammed A."},
        ]
    },
    "recoltes": {
        "file": "recoltes.csv",
        "cols": ["ID","Date","Ruche","Type","Produit","Quantite_kg","Humidite_pct",
                 "Couleur","Qualite_score","Prix_kg","Certification","Notes","Analyste"],
        "demo": [
            {"ID":"REC-001","Date":"2024-06-15","Ruche":"A-03","Type":"miel","Produit":"Miel de jujubier",
             "Quantite_kg":8.5,"Humidite_pct":17.2,"Couleur":"Amber","Qualite_score":92,"Prix_kg":18.0,
             "Certification":"Standard","Notes":"Excellente récolte","Analyste":"Mohammed A."},
            {"ID":"REC-002","Date":"2024-06-15","Ruche":"B-07","Type":"pollen","Produit":"Pollen de romarin",
             "Quantite_kg":1.8,"Humidite_pct":8.5,"Couleur":"Jaune doré","Qualite_score":88,"Prix_kg":45.0,
             "Certification":"Standard","Notes":"Bien séché","Analyste":"Mohammed A."},
            {"ID":"REC-003","Date":"2024-06-15","Ruche":"C-12","Type":"gelée_royale","Produit":"Gelée royale fraîche",
             "Quantite_kg":0.145,"Humidite_pct":66.0,"Couleur":"Blanc crémeux","Qualite_score":95,"Prix_kg":1200.0,
             "Certification":"Standard","Notes":"10-HDA : 1.85%","Analyste":"Mohammed A."},
            {"ID":"REC-004","Date":"2024-07-01","Ruche":"D-02","Type":"miel","Produit":"Miel d'eucalyptus",
             "Quantite_kg":16.3,"Humidite_pct":16.9,"Couleur":"Light Amber","Qualite_score":89,"Prix_kg":17.0,
             "Certification":"Standard","Notes":"","Analyste":"Mohammed A."},
            {"ID":"REC-005","Date":"2024-05-20","Ruche":"A-08","Type":"pollen","Produit":"Pollen mixte",
             "Quantite_kg":2.1,"Humidite_pct":8.1,"Couleur":"Multicolore","Qualite_score":85,"Prix_kg":45.0,
             "Certification":"Standard","Notes":"","Analyste":"Mohammed A."},
            {"ID":"REC-006","Date":"2024-08-10","Ruche":"C-05","Type":"propolis","Produit":"Propolis brute",
             "Quantite_kg":0.08,"Humidite_pct":None,"Couleur":"Brun foncé","Qualite_score":82,"Prix_kg":30.0,
             "Certification":"Standard","Notes":"","Analyste":"Mohammed A."},
        ]
    },
    "traitements": {
        "file": "traitements.csv",
        "cols": ["ID","Date_debut","Date_fin","Ruche","Produit","Pathologie","Dose","Methode",
                 "Duree_j","Statut","Progression_pct","Temperature_app","Notes","Analyste"],
        "demo": [
            {"ID":"TRT-001","Date_debut":"2025-03-20","Date_fin":"2025-04-10","Ruche":"B-07",
             "Produit":"Acide oxalique","Pathologie":"Varroa destructor","Dose":"5ml/ruche",
             "Methode":"Sublimation","Duree_j":21,"Statut":"En cours","Progression_pct":52,
             "Temperature_app":18,"Notes":"3ème application","Analyste":"Mohammed A."},
            {"ID":"TRT-002","Date_debut":"2025-03-10","Date_fin":"2025-03-31","Ruche":"A-03",
             "Produit":"Apiguard (thymol)","Pathologie":"Varroa destructor","Dose":"1 plateau",
             "Methode":"Lanière","Duree_j":21,"Statut":"Terminé","Progression_pct":100,
             "Temperature_app":20,"Notes":"Résultat : 92% efficacité","Analyste":"Mohammed A."},
        ]
    },
    "morpho_analyses": {
        "file": "morpho_analyses.csv",
        "cols": ["ID","Date","Ruche","Taxon","Confiance_pct","L_aile_mm","B_aile_mm","Ri",
                 "DI3_mm","OI","A4_deg","B4_deg","Ti_L_mm","Ba_L_mm","T3_L_mm","T4_W_pct",
                 "Glossa_mm","Wt_mm","Pigment","Hb_mm","Integrite_ailes","Nervation",
                 "Profil_prod","Score_miel","Score_pollen","Score_gelee","Score_propolis",
                 "Langue_classe","VSH_estime","Comportement_hygienique",
                 "Etat_sanitaire","Anomalies","Source_IA","Analyste","Notes","Image_hash"],
        "demo": [
            {"ID":"MOR-001","Date":"2025-02-14","Ruche":"A-03","Taxon":"A. m. intermissa","Confiance_pct":92,
             "L_aile_mm":9.18,"B_aile_mm":3.21,"Ri":2.45,"DI3_mm":1.72,"OI":"−","A4_deg":99.2,"B4_deg":91.5,
             "Ti_L_mm":3.01,"Ba_L_mm":1.88,"T3_L_mm":4.78,"T4_W_pct":37,"Glossa_mm":6.12,"Wt_mm":4.12,
             "Pigment":5,"Hb_mm":0.38,"Integrite_ailes":"intactes","Nervation":"normale",
             "Profil_prod":"miel","Score_miel":78,"Score_pollen":62,"Score_gelee":32,"Score_propolis":45,
             "Langue_classe":"moyenne","VSH_estime":70,"Comportement_hygienique":"moyen",
             "Etat_sanitaire":"sain","Anomalies":"","Source_IA":"Démonstration",
             "Analyste":"Mohammed A.","Notes":"Analyse initiale","Image_hash":""},
        ]
    },
    "stock": {
        "file": "stock.csv",
        "cols": ["ID","Article","Categorie","Quantite","Unite","Seuil_alerte","Prix_unitaire",
                 "Fournisseur","Date_achat","Localisation","Notes"],
        "demo": [
            {"ID":"STK-001","Article":"Cadres Dadant","Categorie":"Matériel","Quantite":145,"Unite":"pièces",
             "Seuil_alerte":50,"Prix_unitaire":2.5,"Fournisseur":"Api-France","Date_achat":"2024-01-15",
             "Localisation":"Hangar A","Notes":""},
            {"ID":"STK-002","Article":"Cire gaufrée","Categorie":"Consommable","Quantite":1.2,"Unite":"kg",
             "Seuil_alerte":5.0,"Prix_unitaire":12.0,"Fournisseur":"Local","Date_achat":"2024-02-01",
             "Localisation":"Hangar A","Notes":"Stock critique"},
            {"ID":"STK-003","Article":"Acide oxalique","Categorie":"Traitement","Quantite":350,"Unite":"g",
             "Seuil_alerte":500,"Prix_unitaire":0.02,"Fournisseur":"Vétérinaire","Date_achat":"2025-01-10",
             "Localisation":"Armoire traitements","Notes":""},
            {"ID":"STK-004","Article":"Apiguard","Categorie":"Traitement","Quantite":6,"Unite":"boîtes",
             "Seuil_alerte":4,"Prix_unitaire":8.5,"Fournisseur":"Vétérinaire","Date_achat":"2025-02-05",
             "Localisation":"Armoire traitements","Notes":""},
            {"ID":"STK-005","Article":"Pots 500g","Categorie":"Conditionnement","Quantite":280,"Unite":"pièces",
             "Seuil_alerte":200,"Prix_unitaire":0.45,"Fournisseur":"Emballage DZ","Date_achat":"2024-11-20",
             "Localisation":"Hangar B","Notes":""},
            {"ID":"STK-006","Article":"Trappe à pollen","Categorie":"Matériel","Quantite":3,"Unite":"pièces",
             "Seuil_alerte":2,"Prix_unitaire":15.0,"Fournisseur":"Local","Date_achat":"2023-04-01",
             "Localisation":"Hangar A","Notes":""},
            {"ID":"STK-007","Article":"Kit gelée royale","Categorie":"Matériel","Quantite":1,"Unite":"kit",
             "Seuil_alerte":1,"Prix_unitaire":120.0,"Fournisseur":"Apiculture Pro","Date_achat":"2024-03-15",
             "Localisation":"Hangar A","Notes":""},
            {"ID":"STK-008","Article":"Combinaison + voile","Categorie":"EPI","Quantite":2,"Unite":"pièces",
             "Seuil_alerte":1,"Prix_unitaire":45.0,"Fournisseur":"Local","Date_achat":"2023-01-01",
             "Localisation":"Hangar A","Notes":""},
        ]
    },
    "genetique": {
        "file": "genetique.csv",
        "cols": ["ID_Reine","Ruche","Race","Date_naissance","Origine","Ligne_genetique",
                 "VSH_pct","Douceur","Productivite_miel","Production_pollen","Production_gelee",
                 "Economie_hiv","Anti_essaimage","Statut_reine","Notes"],
        "demo": [
            {"ID_Reine":"R-2024-01","Ruche":"A-03","Race":"A. m. intermissa","Date_naissance":"2024-05-15",
             "Origine":"Sélection massale","Ligne_genetique":"Ligne 3","VSH_pct":78,"Douceur":88,
             "Productivite_miel":85,"Production_pollen":65,"Production_gelee":35,
             "Economie_hiv":82,"Anti_essaimage":75,"Statut_reine":"Active","Notes":"Meilleure de la saison"},
            {"ID_Reine":"R-2024-07","Ruche":"B-07","Race":"Hybride","Date_naissance":"2024-06-20",
             "Origine":"Inconnue","Ligne_genetique":"—","VSH_pct":52,"Douceur":60,
             "Productivite_miel":60,"Production_pollen":75,"Production_gelee":25,
             "Economie_hiv":65,"Anti_essaimage":45,"Statut_reine":"Active","Notes":"Renouvellement recommandé"},
            {"ID_Reine":"R-2023-14","Ruche":"C-12","Race":"A. m. intermissa","Date_naissance":"2023-06-10",
             "Origine":"Sélection massale","Ligne_genetique":"Ligne 1","VSH_pct":81,"Douceur":92,
             "Productivite_miel":78,"Production_pollen":68,"Production_gelee":82,
             "Economie_hiv":88,"Anti_essaimage":82,"Statut_reine":"Active","Notes":"Excellente productrice gelée"},
        ]
    },
    "alertes": {
        "file": "alertes.csv",
        "cols": ["ID","Date","Type","Ruche","Titre","Message","Priorite","Statut","Date_resolution","Notes"],
        "demo": [
            {"ID":"ALR-001","Date":"2025-04-01","Type":"critique","Ruche":"B-07",
             "Titre":"Varroa critique","Message":"Taux varroa 3.8% — traitement urgent","Priorite":1,
             "Statut":"active","Date_resolution":"","Notes":""},
            {"ID":"ALR-002","Date":"2025-04-01","Type":"avertissement","Ruche":"A-08",
             "Titre":"Varroa en hausse","Message":"Taux varroa 2.1% — surveillance renforcée","Priorite":2,
             "Statut":"active","Date_resolution":"","Notes":""},
            {"ID":"ALR-003","Date":"2025-03-28","Type":"stock","Ruche":"",
             "Titre":"Stock bas — Cire gaufrée","Message":"1.2kg restant, seuil 5kg","Priorite":2,
             "Statut":"active","Date_resolution":"","Notes":""},
        ]
    },
}

# ══════════════════════════════════════════════════════════════════════════════
# INITIALISATION DU DOSSIER DE DONNÉES
# ══════════════════════════════════════════════════════════════════════════════

def init_data_dir():
    """Crée le dossier de données s'il n'existe pas."""
    DATA_DIR.mkdir(exist_ok=True)
    BACKUP_DIR.mkdir(exist_ok=True)
    if not CONFIG_FILE.exists():
        config = {
            "version": "3.0",
            "apiculteur": "Mohammed A.",
            "rucher": "Rucher de l'Oranie",
            "region": "Tlemcen, Algérie",
            "sauvegarde_auto": True,
            "sauvegarde_frequence": "quotidienne",
            "nb_sauvegardes_max": 30,
            "date_creation": datetime.now().isoformat(),
            "derniere_sauvegarde": "",
            "derniere_modif": datetime.now().isoformat(),
        }
        save_config(config)
    return True

def save_config(config: dict):
    """Sauvegarde la configuration."""
    DATA_DIR.mkdir(exist_ok=True)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

def load_config() -> dict:
    """Charge la configuration."""
    if not CONFIG_FILE.exists():
        init_data_dir()
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

# ══════════════════════════════════════════════════════════════════════════════
# CHARGEMENT DES DONNÉES (depuis CSV → DataFrame)
# ══════════════════════════════════════════════════════════════════════════════

def load_table(table_name: str) -> pd.DataFrame:
    """
    Charge une table depuis le fichier CSV.
    Si le fichier n'existe pas → crée le dossier + fichier avec données démo.
    """
    init_data_dir()
    schema = SCHEMAS.get(table_name)
    if not schema:
        return pd.DataFrame()

    filepath = DATA_DIR / schema["file"]

    if filepath.exists():
        try:
            df = pd.read_csv(filepath, encoding="utf-8-sig")
            # Ajouter les colonnes manquantes
            for col in schema["cols"]:
                if col not in df.columns:
                    df[col] = ""
            return df[schema["cols"]]
        except Exception as e:
            print(f"⚠️ Erreur lecture {filepath}: {e}")

    # Créer le fichier avec données démo
    df = pd.DataFrame(schema["demo"])
    for col in schema["cols"]:
        if col not in df.columns:
            df[col] = ""
    df = df[schema["cols"]]
    df.to_csv(filepath, index=False, encoding="utf-8-sig")
    return df

def load_all() -> dict:
    """Charge toutes les tables. Retourne un dict {nom: DataFrame}."""
    init_data_dir()
    return {name: load_table(name) for name in SCHEMAS}

# ══════════════════════════════════════════════════════════════════════════════
# SAUVEGARDE DES DONNÉES (DataFrame → CSV)
# ══════════════════════════════════════════════════════════════════════════════

def save_table(table_name: str, df: pd.DataFrame) -> bool:
    """
    Sauvegarde un DataFrame dans son fichier CSV.
    Retourne True si succès.
    """
    init_data_dir()
    schema = SCHEMAS.get(table_name)
    if not schema:
        return False
    try:
        filepath = DATA_DIR / schema["file"]
        # Garder seulement les colonnes connues + colonnes supplémentaires
        cols_to_save = [c for c in schema["cols"] if c in df.columns]
        extra_cols = [c for c in df.columns if c not in schema["cols"]]
        df[cols_to_save + extra_cols].to_csv(filepath, index=False, encoding="utf-8-sig")
        # Mettre à jour la date de modification dans config
        try:
            cfg = load_config()
            cfg["derniere_modif"] = datetime.now().isoformat()
            save_config(cfg)
        except:
            pass
        return True
    except Exception as e:
        print(f"❌ Erreur sauvegarde {table_name}: {e}")
        return False

def save_all(data: dict) -> dict:
    """
    Sauvegarde toutes les tables.
    data = {"ruches": df, "recoltes": df, ...}
    Retourne {"ruches": True, "recoltes": True, ...}
    """
    results = {}
    for name, df in data.items():
        results[name] = save_table(name, df)
    return results

# ══════════════════════════════════════════════════════════════════════════════
# AJOUT / MODIFICATION / SUPPRESSION D'ENREGISTREMENTS
# ══════════════════════════════════════════════════════════════════════════════

def add_record(table_name: str, record: dict, df: pd.DataFrame) -> pd.DataFrame:
    """Ajoute un enregistrement et sauvegarde immédiatement."""
    record["Date_modif"] = datetime.now().strftime("%Y-%m-%d")
    new_row = pd.DataFrame([record])
    df = pd.concat([df, new_row], ignore_index=True)
    save_table(table_name, df)
    return df

def update_record(table_name: str, df: pd.DataFrame, id_col: str, id_val: str, updates: dict) -> pd.DataFrame:
    """Met à jour un enregistrement existant et sauvegarde."""
    mask = df[id_col] == id_val
    for col, val in updates.items():
        if col in df.columns:
            df.loc[mask, col] = val
    df.loc[mask, "Date_modif"] = datetime.now().strftime("%Y-%m-%d")
    save_table(table_name, df)
    return df

def delete_record(table_name: str, df: pd.DataFrame, id_col: str, id_val: str) -> pd.DataFrame:
    """Supprime un enregistrement et sauvegarde."""
    df = df[df[id_col] != id_val].reset_index(drop=True)
    save_table(table_name, df)
    return df

def generate_id(prefix: str, df: pd.DataFrame, id_col: str = "ID") -> str:
    """Génère un ID unique basé sur le préfixe et le nombre d'enregistrements."""
    if id_col not in df.columns or len(df) == 0:
        return f"{prefix}-001"
    existing = df[id_col].astype(str).tolist()
    n = len(existing) + 1
    new_id = f"{prefix}-{n:03d}"
    while new_id in existing:
        n += 1
        new_id = f"{prefix}-{n:03d}"
    return new_id

# ══════════════════════════════════════════════════════════════════════════════
# SYSTÈME DE SAUVEGARDE (ZIP)
# ══════════════════════════════════════════════════════════════════════════════

def create_backup(label: str = "") -> tuple[bool, str, str]:
    """
    Crée une sauvegarde ZIP de toutes les données.
    Retourne (succès, chemin_fichier, nom_fichier)
    """
    init_data_dir()
    ts = datetime.now().strftime("%Y-%m-%d_%Hh%M")
    label_clean = f"_{label.replace(' ','_')}" if label else ""
    zip_name = f"backup{label_clean}_{ts}.zip"
    zip_path = BACKUP_DIR / zip_name

    try:
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for schema_name, schema in SCHEMAS.items():
                csv_path = DATA_DIR / schema["file"]
                if csv_path.exists():
                    zf.write(csv_path, schema["file"])
            if CONFIG_FILE.exists():
                zf.write(CONFIG_FILE, "config.json")
            # Fichier de métadonnées
            meta = {
                "date_sauvegarde": datetime.now().isoformat(),
                "label": label,
                "version": "3.0",
                "nb_fichiers": len(list(DATA_DIR.glob("*.csv"))),
            }
            zf.writestr("sauvegarde_info.json", json.dumps(meta, ensure_ascii=False, indent=2))

        # Mettre à jour config
        cfg = load_config()
        cfg["derniere_sauvegarde"] = datetime.now().isoformat()
        save_config(cfg)

        # Nettoyer les anciennes sauvegardes
        _cleanup_old_backups(cfg.get("nb_sauvegardes_max", 30))

        return True, str(zip_path), zip_name
    except Exception as e:
        return False, "", str(e)

def list_backups() -> list[dict]:
    """Liste toutes les sauvegardes disponibles, triées par date (plus récent d'abord)."""
    if not BACKUP_DIR.exists():
        return []
    backups = []
    for f in sorted(BACKUP_DIR.glob("backup*.zip"), reverse=True):
        stat = f.stat()
        size_kb = stat.st_size / 1024
        backups.append({
            "nom": f.name,
            "chemin": str(f),
            "date": datetime.fromtimestamp(stat.st_mtime).strftime("%d/%m/%Y %H:%M"),
            "taille": f"{size_kb:.1f} Ko",
            "taille_bytes": stat.st_size,
        })
    return backups

def restore_backup(zip_path: str) -> tuple[bool, str]:
    """
    Restaure les données depuis une sauvegarde ZIP.
    Crée d'abord une sauvegarde automatique de sécurité.
    Retourne (succès, message)
    """
    if not os.path.exists(zip_path):
        return False, f"Fichier introuvable : {zip_path}"
    try:
        # Sauvegarde de sécurité avant restauration
        create_backup("avant_restauration")
        # Restauration
        with zipfile.ZipFile(zip_path, "r") as zf:
            for file_info in zf.infolist():
                if file_info.filename.endswith(".csv"):
                    zf.extract(file_info, DATA_DIR)
                elif file_info.filename == "config.json":
                    zf.extract(file_info, DATA_DIR)
        return True, "Restauration réussie ! Les données ont été rechargées."
    except Exception as e:
        return False, f"Erreur de restauration : {str(e)}"

def restore_from_upload(zip_bytes: bytes) -> tuple[bool, str]:
    """Restaure depuis un fichier ZIP uploadé (bytes)."""
    try:
        # Sauvegarder le ZIP uploadé temporairement
        tmp_path = DATA_DIR / "upload_temp.zip"
        with open(tmp_path, "wb") as f:
            f.write(zip_bytes)
        success, msg = restore_backup(str(tmp_path))
        tmp_path.unlink(missing_ok=True)
        return success, msg
    except Exception as e:
        return False, str(e)

def get_backup_zip_bytes(zip_path: str) -> bytes:
    """Lit un fichier ZIP et retourne ses bytes (pour téléchargement Streamlit)."""
    with open(zip_path, "rb") as f:
        return f.read()

def _cleanup_old_backups(max_count: int = 30):
    """Supprime les anciennes sauvegardes au-delà de max_count."""
    if not BACKUP_DIR.exists():
        return
    backups = sorted(BACKUP_DIR.glob("backup*.zip"), key=lambda f: f.stat().st_mtime, reverse=True)
    for old in backups[max_count:]:
        try:
            old.unlink()
        except:
            pass

# ══════════════════════════════════════════════════════════════════════════════
# EXPORT / IMPORT
# ══════════════════════════════════════════════════════════════════════════════

def export_table_csv(table_name: str) -> tuple[bytes, str]:
    """Exporte une table en CSV (bytes + nom de fichier)."""
    df = load_table(table_name)
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    filename = f"apitrack_{table_name}_{datetime.now().strftime('%Y%m%d')}.csv"
    return csv_bytes, filename

def export_all_csv_zip() -> bytes:
    """Exporte toutes les tables dans un ZIP avec un CSV par table."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in SCHEMAS:
            csv_bytes, filename = export_table_csv(name)
            zf.writestr(filename, csv_bytes)
        # Résumé JSON
        cfg = load_config()
        cfg["date_export"] = datetime.now().isoformat()
        zf.writestr("apitrack_config.json", json.dumps(cfg, ensure_ascii=False, indent=2))
    return buf.getvalue()

def import_csv(table_name: str, csv_bytes: bytes) -> tuple[bool, str, int]:
    """
    Importe des données depuis un CSV uploadé.
    Fusionne avec les données existantes (évite les doublons sur ID).
    Retourne (succès, message, nb_nouveaux_enregistrements)
    """
    try:
        import io as io_mod
        df_import = pd.read_csv(io_mod.BytesIO(csv_bytes), encoding="utf-8-sig")
        df_existing = load_table(table_name)
        schema = SCHEMAS[table_name]
        id_col = schema["cols"][0]  # Premier colonne = ID

        if id_col in df_existing.columns and id_col in df_import.columns:
            existing_ids = set(df_existing[id_col].astype(str).tolist())
            df_new = df_import[~df_import[id_col].astype(str).isin(existing_ids)]
            nb_new = len(df_new)
            if nb_new > 0:
                df_merged = pd.concat([df_existing, df_new], ignore_index=True)
                save_table(table_name, df_merged)
                return True, f"{nb_new} nouveaux enregistrements importés.", nb_new
            return True, "Aucun nouvel enregistrement (tous déjà présents).", 0
        else:
            save_table(table_name, df_import)
            return True, f"{len(df_import)} enregistrements importés (remplacement).", len(df_import)
    except Exception as e:
        return False, f"Erreur import : {str(e)}", 0

# ══════════════════════════════════════════════════════════════════════════════
# STATISTIQUES DU DOSSIER
# ══════════════════════════════════════════════════════════════════════════════

def get_storage_stats() -> dict:
    """Retourne des statistiques sur l'espace de stockage utilisé."""
    stats = {
        "dossier": str(DATA_DIR.absolute()),
        "tables": {},
        "taille_totale_kb": 0,
        "nb_sauvegardes": len(list_backups()),
        "derniere_sauvegarde": load_config().get("derniere_sauvegarde","Jamais"),
    }
    for name, schema in SCHEMAS.items():
        fp = DATA_DIR / schema["file"]
        if fp.exists():
            size = fp.stat().st_size
            try:
                df = pd.read_csv(fp, encoding="utf-8-sig")
                nb = len(df)
            except:
                nb = 0
            stats["tables"][name] = {"nb": nb, "taille_kb": round(size/1024, 2)}
            stats["taille_totale_kb"] += size / 1024
    stats["taille_totale_kb"] = round(stats["taille_totale_kb"], 2)
    return stats

import io as _io_global
