import json
import pandas as pd
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# Configuration
SPREADSHEET_ID = "1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro"
SERVICE_ACCOUNT_FILE = "service_account.json"

def get_google_sheet(sheet_name, header_row=2):
    """
    Lit une feuille Google Sheets en utilisant une ligne spécifique comme en-têtes.
    header_row = 2 signifie utiliser la ligne 2 (index 1 en Python)
    """
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
    
    # Récupère toutes les valeurs
    all_values = sheet.get_all_values()
    
    if not all_values or len(all_values) < header_row:
        return pd.DataFrame()
    
    # Utilise la ligne spécifiée comme en-têtes
    headers = all_values[header_row - 1]  # -1 car Python commence à 0
    data_rows = all_values[header_row:]   # À partir de la ligne suivante
    
    # Nettoie les en-têtes : remplace les vides par col_X
    clean_headers = []
    for i, h in enumerate(headers):
        if not h or h.strip() == "":
            clean_headers.append(f"col_{i}")
        else:
            clean_headers.append(h.strip())
    
    # Construit le DataFrame
    df = pd.DataFrame(data_rows, columns=clean_headers)
    
    # Convertit la colonne date si elle existe
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    elif 'Date' in df.columns:
        df['date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.rename(columns={'Date': 'date'})
    elif 'DATE' in df.columns:
        df['date'] = pd.to_datetime(df['DATE'], errors='coerce')
        df = df.rename(columns={'DATE': 'date'})
    elif 'date_envoi' in df.columns:
        df['date'] = pd.to_datetime(df['date_envoi'], errors='coerce')
    
    # Nettoie les colonnes numériques pour ventes
    numeric_cols = ['ventes_bel', 'ventes_boutique', 'ventes_wholesale', 'ventes_total', 
                    'commandes_bel', 'commandes_boutique', 'commandes_wholesale', 'commandes_total',
                    'panier_moyen_bel', 'panier_moyen_boutique']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def get_weather_data(start_date, end_date, lat=45.5, lon=-73.6):
    """Récupère la météo historique pour une période"""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "snowfall_sum", "weather_code"],
        "timezone": "America/Montreal"
    }
    response = requests.get(url, params=params)
    return response.json()

def main():
    print("📊 Génération du dashboard LMH...")
    
    # 1. Lecture des données Google Sheets
    print("📁 Lecture de ventes_quotidiennes...")
    ventes = get_google_sheet("ventes_quotidiennes", header_row=2)
    
    print("📁 Lecture de campagnes_email...")
    infolettres = get_google_sheet("campagnes_email", header_row=2)
    
    print("📁 Lecture de publications_social...")
    publications = get_google_sheet("publications_social", header_row=2)
    
    print("📁 Lecture de evenements_marketing...")
    evenements = get_google_sheet("evenements_marketing", header_row=2)
    
    print(f"✅ Ventes chargées : {len(ventes)} lignes")
    print(f"✅ Infolettres : {len(infolettres)} lignes")
    print(f"✅ Publications : {len(publications)} lignes")
    print(f"✅ Événements : {len(evenements)} lignes")
    
    # Vérifie qu'on a des données
    if ventes.empty:
        print("❌ ERREUR : Aucune donnée de ventes trouvée !")
        print("   Vérifie que la feuille 'ventes_quotidiennes' existe et contient des données.")
        return
    
    # 2. Nettoyage des dates
    ventes['date'] = pd.to_datetime(ventes['date'], errors='coerce')
    ventes = ventes.dropna(subset=['date'])
    ventes = ventes.sort_values('date')
    
    # 3. Récupération météo pour toute la période
    start_date = ventes['date'].min().strftime('%Y-%m-%d')
    end_date = ventes['date'].max().strftime('%Y-%m-%d')
    print(f"🌦️ Récupération météo du {start_date} au {end_date}...")
    
    weather_data = get_weather_data(start_date, end_date)
    
    # 4. Fusion météo + ventes
    if 'daily' in weather_data and 'time' in weather_data['daily']:
        weather_df = pd.DataFrame({
            'date': pd.to_datetime(weather_data['daily']['time']),
            'temp_max': weather_data['daily']['temperature_2m_max'],
            'temp_min': weather_data['daily']['temperature_2m_min'],
            'precipitation_mm': weather_data['daily']['precipitation_sum'],
            'snowfall_cm': weather_data['daily']['snowfall_sum'],
            'weather_code': weather_data['daily']['weather_code']
        })
        
        ventes_meteo = ventes.merge(weather_df, on='date', how='left')
    else:
        print("⚠️ Attention : Pas de données météo disponibles")
        ventes_meteo = ventes
    
    # 5. Préparation du JSON final (remplacer NaN par None)
    ventes_meteo = ventes_meteo.where(pd.notnull(ventes_meteo), None)
    infolettres_clean = infolettres.where(pd.notnull(infolettres), None)
    publications_clean = publications.where(pd.notnull(publications), None)
    evenements_clean = evenements.where(pd.notnull(evenements), None)
    
    # Convertir les dates en string pour le JSON
    if 'date' in ventes_meteo.columns:
        ventes_meteo['date'] = ventes_meteo['date'].astype(str)
    
    dashboard_data = {
        'ventes': ventes_meteo.to_dict(orient='records'),
        'infolettres': infolettres_clean.to_dict(orient='records'),
        'publications': publications_clean.to_dict(orient='records'),
        'evenements': evenements_clean.to_dict(orient='records'),
        'stats': {
            'total_ventes': float(ventes['ventes_total'].sum()) if 'ventes_total' in ventes else 0,
            'total_commandes': int(ventes['commandes_total'].sum()) if 'commandes_total' in ventes else 0,
            'date_min': start_date,
            'date_max': end_date
        }
    }
    
    # 6. Sauvegarde JSON
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ data.json généré avec succès !")
    print(f"   - Période : {start_date} → {end_date}")
    print(f"   - Total ventes : {dashboard_data['stats']['total_ventes']:,.2f} $")

if __name__ == "__main__":
    main()
