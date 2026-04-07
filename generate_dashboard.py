import json
import pandas as pd
import requests
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# Configuration
SPREADSHEET_ID = "1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro"
SERVICE_ACCOUNT_FILE = "service_account.json"  # Ton fichier JSON local ou GitHub Secret

def get_google_sheet(sheet_name):
    """Lit une feuille Google Sheets et retourne un DataFrame"""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
    data = sheet.get_all_records()
    return pd.DataFrame(data)

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
    ventes = get_google_sheet("ventes_quotidiennes")
    
    print("📁 Lecture de campagnes_email...")
    infolettres = get_google_sheet("campagnes_email")
    
    print("📁 Lecture de publications_social...")
    publications = get_google_sheet("publications_social")
    
    print("📁 Lecture de evenements_marketing...")
    evenements = get_google_sheet("evenements_marketing")
    
    # 2. Nettoyage des données
    ventes['date'] = pd.to_datetime(ventes['DATE'])
    ventes = ventes.sort_values('date')
    
    # 3. Récupération météo pour toute la période
    start_date = ventes['date'].min().strftime('%Y-%m-%d')
    end_date = ventes['date'].max().strftime('%Y-%m-%d')
    print(f"🌦️ Récupération météo du {start_date} au {end_date}...")
    
    weather_data = get_weather_data(start_date, end_date)
    
    # 4. Fusion météo + ventes
    weather_df = pd.DataFrame({
        'date': pd.to_datetime(weather_data['daily']['time']),
        'temp_max': weather_data['daily']['temperature_2m_max'],
        'temp_min': weather_data['daily']['temperature_2m_min'],
        'precipitation_mm': weather_data['daily']['precipitation_sum'],
        'snowfall_cm': weather_data['daily']['snowfall_sum'],
        'weather_code': weather_data['daily']['weather_code']
    })
    
    ventes_meteo = ventes.merge(weather_df, on='date', how='left')
    
    # 5. Préparation du JSON final
    dashboard_data = {
        'ventes': ventes_meteo.to_dict(orient='records'),
        'infolettres': infolettres.to_dict(orient='records'),
        'publications': publications.to_dict(orient='records'),
        'evenements': evenements.to_dict(orient='records'),
        'stats': {
            'total_ventes': float(ventes['ventes_total'].sum()),
            'total_commandes': int(ventes['commandes_total'].sum()) if 'commandes_total' in ventes else 0,
            'date_min': start_date,
            'date_max': end_date
        }
    }
    
    # 6. Sauvegarde JSON
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2)
    
    print("✅ data.json généré avec succès !")

if __name__ == "__main__":
    main()
