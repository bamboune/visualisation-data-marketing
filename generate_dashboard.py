import json
import pandas as pd
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

SPREADSHEET_ID = "1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro"
SERVICE_ACCOUNT_FILE = "service_account.json"

def convert_to_serializable(obj):
    """Convertit les objets non-sérialisables pour JSON"""
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d')
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d')
    if pd.isna(obj):
        return None
    return obj

def get_google_sheet(sheet_name, header_row=1):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
    
    all_values = sheet.get_all_values()
    
    if not all_values or len(all_values) < header_row:
        return pd.DataFrame()
    
    headers = all_values[header_row - 1]
    data_rows = all_values[header_row:]
    
    clean_headers = []
    for i, h in enumerate(headers):
        if not h or h.strip() == "":
            clean_headers.append(f"col_{i}")
        else:
            clean_headers.append(h.strip().lower())
    
    df = pd.DataFrame(data_rows, columns=clean_headers)
    
    # Cherche la colonne date
    date_col = None
    for col in df.columns:
        if col.lower() in ['date', 'date_envoi']:
            date_col = col
            break
    
    if date_col:
        df['date'] = pd.to_datetime(df[date_col], errors='coerce')
        print(f"   ✓ '{sheet_name}' : {len(df)} lignes")
    else:
        print(f"   ✓ '{sheet_name}' : {len(df)} lignes")
    
    return df

def get_weather_data(start_date, end_date, lat=45.5, lon=-73.6):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "snowfall_sum", "weather_code"],
        "timezone": "America/Montreal"
    }
    try:
        print(f"   🌐 Météo : {start_date} → {end_date}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'daily' in data and 'time' in data['daily']:
            print(f"   ✅ {len(data['daily']['time'])} jours")
            return data
        else:
            print(f"   ⚠️ Réponse inattendue")
            return None
    except Exception as e:
        print(f"   ❌ Erreur : {e}")
        return None

def main():
    print("📊 Génération du dashboard LMH...")
    
    print("📁 Lecture des feuilles...")
    ventes = get_google_sheet("ventes_quotidiennes", header_row=1)
    infolettres = get_google_sheet("campagnes_email", header_row=1)
    publications = get_google_sheet("publications_social", header_row=1)
    evenements = get_google_sheet("evenements_marketing", header_row=1)
    
    if ventes.empty:
        print("❌ Aucune donnée de ventes")
        return
    
    if 'date' not in ventes.columns:
        print("❌ Colonne 'date' non trouvée")
        return
    
    ventes['date'] = pd.to_datetime(ventes['date'], errors='coerce')
    ventes = ventes.dropna(subset=['date'])
    ventes = ventes.sort_values('date')
    
    start_date = ventes['date'].min().strftime('%Y-%m-%d')
    end_date = ventes['date'].max().strftime('%Y-%m-%d')
    
    print(f"\n🌦️ Récupération météo...")
    weather_data = get_weather_data(start_date, end_date)
    
    if weather_data and 'daily' in weather_data and 'time' in weather_data['daily']:
        weather_df = pd.DataFrame({
            'date': pd.to_datetime(weather_data['daily']['time']),
            'temp_max': weather_data['daily']['temperature_2m_max'],
            'temp_min': weather_data['daily']['temperature_2m_min'],
            'precipitation_mm': weather_data['daily']['precipitation_sum'],
            'snowfall_cm': weather_data['daily']['snowfall_sum'],
            'weather_code': weather_data['daily']['weather_code']
        })
        ventes_meteo = ventes.merge(weather_df, on='date', how='left')
        print("✅ Météo fusionnée")
    else:
        print("⚠️ Pas de météo, continuation")
        ventes_meteo = ventes
    
    # Conversion en string pour les dates
    ventes_meteo['date'] = ventes_meteo['date'].dt.strftime('%Y-%m-%d')
    
    # Nettoyage des NaN
    ventes_meteo = ventes_meteo.where(pd.notnull(ventes_meteo), None)
    
    # Calcul du total des ventes
    total_ventes = 0
    if 'ventes_total' in ventes.columns:
        ventes_total_clean = pd.to_numeric(ventes['ventes_total'], errors='coerce')
        total_ventes = float(ventes_total_clean.sum()) if ventes_total_clean.notna().any() else 0
    
    dashboard_data = {
        'ventes': ventes_meteo.to_dict(orient='records'),
        'infolettres': infolettres.where(pd.notnull(infolettres), None).to_dict(orient='records'),
        'publications': publications.where(pd.notnull(publications), None).to_dict(orient='records'),
        'evenements': evenements.where(pd.notnull(evenements), None).to_dict(orient='records'),
        'stats': {
            'total_ventes': total_ventes,
            'total_commandes': 0,
            'date_min': start_date,
            'date_max': end_date
        }
    }
    
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2, default=convert_to_serializable)
    
    print(f"\n✅ SUCCÈS ! data.json généré")
    print(f"   📅 {len(ventes)} jours du {start_date} au {end_date}")
    print(f"   💰 Total ventes : {total_ventes:,.2f} $")

if __name__ == "__main__":
    main()
