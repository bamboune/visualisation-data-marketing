import json
import pandas as pd
import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import re

SPREADSHEET_ID = "1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro"
SERVICE_ACCOUNT_FILE = "service_account.json"

def clean_numeric(value):
    """Nettoie une valeur pour la convertir en nombre (gère virgules françaises)"""
    if value is None or pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip()
    if s == '':
        return 0.0
    
    # Remplace la virgule décimale par un point (format français → anglais)
    s = s.replace(',', '.')
    
    # Enlève tout ce qui n'est pas chiffre, point ou moins
    s = re.sub(r'[^\d.-]', '', s)
    
    # Gère les cas comme ".123" ou "123."
    if s == '' or s == '-':
        return 0.0
    
    try:
        return float(s)
    except:
        return 0.0

def convert_to_serializable(obj):
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
    
    clean_headers = [h.strip().lower() if h else f"col_{i}" for i, h in enumerate(headers)]
    
    df = pd.DataFrame(data_rows, columns=clean_headers)
    
    # Cherche la colonne date
    date_col = None
    for col in df.columns:
        if col in ['date', 'date_envoi']:
            date_col = col
            break
    
    if date_col:
        df['date'] = pd.to_datetime(df[date_col], errors='coerce')
    
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
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if 'daily' in data and 'time' in data['daily']:
            return data
        return None
    except Exception as e:
        print(f"   ❌ Erreur météo : {e}")
        return None

def main():
    print("📊 Génération du dashboard LMH...")
    
    print("📁 Lecture des feuilles...")
    ventes = get_google_sheet("ventes_quotidiennes", header_row=1)
    infolettres = get_google_sheet("campagnes_email", header_row=1)
    publications = get_google_sheet("publications_social", header_row=1)
    evenements = get_google_sheet("evenements_marketing", header_row=1)
    
    print(f"   📊 Ventes : {len(ventes)} lignes")
    
    if ventes.empty:
        print("❌ Aucune donnée de ventes")
        return
    
    if 'date' not in ventes.columns:
        print("❌ Colonne 'date' non trouvée")
        return
    
    # NETTOYAGE DES DONNÉES NUMÉRIQUES (avec gestion virgule)
    numeric_columns = ['ventes_bel', 'ventes_boutique', 'ventes_wholesale', 'ventes_total',
                       'commandes_bel', 'commandes_boutique', 'commandes_wholesale', 'commandes_total',
                       'panier_moyen_bel', 'panier_moyen_boutique']
    
    for col in numeric_columns:
        if col in ventes.columns:
            ventes[col] = ventes[col].apply(clean_numeric)
            print(f"   ✅ Nettoyé : {col}")
    
    ventes['date'] = pd.to_datetime(ventes['date'], errors='coerce')
    ventes = ventes.dropna(subset=['date'])
    ventes = ventes.sort_values('date')
    
    start_date = ventes['date'].min().strftime('%Y-%m-%d')
    end_date = ventes['date'].max().strftime('%Y-%m-%d')
    
    print(f"\n🌦️ Récupération météo {start_date} → {end_date}...")
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
        print("   ✅ Météo fusionnée")
    else:
        ventes_meteo = ventes
    
    ventes_meteo['date'] = ventes_meteo['date'].dt.strftime('%Y-%m-%d')
    ventes_meteo = ventes_meteo.where(pd.notnull(ventes_meteo), None)
    
    # Calcul des totaux
    total_bel = ventes['ventes_bel'].sum() if 'ventes_bel' in ventes else 0
    total_boutique = ventes['ventes_boutique'].sum() if 'ventes_boutique' in ventes else 0
    total_wholesale = ventes['ventes_wholesale'].sum() if 'ventes_wholesale' in ventes else 0
    total_all = total_bel + total_boutique + total_wholesale
    
    # Panier moyen BEL (moyenne des paniers, pas somme)
    panier_moyen = ventes['panier_moyen_bel'].mean() if 'panier_moyen_bel' in ventes else 0
    
    print(f"\n💰 Vérification des totaux :")
    print(f"   BEL : {total_bel:,.2f} $")
    print(f"   Boutique : {total_boutique:,.2f} $")
    print(f"   Wholesale : {total_wholesale:,.2f} $")
    print(f"   TOTAL : {total_all:,.2f} $")
    print(f"   Panier moyen BEL : {panier_moyen:,.2f} $")
    
    dashboard_data = {
        'ventes': ventes_meteo.to_dict(orient='records'),
        'infolettres': infolettres.where(pd.notnull(infolettres), None).to_dict(orient='records'),
        'publications': publications.where(pd.notnull(publications), None).to_dict(orient='records'),
        'evenements': evenements.where(pd.notnull(evenements), None).to_dict(orient='records'),
        'stats': {
            'total_ventes': total_all,
            'total_ventes_bel': total_bel,
            'total_ventes_boutique': total_boutique,
            'total_ventes_wholesale': total_wholesale,
            'panier_moyen_bel': panier_moyen,
            'date_min': start_date,
            'date_max': end_date
        }
    }
    
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(dashboard_data, f, ensure_ascii=False, indent=2, default=convert_to_serializable)
    
    print(f"\n✅ SUCCÈS ! data.json généré")
    print(f"   📅 {len(ventes)} jours du {start_date} au {end_date}")

if __name__ == "__main__":
    main()
