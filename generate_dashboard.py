import json
import os
import pandas as pd
import requests
from datetime import date, datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re

# Configuration
SPREADSHEET_ID = "1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro"
MAILERLITE_API_KEY = os.environ.get("MAILERLITE_API_KEY", "")
FACEBOOK_PAGE_TOKEN = os.environ.get("FACEBOOK_PAGE_TOKEN", "")
FACEBOOK_PAGE_ID = "164239587106721"
SERVICE_ACCOUNT_FILE = "service_account.json"

def super_clean_numeric(value):
    """Version ultra-agressive pour nettoyer les nombres"""
    if value is None or pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    
    s = str(value).strip()
    if s == '':
        return 0.0
    
    if not hasattr(super_clean_numeric, "count"):
        super_clean_numeric.count = 0
    if super_clean_numeric.count < 5:
        print(f"   🔍 Valeur brute: '{s}'")
        super_clean_numeric.count += 1
    
    s = s.replace(',', '.')
    s = re.sub(r'[^\d.-]', '', s)
    parts = s.split('.')
    if len(parts) > 2:
        s = parts[0] + ''.join(parts[1:-1]) + '.' + parts[-1]
    if s == '' or s == '-' or s == '.':
        return 0.0
    try:
        return float(s)
    except:
        return 0.0

def convert_to_serializable(obj):
    if pd.isna(obj) or (hasattr(pd, 'NaT') and obj is pd.NaT) or (isinstance(obj, pd._libs.tslibs.nattype.NaTType)):
        return None
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d')
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%d')
    return obj

def get_google_sheet(sheet_name, header_row=1, col_aliases=None):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(sheet_name)
    
    all_values = sheet.get_all_values()
    if not all_values or len(all_values) < header_row:
        return pd.DataFrame()
    
    headers = all_values[header_row - 1]
    clean_headers = []
    for i, h in enumerate(headers):
        if not h or h.strip() == "":
            clean_headers.append((col_aliases or {}).get(i, f"col_{i}"))
        else:
            clean_headers.append(h.strip().lower())
    
    data_rows = all_values[header_row:]
    rows = []
    for row in data_rows:
        if len(row) > 0 and row[0] and str(row[0]).strip():
            if len(row) < len(clean_headers):
                row.extend([''] * (len(clean_headers) - len(row)))
            rows.append(row)
    
    df = pd.DataFrame(rows, columns=clean_headers)
    
    # Date
    date_col = None
    for col in df.columns:
        if col in ['date', 'date_envoi']:
            date_col = col
            break
    if date_col:
        df['date'] = pd.to_datetime(df[date_col], errors='coerce')
    
    # Nettoyage numérique
    numeric_cols = [
        'ventes_bel', 'ventes_boutique', 'ventes_wholesale', 'ventes_total',
        'commandes_bel', 'commandes_boutique', 'commandes_wholesale', 'commandes_total',
        'panier_moyen_bel', 'panier_moyen_boutique',
        'likes', 'commentaires', 'partages', 'sauvegardes', 'reach', 'impressions'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(super_clean_numeric)
    
    return df

def get_mailerlite_campaigns():
    """Récupère toutes les campagnes envoyées depuis le nouveau Mailerlite (API v1)."""
    if not MAILERLITE_API_KEY:
        print("   ⚠️  MAILERLITE_API_KEY non définie — infolettres ignorées")
        return pd.DataFrame()

    headers = {
        "Authorization": f"Bearer {MAILERLITE_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    campaigns = []
    page = 1

    while True:
        try:
            resp = requests.get(
                "https://connect.mailerlite.com/api/campaigns",
                headers=headers,
                params={"filter[status]": "sent", "limit": 25, "page": page},
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
        except Exception as e:
            print(f"   ❌ Erreur Mailerlite : {e}")
            break

        for c in body.get("data", []):
            sent_at = c.get("sent_at") or c.get("created_at") or ""
            date_str = sent_at[:10] if sent_at else None
            campaigns.append({
                "date":       date_str,
                "date_envoi": date_str,
                "sujet":      c.get("subject") or c.get("name") or "",
            })

        meta = body.get("meta", {})
        if page >= meta.get("last_page", 1):
            break
        page += 1

    print(f"   📧 Mailerlite : {len(campaigns)} campagnes récupérées")
    if not campaigns:
        return pd.DataFrame()

    df = pd.DataFrame(campaigns)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.sort_values("date")


POSTS_SINCE = "2026-05-01"


def get_facebook_posts():
    """Récupère les posts de la page Facebook via Graph API depuis POSTS_SINCE."""
    if not FACEBOOK_PAGE_TOKEN:
        print("   ⚠️  FACEBOOK_PAGE_TOKEN non définie — posts Facebook ignorés")
        return []

    posts = []
    url = f"https://graph.facebook.com/v19.0/{FACEBOOK_PAGE_ID}/posts"
    params = {
        "fields": "id,message,created_time,permalink_url",
        "access_token": FACEBOOK_PAGE_TOKEN,
        "limit": 100,
        "since": POSTS_SINCE,
    }

    while url:
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            body = resp.json()
        except Exception as e:
            print(f"   ❌ Erreur Facebook posts : {e}")
            break

        for post in body.get("data", []):
            date_str = post.get("created_time", "")[:10]
            posts.append({
                "date": date_str,
                "plateforme": "facebook",
                "description_courte": (post.get("message") or "")[:200],
                "lien_post": post.get("permalink_url", ""),
            })

        next_page = body.get("paging", {}).get("next")
        url = next_page if next_page else None
        params = {}

    print(f"   📘 Facebook : {len(posts)} posts récupérés")
    return posts


def get_instagram_posts():
    """Récupère les posts Instagram Business liés à la page Facebook."""
    if not FACEBOOK_PAGE_TOKEN:
        print("   ⚠️  FACEBOOK_PAGE_TOKEN non définie — posts Instagram ignorés")
        return []

    # Récupère l'ID du compte Instagram Business lié à la page
    try:
        resp = requests.get(
            f"https://graph.facebook.com/v19.0/{FACEBOOK_PAGE_ID}",
            params={"fields": "instagram_business_account", "access_token": FACEBOOK_PAGE_TOKEN},
            timeout=30,
        )
        resp.raise_for_status()
        ig_account = resp.json().get("instagram_business_account", {})
        ig_id = ig_account.get("id")
    except Exception as e:
        print(f"   ❌ Erreur récupération compte Instagram : {e}")
        return []

    if not ig_id:
        print("   ⚠️  Aucun compte Instagram Business lié à la page")
        return []

    posts = []
    url = f"https://graph.facebook.com/v19.0/{ig_id}/media"
    params = {
        "fields": "id,caption,timestamp,permalink",
        "access_token": FACEBOOK_PAGE_TOKEN,
        "limit": 100,
    }

    while url:
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            body = resp.json()
        except Exception as e:
            print(f"   ❌ Erreur Instagram posts : {e}")
            break

        page_data = body.get("data", [])
        for post in page_data:
            date_str = post.get("timestamp", "")[:10]
            if date_str < POSTS_SINCE:
                continue
            posts.append({
                "date": date_str,
                "plateforme": "instagram",
                "description_courte": (post.get("caption") or "")[:200],
                "lien_post": post.get("permalink", ""),
            })

        # Arrête la pagination si la page entière est avant la date limite
        oldest = min((p.get("timestamp", "")[:10] for p in page_data), default="")
        if oldest and oldest < POSTS_SINCE:
            break

        next_page = body.get("paging", {}).get("next")
        url = next_page if next_page else None
        params = {}

    print(f"   📸 Instagram : {len(posts)} posts récupérés")
    return posts


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
    
    print("📁 Lecture des données...")
    ventes = get_google_sheet("ventes_quotidiennes", header_row=1)
    infolettres = get_mailerlite_campaigns()
    # col_aliases: positional fallback when header cells are empty (e.g. after migration glitch)
    evenements = get_google_sheet(
        "evenements_marketing", header_row=1,
        col_aliases={2: 'description', 3: 'note', 4: 'event_id'}
    )

    print("📱 Récupération des publications Facebook/Instagram...")
    fb_posts = get_facebook_posts()
    ig_posts = get_instagram_posts()
    all_posts = fb_posts + ig_posts
    publications = pd.DataFrame(all_posts) if all_posts else pd.DataFrame()
    if not publications.empty:
        publications["date"] = pd.to_datetime(publications["date"], errors="coerce")

    # Filtrer événements avec date nulle ou future
    if not evenements.empty and 'date' in evenements.columns:
        evenements = evenements[
            evenements['date'].notna() &
            (evenements['date'] <= pd.Timestamp(date.today()))
        ]

    print(f"   📊 Ventes brutes : {len(ventes)} lignes")
    print(f"   📧 Infolettres : {len(infolettres)} lignes")
    print(f"   📱 Publications : {len(publications)} lignes")
    print(f"   ⚡ Événements : {len(evenements)} lignes")
    
    if ventes.empty:
        print("❌ Aucune donnée de ventes")
        return
    
    if 'ventes_bel' in ventes.columns:
        print(f"   🔍 Exemples ventes_bel après conversion : {ventes['ventes_bel'].head(5).tolist()}")
    
    if 'date' not in ventes.columns:
        print("❌ Colonne 'date' non trouvée")
        return
    
    ventes['date'] = pd.to_datetime(ventes['date'], errors='coerce')
    ventes = ventes.dropna(subset=['date'])
    
    aujourdhui = date.today()
    ventes = ventes[ventes['date'] <= pd.Timestamp(aujourdhui)]
    print(f"   🗓️ Après filtrage dates futures : {len(ventes)} lignes")
    
    ventes = ventes.sort_values('date')
    start_date = ventes['date'].min().strftime('%Y-%m-%d')
    end_date = ventes['date'].max().strftime('%Y-%m-%d')
    
    print(f"\n🌦️ Récupération météo du {start_date} au {end_date}...")
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
        print("   ⚠️ Pas de météo disponible")
    
    ventes_meteo['date'] = ventes_meteo['date'].dt.strftime('%Y-%m-%d')
    ventes_meteo = ventes_meteo.where(pd.notnull(ventes_meteo), None)
    
    total_bel = ventes['ventes_bel'].sum() if 'ventes_bel' in ventes else 0
    total_boutique = ventes['ventes_boutique'].sum() if 'ventes_boutique' in ventes else 0
    total_wholesale = ventes['ventes_wholesale'].sum() if 'ventes_wholesale' in ventes else 0
    total_all = total_bel + total_boutique + total_wholesale
    panier_moyen = ventes['panier_moyen_bel'].mean() if 'panier_moyen_bel' in ventes else 0
    
    print(f"\n💰 Totaux : BEL={total_bel:,.2f}, Boutique={total_boutique:,.2f}, TOTAL={total_all:,.2f}")
    
    infolettres_clean = infolettres.where(pd.notnull(infolettres), None)
    publications_clean = publications.where(pd.notnull(publications), None)
    evenements_clean = evenements.where(pd.notnull(evenements), None)
    
    dashboard_data = {
        'ventes': ventes_meteo.to_dict(orient='records'),
        'infolettres': infolettres_clean.to_dict(orient='records'),
        'publications': publications_clean.to_dict(orient='records'),
        'evenements': evenements_clean.to_dict(orient='records'),
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
    print(f"   📱 {len(publications_clean)} publications incluses")
    print(f"   ⚡ {len(evenements_clean)} événements inclus")

if __name__ == "__main__":
    main()
