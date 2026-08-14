"""
Script one-time : corrige les dates et descriptions dans le sheet evenements_marketing
en le réécrivant depuis events_reference.json (source de vérité avec dates correctes).

À exécuter UNE SEULE FOIS via GitHub Actions (workflow fix_sheet.yml).
Après exécution, generate_dashboard.py lira des données correctes directement depuis le sheet.
"""
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SPREADSHEET_ID = "1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro"
SERVICE_ACCOUNT_FILE = "service_account.json"
EVENTS_REFERENCE_FILE = "events_reference.json"
SHEET_NAME = "evenements_marketing"

def fix_sheet():
    print(f"📋 Chargement de {EVENTS_REFERENCE_FILE}...")
    with open(EVENTS_REFERENCE_FILE, 'r', encoding='utf-8') as f:
        reference = json.load(f)
    print(f"   {len(reference)} événements à écrire")

    print("🔗 Connexion à Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    print(f"🗑️  Effacement du sheet {SHEET_NAME}...")
    sheet.clear()

    header = ['date', 'type', 'description', 'note', 'event_id']
    rows = [header]
    for ev in reference:
        rows.append([
            ev.get('date') or '',
            ev.get('type') or '',
            ev.get('description') or '',
            ev.get('note') or '',
            ev.get('event_id') or '',
        ])

    print(f"✍️  Écriture de {len(reference)} événements...")
    sheet.update('A1', rows)
    print(f"✅ Sheet mis à jour avec {len(reference)} événements et des dates correctes.")
    print("   Le prochain run de generate_dashboard.py lira des dates correctes depuis le sheet.")

if __name__ == "__main__":
    fix_sheet()
