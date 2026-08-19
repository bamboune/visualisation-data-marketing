# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

A static marketing dashboard for LMH (Les Mauvaises Herbes). It visualises daily sales, newsletter sends, social media posts, weather, and marketing events in a single-page HTML app. There is no backend — data is generated offline and committed as `data.json`.

## Data pipeline

```
Google Sheets ──► generate_dashboard.py ──► data.json ──► index.html (Vercel)
                         ▲
                  events_reference.json  (historical events, correct dates)
```

1. **`generate_dashboard.py`** runs daily via GitHub Actions (`update.yml`, 6h UTC / 2h Montréal). It reads three Google Sheets (`ventes_quotidiennes`, `evenements_marketing`, from Mailerlite API, and Facebook/Instagram Graph API), fetches weather from Open-Meteo, then writes `data.json` and commits it.
2. **`data.json`** is the only runtime artefact consumed by the front end. Vercel serves it with `no-cache` headers (see `vercel.json`).
3. **`index.html`** fetches `data.json` on load, renders everything client-side with Chart.js and dayjs (both loaded from CDN).

## Key architectural decisions

### events_reference.json — why it exists
The Google Sheet `evenements_marketing` was migrated from wide format (one row per day, many columns) to long format (one row per event) on 2026-08-13. The migration introduced a day↔month date inversion for events whose dates were stored as DD/MM text. `events_reference.json` is a committed snapshot of 1200 corrected events. `generate_dashboard.py` uses it as the authoritative source for all events on or before `MIGRATION_DATE = '2026-08-13'` and reads only newer rows from the sheet.

**If this file needs to be regenerated:** run the reconstruction script (described in git history) against commit `2e60363` (the last correct pre-migration `data.json`).

### Event type normalisation
The sheet accepts free-text type values. `_normalize_event_type()` in `generate_dashboard.py` maps aliases (`"web/funnel"` → `webmestre_funnels`, `"promo"` → `rabais_promos`, etc.). The canonical type values that `ACTION_TAGS` in `index.html` recognises are: `rabais_promos`, `lancement_produits_ateliers`, `bis` / `bis_alertes_back_in_stock`, `infolettre`, `push_notif`, `billet_blogue`, `reseaux_sociaux`, `webmestre_funnels`, `commentaires`, `autre`, `contexte`.

### Event add / delete UI
The dashboard has a modal form. On submit it POSTs to a Google Apps Script webhook (`WEBHOOK_URL` in `index.html`). The script (`webhook.ts` — source reference only, deployed separately in Apps Script) mutates the sheet and calls `triggerWorkflow()` to dispatch the GitHub Actions `update.yml` run. Changes are also persisted in **localStorage** (`lmh_events_added`, `lmh_events_deleted`) so they survive page reloads before the next `data.json` regeneration.

## GitHub secrets required

| Secret | Used by |
|---|---|
| `GCP_SERVICE_ACCOUNT_KEY` | `generate_dashboard.py` → Google Sheets read access |
| `MAILERLITE_API_KEY` | MailerLite campaign fetch |
| `FACEBOOK_PAGE_TOKEN` | Facebook/Instagram Graph API |
| `GITHUB_TOKEN` | auto-provided; used by Apps Script webhook to dispatch `update.yml` |

The Apps Script also needs a `GITHUB_TOKEN` script property set manually in the Apps Script editor.

## Running locally

```bash
pip install -r requirements.txt
# Place GCP service account JSON at service_account.json (not committed)
MAILERLITE_API_KEY=... FACEBOOK_PAGE_TOKEN=... python generate_dashboard.py
# Then open index.html in a browser (needs data.json in the same directory)
```

## Triggering a manual data refresh

GitHub Actions → **"Mettre à jour le dashboard"** → **Run workflow** (on `main`).

## One-time sheet correction

If the `evenements_marketing` Google Sheet needs to be rewritten with the correct dates from `events_reference.json`:

GitHub Actions → **"Corriger le sheet evenements_marketing"** → **Run workflow**.

Script: `fix_sheet_events.py`. Only needed if `events_reference.json` is updated or the sheet is corrupted again.

## Deployment

Vercel auto-deploys `main`. There is no build step — Vercel serves `index.html` and `data.json` as static files.
