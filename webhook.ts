// Nouvelle structure : date | type | description | note | event_id
// Ligne 1 = en-têtes, données à partir de la ligne 2

function doPost(e) {
  try {
    const data  = JSON.parse(e.postData.contents);
    const ss    = SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
    const sheet = ss.getSheetByName(CONFIG.SHEETS.EVENEMENTS);
    const type  = data.type || 'create';

    if (type === 'delete') {
      const deleted = deleteEvent(sheet, data.event_id, data.date, data.action);
      if (deleted) triggerWorkflow();
      return ContentService.createTextOutput(JSON.stringify({ status: deleted ? 'success' : 'not_found' }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    if (type === 'update') {
      const updated = updateEvent(sheet, data.event_id, data.date, data.action,
                                  data.date_new, data.action_new, data.note_new, data.type_action);
      if (updated) triggerWorkflow();
      return ContentService.createTextOutput(JSON.stringify({ status: updated ? 'success' : 'not_found' }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    // CREATE
    const date        = data.date;
    const action      = data.action;
    const note        = data.note || '';
    const event_id    = data.event_id || String(Date.now());
    const type_action = data.type_action || 'autre';

    if (!date || !action) {
      return ContentService.createTextOutput(JSON.stringify({ status: 'error', message: 'Date et action requis' }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    sheet.appendRow([date, type_action, action, note, event_id]);
    triggerWorkflow();

    return ContentService.createTextOutput(JSON.stringify({ status: 'success' }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function findEventRow(sheet, event_id, date, action) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return -1;

  if (event_id) {
    const ids = sheet.getRange(2, 5, lastRow - 1, 1).getValues();
    for (let i = 0; i < ids.length; i++) {
      if (String(ids[i][0]).trim() === String(event_id).trim()) return i + 2;
    }
  }

  if (date && action) {
    const dateKey = String(date).substring(0, 10);
    const rows = sheet.getRange(2, 1, lastRow - 1, 3).getValues();
    for (let i = 0; i < rows.length; i++) {
      if (String(rows[i][0]).substring(0, 10) === dateKey &&
          String(rows[i][2]).trim() === String(action).trim()) return i + 2;
    }
  }

  return -1;
}

function deleteEvent(sheet, event_id, date, action) {
  const row = findEventRow(sheet, event_id, date, action);
  if (row === -1) return false;
  sheet.deleteRow(row);
  return true;
}

function updateEvent(sheet, event_id, date_orig, action_orig, date_new, action_new, note_new, type_action_new) {
  const row = findEventRow(sheet, event_id, date_orig, action_orig);
  if (row === -1) return false;
  if (date_new)        sheet.getRange(row, 1).setValue(date_new);
  if (type_action_new) sheet.getRange(row, 2).setValue(type_action_new);
  if (action_new)      sheet.getRange(row, 3).setValue(action_new);
  sheet.getRange(row, 4).setValue(note_new || '');
  return true;
}

function triggerWorkflow() {
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  const url   = 'https://api.github.com/repos/bamboune/visualisation-data-marketing/actions/workflows/update.yml/dispatches';
  UrlFetchApp.fetch(url, {
    method: 'post',
    headers: {
      'Authorization': `token ${token}`,
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify({ ref: 'main' }),
    muteHttpExceptions: true
  });
}

// ── À EXÉCUTER UNE SEULE FOIS pour migrer l'ancienne structure ──
function migrateEvenementsToLongFormat() {
  const ss    = SpreadsheetApp.openById(CONFIG.SPREADSHEET_ID);
  const sheet = ss.getSheetByName(CONFIG.SHEETS.EVENEMENTS);

  // Backup
  const backup = sheet.copyTo(ss);
  backup.setName('evenements_backup_' + new Date().toISOString().substring(0, 10));
  Logger.log('✅ Backup créé : ' + backup.getName());

  const all = sheet.getDataRange().getValues();

  // L'ancienne structure a les en-têtes en ligne 2 (index 1)
  // Les données commencent à la ligne 3 (index 2)
  const HEADERS = all[1] || [];
  const colIdx  = name => HEADERS.findIndex(h => String(h).trim().toLowerCase() === name.toLowerCase());

  const ACTION_TYPES = [
    'rabais_promos', 'lancement_produits_ateliers', 'bis_alertes_back_in_stock',
    'infolettre', 'push_notif', 'billet_blogue', 'reseaux_sociaux', 'webmestre_funnels',
  ];
  const CONTEXTE_TYPES = ['politique_actualite', 'fetes_feries_conges', 'couvertures_mediatiques'];

  const iDate = colIdx('date');
  const iNote = colIdx('commentaires_notes');
  const iId   = colIdx('event_id');

  const newRows = [['date', 'type', 'description', 'note', 'event_id']];
  let count = 0;

  for (let r = 2; r < all.length; r++) {
    const row  = all[r];
    const date = row[iDate];
    if (!date || String(date).trim() === '') continue;

    const note = String(iNote >= 0 ? row[iNote] : '').trim();
    const eid  = String(iId   >= 0 ? row[iId]   : '').trim();

    let first = true;

    for (const t of ACTION_TYPES) {
      const ci  = colIdx(t);
      if (ci < 0) continue;
      const val = String(row[ci]).trim();
      if (!val) continue;
      newRows.push([date, t, val, first ? note : '', first && eid ? eid : String(Date.now()) + '_' + r + '_' + ci]);
      first = false;
      count++;
    }

    for (const t of CONTEXTE_TYPES) {
      const ci  = colIdx(t);
      if (ci < 0) continue;
      const val = String(row[ci]).trim();
      if (!val) continue;
      newRows.push([date, 'contexte', t.replace(/_/g,' ') + ' : ' + val, '', String(Date.now()) + '_' + r + '_' + ci]);
      count++;
    }

    // Ligne avec une note mais aucune action
    if (first && note) {
      newRows.push([date, 'autre', note, '', eid || String(Date.now()) + '_' + r]);
      count++;
    }
  }

  sheet.clearContents();
  sheet.getRange(1, 1, newRows.length, 5).setValues(newRows);
  Logger.log(`✅ Migration terminée : ${count} événements dans ${newRows.length - 1} lignes.`);
}
