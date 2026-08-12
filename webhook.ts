// Mapping fixe catégorie → numéro de colonne (structure immuable de la feuille)
var COL_MAP = {
  'rabais_promos':               2,   // B
  'lancement_produits_ateliers': 3,   // C
  'bis_alertes_back_in_stock':   4,   // D
  'infolettre':                  5,   // E
  'push_notif':                  6,   // F
  'billet_blogue':               7,   // G
  'webmestre_funnels':           8,   // H
};
// 'reseaux_sociaux', 'autre', 'contexte' → pas de colonne dédiée → commentaires_notes (O = 15)

function doPost(e) {
  try {
    const data  = JSON.parse(e.postData.contents);
    const sheet = SpreadsheetApp.openById('1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro').getSheetByName('evenements_marketing');
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

    // type === 'create'
    const date        = data.date;
    const action      = data.action;
    const note        = data.note || '';
    const event_id    = data.event_id || String(Date.now());
    const type_action = data.type_action || 'autre';

    if (!date || !action) {
      return ContentService.createTextOutput(JSON.stringify({ status: 'error', message: 'Date et action requis' }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    const newRow   = sheet.getLastRow() + 1;
    const actionCol = COL_MAP[type_action];

    sheet.getRange(newRow, 1).setValue(date);       // A : date

    if (actionCol) {
      sheet.getRange(newRow, actionCol).setValue(action);
      if (note) sheet.getRange(newRow, 15).setValue(note);  // O : commentaires_notes
    } else {
      // Pas de colonne dédiée (réseaux sociaux, autre, contexte…) → tout dans commentaires_notes
      const combined = note ? action + ' — ' + note : action;
      sheet.getRange(newRow, 15).setValue(combined);
    }

    sheet.getRange(newRow, 16).setValue(event_id);  // P : event_id

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

  // Cherche d'abord par event_id (colonne P = 16)
  if (event_id) {
    const idValues = sheet.getRange(2, 16, lastRow - 1, 1).getValues();
    for (let i = 0; i < idValues.length; i++) {
      if (String(idValues[i][0]).trim() === String(event_id).trim()) return i + 2;
    }
  }

  // Repli : date (col A) + description (colonnes B–H ou O)
  if (date && action) {
    const dateKey   = String(date).substring(0, 10);
    const dateVals  = sheet.getRange(2, 1, lastRow - 1, 1).getValues();
    // Cherche dans toutes les colonnes d'action (B à H) + commentaires (O)
    const allCols   = sheet.getRange(2, 1, lastRow - 1, 15).getValues();
    for (let i = 0; i < allCols.length; i++) {
      const rowDate = String(allCols[i][0]).substring(0, 10);
      if (rowDate !== dateKey) continue;
      // Vérifie si l'action correspond à n'importe quelle colonne d'action
      for (let c = 1; c <= 7; c++) {
        if (String(allCols[i][c]).trim() === String(action).trim()) return i + 2;
      }
      // Vérifie la colonne commentaires (O = index 14)
      if (String(allCols[i][14]).trim() === String(action).trim()) return i + 2;
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

function updateEvent(sheet, event_id, date, action, date_new, action_new, note_new, type_action_new) {
  const row = findEventRow(sheet, event_id, date, action);
  if (row === -1) return false;

  if (date_new) sheet.getRange(row, 1).setValue(date_new);

  if (action_new) {
    // Effacer toutes les colonnes d'action de cette ligne
    [2, 3, 4, 5, 6, 7, 8].forEach(col => sheet.getRange(row, col).setValue(''));

    const actionCol = COL_MAP[type_action_new || 'autre'];
    if (actionCol) {
      sheet.getRange(row, actionCol).setValue(action_new);
      sheet.getRange(row, 15).setValue(note_new || '');
    } else {
      // Pas de colonne dédiée → commentaires_notes
      const combined = note_new ? action_new + ' — ' + note_new : action_new;
      sheet.getRange(row, 15).setValue(combined);
    }
  } else {
    sheet.getRange(row, 15).setValue(note_new || '');
  }

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
