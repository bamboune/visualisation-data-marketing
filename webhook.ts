function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const sheet = SpreadsheetApp.openById('1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro').getSheetByName('evenements_marketing');
    const type = data.type || 'create';

    if (type === 'delete') {
      const deleted = deleteEvent(sheet, data.event_id, data.date, data.action);
      if (deleted) triggerWorkflow();
      return ContentService.createTextOutput(JSON.stringify({ status: deleted ? 'success' : 'not_found' }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    if (type === 'update') {
      const updated = updateEvent(sheet, data.event_id, data.date, data.action, data.date_new, data.action_new, data.note_new);
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

    const newRow = sheet.getLastRow() + 1;
    sheet.getRange(newRow, 1).setValue(date);

    // Écrire dans la colonne correspondant au type d'action
    const actionColNames = [
      'rabais_promos', 'lancement_produits_ateliers', 'bis_alertes_back_in_stock',
      'infolettre', 'push_notif', 'billet_blogue', 'reseaux_sociaux',
    ];
    if (actionColNames.includes(type_action)) {
      const col = getColByHeader(sheet, type_action);
      if (col > 0) {
        sheet.getRange(newRow, col).setValue(action);
      } else {
        sheet.getRange(newRow, 3).setValue(action); // colonne générique si header introuvable
      }
    } else {
      sheet.getRange(newRow, 3).setValue(action); // 'autre' ou 'contexte'
    }

    sheet.getRange(newRow, 15).setValue(note);
    sheet.getRange(newRow, 16).setValue(event_id);

    triggerWorkflow();

    return ContentService.createTextOutput(JSON.stringify({ status: 'success' }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function getColByHeader(sheet, headerName) {
  const headers = sheet.getRange(2, 1, 1, sheet.getLastColumn()).getValues()[0];
  const idx = headers.findIndex(h => String(h).trim().toLowerCase() === headerName.toLowerCase());
  return idx >= 0 ? idx + 1 : -1;
}

function findEventRow(sheet, event_id, date, action) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 3) return -1;

  // Cherche d'abord par event_id (colonne 16)
  if (event_id) {
    const idValues = sheet.getRange(3, 16, lastRow - 2, 1).getValues();
    for (let i = 0; i < idValues.length; i++) {
      if (String(idValues[i][0]).trim() === String(event_id).trim()) {
        return i + 3;
      }
    }
  }

  // Repli : date (col 1) + action (col 3)
  if (date && action) {
    const dateValues   = sheet.getRange(3, 1, lastRow - 2, 1).getValues();
    const actionValues = sheet.getRange(3, 3, lastRow - 2, 1).getValues();
    const dateKey = String(date).substring(0, 10);
    for (let i = 0; i < dateValues.length; i++) {
      const rowDate   = String(dateValues[i][0]).substring(0, 10);
      const rowAction = String(actionValues[i][0]).trim();
      if (rowDate === dateKey && rowAction === String(action).trim()) {
        return i + 3;
      }
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

function updateEvent(sheet, event_id, date, action, date_new, action_new, note_new) {
  const row = findEventRow(sheet, event_id, date, action);
  if (row === -1) return false;
  if (date_new)    sheet.getRange(row, 1).setValue(date_new);
  if (action_new)  sheet.getRange(row, 3).setValue(action_new);
  sheet.getRange(row, 15).setValue(note_new || '');
  return true;
}

function triggerWorkflow() {
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  const url = 'https://api.github.com/repos/bamboune/visualisation-data-marketing/actions/workflows/update.yml/dispatches';
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
