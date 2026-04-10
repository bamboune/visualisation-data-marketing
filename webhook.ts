function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const date      = data.date;
    const action    = data.action;
    const categorie = data.categorie || 'marketing';
    const note      = data.note || '';

    if (!date || !action) {
      return ContentService.createTextOutput(JSON.stringify({ status: 'error', message: 'Date et action requis' }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    const sheet = SpreadsheetApp.openById('1vYqgbiiYDnJONtFCx11LkTdPUM14fCf0IG1L7P2O4ro').getSheetByName('evenements_marketing');

    const lastRow = sheet.getLastRow();
    const newRow = lastRow + 1;

    sheet.getRange(newRow, 1).setValue(date);  // colonne A : date
    if (categorie === 'contexte') {
      // Événement contexte → va dans les notes (col O)
      sheet.getRange(newRow, 15).setValue(action + (note ? ' — ' + note : ''));
    } else {
      // Événement marketing → va dans lancement_produits_ateliers (col C)
      sheet.getRange(newRow, 3).setValue(action);
      sheet.getRange(newRow, 15).setValue(note);
    }

    // === AJOUT : déclencher le workflow GitHub ===
    triggerWorkflow();

    return ContentService.createTextOutput(JSON.stringify({ status: 'success' }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ status: 'error', message: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// === NOUVELLE FONCTION À AJOUTER ===
function triggerWorkflow() {
  const token = PropertiesService.getScriptProperties().getProperty('GITHUB_TOKEN');
  const owner = 'bamboune';
  const repo = 'visualisation-data-marketing';
  const workflow_id = 'update.yml';

  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow_id}/dispatches`;

  const payload = {
    ref: 'main'   // ou 'master' si ta branche par défaut s'appelle master
  };

  const options = {
    method: 'post',
    headers: {
      'Authorization': `token ${token}`,
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  const response = UrlFetchApp.fetch(url, options);
  Logger.log('Workflow déclenché, code HTTP : ' + response.getResponseCode());
}