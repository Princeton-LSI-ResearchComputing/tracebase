var exportTypes = [] // eslint-disable-line no-var
var exportDataElemName = 'export_data' // eslint-disable-line no-var
var exportTypeElemName = 'export_type' // eslint-disable-line no-var
var exportFunc = 'exportAllPages' // eslint-disable-line no-var
var notExported = [] // eslint-disable-line no-var

// To use this code, static/js/cookies.js must be imported.

/**
 * Initializes this package.
 * @param {*} exportTypes A list of export type strings to present in the export button's contextual menu.
 */
function initExporter (
  exportTypesElemName,
  exportDataElemName,
  exportFileName,
  exportFileType,
  djangoTableID,
  exportFunc,
  notExportedElemName
) {
  console.log(exportTypesElemName)
  const exportTypesElem = document.getElementById(exportTypesElemName);
  const exportTypes = JSON.parse(exportTypesElem.textContent);
  const notExportedElem = document.getElementById(notExportedElemName);
  const notExported = JSON.parse(notExportedElem.textContent);

  globalThis.exportTypes = exportTypes
  globalThis.exportFunc = exportFunc

  if (typeof exportDataElemName !== 'undefined' && exportDataElemName) {
    globalThis.exportDataElemName = exportDataElemName
  }

  const exportDataElem = document.getElementById(exportDataElemName);

  // If there is download data, trigger the download
  if (typeof exportDataElem !== 'undefined' && exportDataElem) {
    browserDownloadBase64( // eslint-disable-line no-undef
      exportFileName,
      exportDataElem.innerHTML,
      exportFileType
    );
  }

  checkBuiltinExport(djangoTableID);

  var exportObject = {
    exportSelectHtml: generateExportSelect(exportTypes),
    exportFallbackFunc: exportOnePage,
    notExported: notExported
  }

  return exportObject
}

/**
 * Takes the export options defined in the bootstrap table and generates a drop-down list that calls the custom
 * exportAllPages function when clicked.
 * @param {*} exportTypes A list of export type strings to present in the export button's contextual menu.
 * @returns html
 */
function generateExportSelect (exportTypes) {
  let html = `              <div class="btn-group">
                    <button type="button"
                            class="btn btn-primary dropdown-toggle"
                            data-bs-toggle="dropdown"
                            aria-expanded="false">
                        <i class="bi bi-download"></i>
                    </button>
                    <ul class="dropdown-menu">\n`
  for (let i=0; i < exportTypes.length; i++) {
    let val = exportTypes[i];
    html += `                        <li><a class="dropdown-item" onclick="${exportFunc}('${val}')">${val}</a></li>\n`;
  }
  html += `                    </ul>
              </div>\n`;
  return html
}

/**
 * Validates the BST export settings.  With this server-side pagination, the bootstrap table's data-show-export
 * attribute must be false.
 */
function checkBuiltinExport () {
  let tableElem = document.getElementById(djangoTableID);
  if (tableElem.hasAttribute('data-show-export') && $('#' + djangoTableID).data("show-export"))
    alert(
      "ERROR: Bootstrap Table's builtin data-show-export must be false to support server-side data.  Otherwise, only 1 "
      + 'page of data will be exported.'
    );
}

/**
 * Fallback export behavior.  If the custom export dropdown button is eliminated, this kicks in to tell the user how to
 * download all results.  If the user decides they want all results, they are instructed to select all rows per page and
 * an error is thrown to stop bootstrap from doing an export of 1 page of data.
 */
function exportOnePage (page, limit, total) {
  if (typeof limit === 'undefined' || !limit) {
    limit = 1
  }
  if (typeof total === 'undefined' || !total) {
    total = 2
  }
  if (typeof page === 'undefined' || !page) {
    page = 'unknown'
  }
  if (limit !== 0 && limit < total) {
    if (
      confirm(
          "Download 1 page?\n\nTo retrieve all data, cancel and select 'ALL' from the rows per page select list "
          + "below and try again."
      )
    ) {
      console.log("Downloading page " + page.toString() + ".");
    } else {
      throw new Error("Canceling download");
    }
  }
}
