/* eslint-env browser */
/* global $ */

var exportTypes = [] // eslint-disable-line no-var

/**
 * Initializes this package.
 * See design in: https://princeton-university.atlassian.net/wiki/x/IgAcH
 * @param {*} exportTypes A list of export type strings to present in the export button's contextual menu.
 * @param {*} djangoTableID The ID of the Django table.
 * @returns html for the export select list.
 */
window.initExporter = function (
  exportTypesElemName,
  djangoTableID
) {
  const exportTypesElem = document.getElementById(exportTypesElemName)
  globalThis.exportTypes = JSON.parse(exportTypesElem.textContent)

  checkBuiltinExport(djangoTableID)

  return generateExportSelect(exportTypes)
}

/**
 * Takes the export options defined in the bootstrap table and generates a drop-down list that calls the custom
 * exportAllPages function when clicked.
 * See design in: https://princeton-university.atlassian.net/wiki/x/HIAVH
 * @param {*} exportTypes A javascript object containing export type strings (as keys) to present in the export button's
 * contextual menu and the values are the URLs of the downloads.
 * @returns html
 */
function generateExportSelect (exportTypes) {
  let html = `
              <div class="btn-group">
                    <button type="button"
                            class="btn btn-primary dropdown-toggle"
                            data-bs-toggle="dropdown"
                            aria-expanded="false">
                        <i class="bi bi-download"></i>
                    </button>
                    <ul class="dropdown-menu">\n`
  for (let i = 0; i < exportTypes.length; i++) {
    const name = exportTypes[i].name
    const url = exportTypes[i].url
    // NOTE: Clicking this link passes along all the cookies, even though it's a different view/url
    html += `                        <li><a href="${url}" class="dropdown-item">${name}</a></li>\n`
  }
  html += `                    </ul>
              </div>\n`
  return html
}

/**
 * Validates the BST export settings.  With this server-side pagination, the bootstrap table's data-show-export
 * attribute must be false.
 * See design in: https://princeton-university.atlassian.net/wiki/x/IgAcH
 * @param {*} djangoTableID The ID of the Django table.
 * @returns void
 */
function checkBuiltinExport (djangoTableID) {
  const tableElem = document.getElementById(djangoTableID)
  if (tableElem.hasAttribute('data-show-export') && $('#' + djangoTableID).data('show-export')) {
    alert(
      "ERROR: Bootstrap Table's builtin data-show-export must be false to support server-side data.  Otherwise, only " +
      '1 page of data will be exported.'
    )
  }
}
