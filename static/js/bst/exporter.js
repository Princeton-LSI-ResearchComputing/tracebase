/**
 * Takes the export options defined in the bootstrap table and generates a drop-down list that calls the custom
 * exportAllPages function when clicked.
 * See design in: https://princeton-university.atlassian.net/wiki/x/HIAVH
 * @param {*} exportTypes A javascript object containing export type strings (as keys) to present in the export button's
 * contextual menu and the values are the URLs of the downloads.
 * @returns html
 */
window.generateExportSelect = function (exportTypes) {
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
