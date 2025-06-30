const pageParamName = 'page' // eslint-disable-line no-var
const limitParamName = 'limit' // eslint-disable-line no-var
var djangoLimit = 15 // eslint-disable-line no-var

/**
 * Initializes rows per page functionality.
 * @param {*} optionElemName (str) The name of all of the select list option elements (same for each).
 * @param {*} pageParamName (str) The name of the page URL parameter
 * @param {*} limitParamName (str) The name of the rows per page URL parameter
 */
function initPaginator ( // eslint-disable-line no-unused-vars
  optionElemName,
  pageParamName,
  limitParamName,
  limit
) {
  if (typeof pageParamName !== 'undefined' && pageParamName) globalThis.pageParamName = pageParamName
  if (typeof limitParamName !== 'undefined' && limitParamName) globalThis.limitParamName = limitParamName
  if (typeof djangoLimit !== 'undefined' && djangoLimit) globalThis.djangoLimit = limit

  // Add click listeners on the rows-per-page-option select list items.
  // See DataRepo.widgets.bst.BSTRowsPerPageSelectWidget.
  const rowsPerPageOptionElems = document.getElementsByName(optionElemName)
  for (let i = 0; i < rowsPerPageOptionElems.length; i++) {
    rowsPerPageOptionElems[i].addEventListener('click', function (event) {
      onRowsPerPageChange($(this).data('value')) // eslint-disable-line no-undef
    })
  }
}

/**
 * Prompts the user for a page number to jump to.
 * @param {*} curPage The current page (for autofill into the prompt).
 * @param {*} numPages The total number of pages to ensure valid input.
 */
function askForPage (curPage, numPages) { // eslint-disable-line no-unused-vars
  let valid = false
  let canceled = false
  let errmsg = ''
  let newpagenum = curPage
  while (!valid) {
    const newpagestr = prompt(errmsg + 'Enter a page number between 1 and ' + numPages + ':', curPage); // eslint-disable-line no-undef
    [newpagenum, valid, canceled, errmsg] = validatePageNum(newpagestr, numPages)
  }
  if (canceled || typeof newpagenum === 'undefined' || !newpagenum) {
    newpagenum = curPage
  }
  if (newpagenum !== curPage) {
    const url = '?' + pageParamName + '=' + newpagenum + '&' + limitParamName + '=' + djangoLimit
    window.location.href = url
  }
}

/**
 * Validate a user-entered page number given an entered page number string and the number of available pages
 * @param {*} newPageStr (str) User-entered page number from a prompt.
 * @param {*} numPages (int) Number of pages to choose from.
 * @returns [newpagenum, valid, canceled, errmsg]
 */
function validatePageNum (newPageStr, numPages) { // eslint-disable-line no-unused-vars
  let newpagenum
  let valid = false
  let canceled = false
  let errmsg = ''
  if (typeof newPageStr === 'undefined' || !newPageStr) {
    canceled = true
    valid = true
  } else {
    newpagenum = parseInt(newPageStr)
    if (isNaN(newpagenum)) {
      newpagenum = undefined
      errmsg = 'Error: [' + newPageStr + '] is not an integer.\n'
    } else if (newpagenum < 1) {
      newpagenum = undefined
      errmsg = 'Error: [' + newPageStr + '] must be greater than 0.\n'
    } else if (newpagenum > numPages) {
      newpagenum = undefined
      errmsg = `Error: [${newPageStr}] must be less than or equal to the number of pages: [${numPages}].\n`
    } else {
      valid = true
    }
  }
  return [newpagenum, valid, canceled, errmsg]
}
