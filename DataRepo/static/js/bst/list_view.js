// The defaults only exist for unit testing purposes
const djangoLimitDefault = 15 // {{ limit_default }} eslint-disable-line no-var
const djangoLimit = djangoLimitDefault // {{ limit }} eslint-disable-line no-var
const djangoPerPage = djangoLimitDefault // {{ page_obj.paginator.per_page }} eslint-disable-line no-var
const djangoCurrentURL = window.location.href.split('?')[0] // {% url request.resolver_match.url_name %} eslint-disable-line no-var

// TODO: Add support for multiple tables on the same page.
// See: https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/1577

/**
 * Requests a new page from the server based on the values passed in (or the cookies as defaults).
 * @param {*} page Page number.
 * @param {*} limit Rows per page.
 * @param {*} exportType Whether to export to a file and the file type.
 */
function updatePage (page, limit, exportType) { // eslint-disable-line no-unused-vars
  window.location.href = getPageURL(page, limit, exportType)
}

/**
 * Determines a URL for a new page based on the values passed in (or the cookies as defaults).
 * This is a supporting method for updatePage, mainly for testing purposes.
 * @param {*} page Page number.
 * @param {*} limit Rows per page.
 * @param {*} exportType Whether to export to a file and the file type.
 * @return url - The URL of the new page
 */
function getPageURL (page, limit, exportType) { // eslint-disable-line no-unused-vars
  // Get or set the page and limit cookies
  [page, limit] = updatePageCookies(page, limit)
  // Create the URL
  let url = djangoCurrentURL + '?page=' + page
  if (typeof limit !== 'undefined' && (limit === 0 || limit)) {
    url += '&limit=' + limit
  }
  // Add the export param, if supplied
  if (typeof exportType !== 'undefined' && exportType) {
    url += '&export=' + exportType
  }
  return url
}

/**
 * Updates and returns page cookies.
 * This is a supporting method for updatePage, mainly for testing purposes.
 * @param {*} page Page number.
 * @param {*} limit Rows per page.
 * @return page, limit
 */
function updatePageCookies (page, limit) { // eslint-disable-line no-unused-vars
  // Determine the limit
  if (typeof limit === 'undefined' || (limit !== 0 && !limit)) {
    limit = getViewCookie('limit', djangoLimit) // eslint-disable-line no-undef
  }
  // Determine the page
  if (typeof page === 'undefined' || !page) {
    page = getViewCookie('page', 1) // eslint-disable-line no-undef
  }
  return [page, limit]
}

/**
 * Updates the rows per page and requests a new page from the server.
 * @param {*} numRows Number of rows per page to request.
 */
function onRowsPerPageChange (numRows) { // eslint-disable-line no-unused-vars
  updateRowsPerPage(numRows)
  updatePage()
}

/**
 * Updates the cookies relating to a rows per page change.
 * This is a supporting method for onRowsPerPageChange, mainly for testing purposes.
 * @param {*} numRows Number of rows per page to request.
 */
function updateRowsPerPage (numRows) { // eslint-disable-line no-unused-vars
  let oldLimit = parseInt(getViewCookie('limit', djangoLimit)) // eslint-disable-line no-undef
  if (isNaN(oldLimit)) {
    oldLimit = djangoPerPage
  }
  let curPage = parseInt(getViewCookie('page')) // eslint-disable-line no-undef
  if (isNaN(curPage)) {
    curPage = 1
  }
  const curOffset = (curPage - 1) * oldLimit + 1
  let closestPage = curPage
  setViewCookie('limit', numRows) // eslint-disable-line no-undef
  if (numRows !== 0) {
    closestPage = parseInt(curOffset / numRows) + 1
    // Reset the page
    setViewCookie('page', closestPage) // eslint-disable-line no-undef
  }
}

/**
 * Advances to a different page.
 * @param {*} page Page number.
 */
function onPageChange (page) { // eslint-disable-line no-unused-vars
  updatePageNum(page)
  updatePage()
}

/**
 * Updates the cookies relating to a page change.
 * This is a supporting method for onPageChange, mainly for testing purposes.
 * @param {*} page Page number.
 */
function updatePageNum (page) { // eslint-disable-line no-unused-vars
  setViewCookie('page', page) // eslint-disable-line no-undef
}

/**
 * Clears cookies and requests a reinitialized page from the server.
 */
function resetTable () { // eslint-disable-line no-unused-vars
  deleteViewCookies() // eslint-disable-line no-undef
  updatePage()
}

/**
 * Clears all view cookies.
 * This is a supporting method for resetTable, mainly for testing purposes.
 */
function resetTableCookies () { // eslint-disable-line no-unused-vars
  deleteViewCookies() // eslint-disable-line no-undef
  updatePageCookies(1, djangoLimitDefault)
}

/**
 * Takes an undefined, boolean, or string value (and a default) and returns the equivalent boolean.
 * NOTE: An empty string (like from a cookie) for either def or boolval results in false.
 * @param {*} boolval (Optional[boolean|string]) A boolean or string containing "true" or "false" (case insensitive).
 * @param {*} def (Optional[boolean|string]) [false] The default, in case boolval is undefined.
 * @returns The boolean equivalent of the parsed boolval (or the default if undefined).
 */
function parseBool (boolval, def) {
  if (typeof def !== 'boolean') {
    if (typeof def === 'undefined') {
      def = false
    } else {
      // The default default is false (e.g. an empty string would make this set false)
      def = def.toLowerCase() === 'true'
    }
  }
  if (typeof boolval !== 'boolean') {
    // If the type is undefined or the string is empty (which is the value of an "undefined cookie"), set the default
    if (typeof boolval === 'undefined' || boolval === '') {
      boolval = def
    } else {
      boolval = boolval.toLowerCase() === 'true'
    }
  }
  return boolval
}

/**
 * Obtains the current collapsed state according to the "collapsed" cookie and toggles it to the opposite state.
 * Assumes that the initial collapsed state is true if the cookie is not set.
 * TODO: Instead of assuming the current collapsed state, search the DOM for evidence of the state.
 */
function toggleCollapse () {
  // Initial collapsed state (if not set) is assumed to be collapsed = true, so the toggle will be the opposite.
  const collapsed = parseBool(getViewCookie('collapsed'), true) // eslint-disable-line no-undef
  const collapse = !collapsed
  setCollapse(collapse)
}

/**
 * Modify the table cell (td) and its contained br elements in the DOM to either collapse content to a single unwrapped
 * line or expand to a wrapped line.
 * @param {*} collapse (boolean) Set to true to make every table cell content collapse to a single line.  Set to false
 * to expand/wrap the table cell contents.
 */
function setCollapse (collapse) {
  if (typeof collapse === 'undefined') collapse = true
  const cellElems = document.getElementsByClassName('table-cell')
  for (let i = 0; i < cellElems.length; i++) {
    const cellElem = cellElems[i]
    if (collapse) cellElem.classList.add('nobr')
    else cellElem.classList.remove('nobr')
  }
  const wrapElems = document.getElementsByClassName('cell-wrap')
  for (let i = 0; i < wrapElems.length; i++) {
    const wrapElem = wrapElems[i]
    if (collapse) wrapElem.classList.add('d-none')
    else wrapElem.classList.remove('d-none')
  }
  // Update the icon to the opposite to indicate the action of the button
  setCollapseIcon(!collapse)
  // Update the state in the collapsed cookie
  setViewCookie('collapsed', collapse) // eslint-disable-line no-undef
}

/**
 * Set the collapse icon (for the collapse button) to show the action clicking it will perform.  If the action is to
 * collapse, show a collapse button.  If the action is the expand, show an expand button.
 * @param {*} collapse (boolean) Set to true to make the collapse button icon indicate a collapse action.  Set to false
 * to indicate an expand action.
 */
function setCollapseIcon (collapse) {
  const addIconName = collapse ? 'bi-arrows-collapse' : 'bi-arrows-expand'
  const removeIconName = !collapse ? 'bi-arrows-collapse' : 'bi-arrows-expand'

  // Get the collapse button icon
  const iconElem = document.querySelectorAll("button[name='btnCollapse'] > .bi")[0]
  // TODO: The custom button name is not table-specific, which means this does not support multiple table toolbars on
  // the same page.  See https://github.com/Princeton-LSI-ResearchComputing/tracebase/issues/1577

  // Replace the previous icon with the current one
  iconElem.classList.remove(removeIconName)
  iconElem.classList.add(addIconName)
}

/**
 * Initializes settings for custom buttons in the BST toolbar, including a clear button to clear out cookies and a
 * custom export dropdown button..
 * @returns Settings object for BST.
 */
function customButtonsFunction () { // eslint-disable-line no-unused-vars
  return {
    btnClear: {
      text: 'Reset Page to default settings',
      icon: 'bi-house',
      event: function btnClearTableSettings () {
        resetTable()
      },
      attributes: {
        title: 'Restore default sort, filter, search, column visibility, and pagination'
      }
    },
    btnCollapse: {
      text: 'Toggle line-wrap in all table cells',
      icon: !(getViewCookie('collapsed', 'true') === 'true') ? 'bi-arrows-collapse' : 'bi-arrows-expand', // eslint-disable-line no-undef
      event: function btnToggleCollapse () {
        toggleCollapse()
      },
      attributes: {
        title: 'Toggle line-wrap in all table cells'
      }
    }
  }
}
