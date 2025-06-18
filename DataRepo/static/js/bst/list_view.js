const urlParams = new URLSearchParams(window.location.search)
const djangoCurrentURL = window.location.href.split('?')[0] // {% url request.resolver_match.url_name %} eslint-disable-line no-var

// The defaults only exist for unit testing purposes
// NOTE: These are intentionally `var`s, not `const`s.  Otherwise, the code will fail, because these must be able to change.
var djangoTableID = 'bstlistviewtable' // eslint-disable-line no-var
var jqTableID = '#' + djangoTableID // eslint-disable-line no-var
var djangoPageNumber = 1 // eslint-disable-line no-var
var djangoLimitDefault = 15 // eslint-disable-line no-var
var djangoLimit = djangoLimitDefault // eslint-disable-line no-var
var djangoPerPage = djangoLimitDefault // eslint-disable-line no-var
var djangoRawTotal = 0 // eslint-disable-line no-var
var djangoTotal = djangoRawTotal // eslint-disable-line no-var

var sortCookieName = 'sort' // eslint-disable-line no-var, no-unused-vars
var ascCookieName = 'asc' // eslint-disable-line no-var, no-unused-vars
var searchCookieName = 'search' // eslint-disable-line no-var, no-unused-vars
var filterCookieName = 'filter' // eslint-disable-line no-var, no-unused-vars
var visibleCookieName = 'visible' // eslint-disable-line no-var
var limitCookieName = 'limit' // eslint-disable-line no-var
var pageCookieName = 'page' // eslint-disable-line no-var

/**
 * This function exists solely for testing purposes
 */
function initGlobalDefaults () { // eslint-disable-line no-unused-vars
  globalThis.djangoCurrentURL = window.location.href.split('?')[0]
  globalThis.djangoTableID = 'bstlistviewtable'
  globalThis.jqTableID = '#' + djangoTableID
  globalThis.djangoLimitDefault = 15
  globalThis.djangoLimit = djangoLimitDefault
  globalThis.djangoPerPage = djangoLimitDefault
  globalThis.djangoPageNumber = 1
  globalThis.djangoRawTotal = 0
  globalThis.djangoTotal = djangoRawTotal
  globalThis.sortCookieName = 'sort'
  globalThis.ascCookieName = 'asc'
  globalThis.searchCookieName = 'search'
  globalThis.filterCookieName = 'filter'
  globalThis.visibleCookieName = 'visible'
  globalThis.limitCookieName = 'limit'
  globalThis.pageCookieName = 'page'
}

/**
 * Initializes the bootstrap table functionality.
 * @param {*} limit Number of rows per page when the page loaded.
 * @param {*} limitDefault Default number of rows per page, prescribed by the view.
 * @param {*} tableID ID of the table element that is a bootstrap table.
 * @param {*} cookiePrefix Prefix of cookie names specific to the page.
 * @param {*} pageNumber The current page number.
 * @param {*} perPage Number of rows per page when the page loaded.  TODO: delete & replace with limit.
 * @param {*} total The total number of results/rows given the current searc/filter.
 * @param {*} rawTotal The total unfiltered number of results.
 * @param {*} currentURL The current URL of the page.
 * @param {*} warnings A list of warnings from django.
 * @param {*} cookieResets A list of cookie names to reset from django.  (Not the whole cookie name, just the last bit, e.g. 'sortcol'.)
 * @param {*} clearCookies A boolean represented as a string, e.g. 'false'.
 * @param {*} sortCookieName name of the sort cookie
 * @param {*} ascCookieName name of the asc cookie
 * @param {*} searchCookieName name of the search cookie
 * @param {*} filterCookieName name of the filter cookie
 * @param {*} visibleCookieName name of the visible cookie
 * @param {*} limitCookieName name of the limit cookie
 * @param {*} pageCookieName name of the page cookie
 */
function initBST ( // eslint-disable-line no-unused-vars
  limit,
  limitDefault,
  tableID,
  cookiePrefix,
  pageNumber,
  perPage,
  total,
  rawTotal,
  currentURL,
  warnings,
  cookieResets,
  clearCookies,
  sortCookieName,
  ascCookieName,
  searchCookieName,
  filterCookieName,
  visibleCookieName,
  limitCookieName,
  pageCookieName
) {
  globalThis.djangoCurrentURL = currentURL
  globalThis.djangoTableID = tableID
  globalThis.jqTableID = '#' + tableID
  globalThis.djangoLimitDefault = parseInt(limitDefault)
  globalThis.djangoLimit = parseInt(limit)
  globalThis.djangoPerPage = parseInt(perPage)
  globalThis.djangoPageNumber = parseInt(pageNumber)
  globalThis.djangoTotal = parseInt(total)
  globalThis.djangoRawTotal = parseInt(rawTotal)
  globalThis.sortCookieName = sortCookieName
  globalThis.ascCookieName = ascCookieName
  globalThis.searchCookieName = searchCookieName
  globalThis.filterCookieName = filterCookieName
  globalThis.visibleCookieName = visibleCookieName
  globalThis.limitCookieName = limitCookieName
  globalThis.pageCookieName = pageCookieName

  // Initialize the cookies (basically just the prefix)
  initViewCookies(cookiePrefix) // eslint-disable-line no-undef
  if (parseBool(clearCookies)) {
    deleteViewCookies() // eslint-disable-line no-undef
  } else if (typeof cookieResets !== 'undefined' && cookieResets && cookieResets.length > 0) {
    deleteViewCookies(cookieResets) // eslint-disable-line no-undef
  }

  // Set cookies for the current page and limit that comes from the context and is sent via url params.
  // Everything else is saved in cookies.
  const limitParam = urlParams.get(limitCookieName)
  const limitCookie = getViewCookie(limitCookieName, djangoLimit) // eslint-disable-line no-undef
  if (limitParam) {
    // The 'limit' URL parameter overrides cookie and context versions
    setViewCookie(limitCookieName, limitParam) // eslint-disable-line no-undef
  } else if (typeof limitCookie !== 'undefined' && parseInt(limitCookie) === 0) {
    // The 'limit' is never allowed to be set to 0 (i.e. 'unlimited') by a cookie.
    // This is so that if the user requests too many rows per page and hits a timeout, they don't get locked out.
    setViewCookie(limitCookieName, djangoLimitDefault) // eslint-disable-line no-undef
  } else {
    // Finally, if there's no URL param and no cookie, set the 'limit' from the context
    setViewCookie(limitCookieName, djangoLimit) // eslint-disable-line no-undef
  }
  setViewCookie(pageCookieName, djangoPageNumber) // eslint-disable-line no-undef

  // Set a variable to be able to forgo events from BST during init
  let loading = true
  $(jqTableID).bootstrapTable({ // eslint-disable-line no-undef
    onSort: function (orderBy, orderDir) {
      // Sort is just a click, and it appears that sort is not called for each column on load like onColumnSearch
      // is, so we're not going to check 'loading' here.  I was encountering issues with the sort not happening.
      setViewCookie(sortCookieName, orderBy) // eslint-disable-line no-undef
      setViewCookie(ascCookieName, orderDir.toLowerCase().startsWith('a')) // eslint-disable-line no-undef
      // BST sorting has 2 issues...
      // 1. BST sort and server side sort sometimes sort differently (c.i.p. imported_timestamp)
      // 2. BST sort completely fails when the number of rows is very large
      // ...so we will always let the sort hit the server to be on the safe side.
      console.log('Sorting by ' + orderBy + ', ' + orderDir)
      updatePage(1)
    },
    onSearch: function (searchTerm) {
      if (!loading) {
        // NOTE: Turns out that on page load, a global search event is triggered, so we check to see if anything
        // changed before triggering a page update.
        const oldTerm = getViewCookie(searchCookieName) // eslint-disable-line no-undef
        const oldTermDefined = typeof oldTerm === 'undefined' || !oldTerm
        const newTermDefined = typeof searchTerm === 'undefined' || !searchTerm
        if (oldTermDefined !== newTermDefined || (oldTermDefined && newTermDefined && oldTerm !== searchTerm)) {
          setViewCookie(searchCookieName, searchTerm) // eslint-disable-line no-undef
          // No need to hit the server if we're displaying all results. Just let BST do it.
          if ((djangoLimit > 0 && djangoLimit < djangoTotal) || djangoTotal < djangoRawTotal) {
            updatePage(1)
          }
        }
      }
    },
    onColumnSearch: function (columnName, searchTerm) {
      console.log('Filtering column ' + columnName + ' with term: ' + searchTerm)
      if (!loading) {
        // NOTE: Turns out that on page load, a column search event is triggered, so we check to see if anything
        // changed before triggering a page update.
        const oldTerm = getViewColumnCookie(columnName, filterCookieName) // eslint-disable-line no-undef
        const oldTermDefined = typeof oldTerm !== 'undefined' && oldTerm
        const newTermDefined = typeof searchTerm !== 'undefined' && searchTerm
        if (oldTermDefined !== newTermDefined || (oldTermDefined && newTermDefined && oldTerm !== searchTerm)) {
          setViewColumnCookie(columnName, filterCookieName, searchTerm) // eslint-disable-line no-undef
          // No need to hit the server if we're displaying all results. Just let BST do it.
          if ((djangoLimit > 0 && djangoLimit < djangoTotal) || djangoTotal < djangoRawTotal) {
            updatePage(1)
          }
        }
      }
    },
    onColumnSwitch: function (columnName, visible) {
      updateVisible(visible, columnName)
    },
    onColumnSwitchAll: function (visible) {
      updateVisible(visible)
    },
    onLoadError: function (status, jqXHR) {
      console.error("BootstrapTable Error.  Status: '" + status + "' Data:", jqXHR)
    }
  })

  // TODO: Add collapsed and collapsedDefault as arguments and only call setCollapse if they differ
  const collapse = parseBool(getViewCookie('collapsed', true)) // eslint-disable-line no-undef
  setCollapse(collapse)

  // Display any warnings received from the server
  displayWarnings(warnings)

  setTimeout(function () { loading = false }, 2000)
}

/**
 * Displays any warnings from django.
 * @param {*} warningsArray Array of warning strings.
 */
function displayWarnings (warningsArray) {
  if (typeof warningsArray !== 'undefined' && warningsArray.length > 0) {
    let warningText = 'Please note the following warnings that occurred:\n\n'
    for (let i = 0; i < warningsArray.length; i++) {
      const num = i + 1
      warningText += num + '. ' + warningsArray[i] + '\n'
    }
    alert(warningText) // eslint-disable-line no-undef
  }
}

/**
 * Retrieves a list of column names (obtained from the data-field attribute of every th element).
 * Bootstrap table provides alternate methods of retrieving the fields, but they are not in order and are limited to
 * only either the visible or hidden ones.
 * @returns Column names.
 */
function getColumnNames () {
  const columnNames = []
  const headerCells = document.querySelectorAll(`${jqTableID} th`)
  for (let i = 0; i < headerCells.length; i++) {
    columnNames.push(headerCells[i].dataset.field)
  }
  return columnNames
}

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
  // TODO: Add global variable for export URL parameter name, which is stored in a variable in BSTClientInterface
  let url = djangoCurrentURL + '?' + pageCookieName + '=' + page
  if (typeof limit !== 'undefined' && (limit === 0 || limit)) {
    url += '&' + limitCookieName + '=' + limit
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
    limit = getViewCookie(limitCookieName, djangoLimit) // eslint-disable-line no-undef
  }
  // Determine the page
  if (typeof page === 'undefined' || !page) {
    page = getViewCookie(pageCookieName, 1) // eslint-disable-line no-undef
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
  let oldLimit = parseInt(getViewCookie(limitCookieName, djangoLimit)) // eslint-disable-line no-undef
  if (isNaN(oldLimit)) {
    oldLimit = djangoPerPage
  }
  let curPage = parseInt(getViewCookie(pageCookieName)) // eslint-disable-line no-undef
  if (isNaN(curPage)) {
    curPage = 1
  }
  const curOffset = (curPage - 1) * oldLimit + 1
  let closestPage = curPage
  setViewCookie(limitCookieName, numRows) // eslint-disable-line no-undef
  if (numRows !== 0) {
    closestPage = parseInt(curOffset / numRows) + 1
    // Reset the page
    setViewCookie(pageCookieName, closestPage) // eslint-disable-line no-undef
  }
}

/**
 * Clears cookies and requests a reinitialized page from the server.
 */
function resetTable () { // eslint-disable-line no-unused-vars
  deleteViewCookies() // eslint-disable-line no-undef
  updatePage(1, djangoLimitDefault)
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
 * Updates column visibility in the cookies.  Bootstrap table handles the actual visibility of the column, but setting
 * the cookie here assures that the visibility is remembered for the next server (e.g. page) request.
 * @param {*} visible The visible state of the column.
 * @param {*} columnName Optional column name.  When not provided, every column's visibility is set to visible.
 * @returns Nothing.
 */
function updateVisible (visible, columnName) {
  const columnNames = getColumnNames()
  if (typeof columnName !== 'undefined' && columnName) {
    if (columnNames.includes(columnName)) {
      setViewColumnCookie(columnName, visibleCookieName, visible) // eslint-disable-line no-undef
    } else if (columnNames.length === 0) {
      console.error('No th data-field attributes found.')
      alert('Error: Unable to save your column visibility selection') // eslint-disable-line no-undef
    } else {
      console.error(
        "Column '" + columnName.toString() + "' not found.  The second argument must match a th data-field attribute.  " +
        'Current data-fields: [' + columnNames.toString() + ']'
      )
      alert('Error: Unable to save your column visibility selection') // eslint-disable-line no-undef
    }
  } else {
    // When a columnName is not provided, set all columns' visibility
    for (let i = 0; i < columnNames.length; i++) {
      setViewColumnCookie(columnNames[i], visibleCookieName, visible) // eslint-disable-line no-undef
    }
  }
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
