// The defaults only exist for unit testing purposes
const djangoLimit = 15 // {{ limit }} eslint-disable-line no-var
const djangoLimitDefault = 15 // {{ limit_default }} eslint-disable-line no-var
const djangoPerPage = 15 // {{ page_obj.paginator.per_page }} eslint-disable-line no-var
const djangoCurrentURL = window.location.href.split('?')[0] // {% url request.resolver_match.url_name %} eslint-disable-line no-var

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
