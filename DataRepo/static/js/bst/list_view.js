const urlParams = new URLSearchParams(window.location.search);
const djangoCurrentURL = window.location.href.split('?')[0] // {% url request.resolver_match.url_name %} eslint-disable-line no-var

// The defaults only exist for unit testing purposes
var djangoTableID = 'bstlistviewtable' // {{ table_id }} eslint-disable-line no-var
var jqTableID = '#' + djangoTableID
var djangoPageNumber = 1 // {{ page_obj.number }} eslint-disable-line no-var
var djangoLimitDefault = 15 // {{ limit_default }} eslint-disable-line no-var
var djangoLimit = djangoLimitDefault // {{ limit }} eslint-disable-line no-var
var djangoPerPage = djangoLimitDefault // {{ page_obj.paginator.per_page }} eslint-disable-line no-var
var djangoRawTotal = 0 // {{ raw_total }} eslint-disable-line no-var
var djangoTotal = djangoRawTotal // {{ total }} eslint-disable-line no-var

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
 */
function initBST(
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
){
  globalThis.djangoCurrentURL = currentURL;
  globalThis.djangoTableID = tableID;
  globalThis.jqTableID = '#' + tableID;
  globalThis.djangoLimitDefault = limitDefault;
  globalThis.djangoLimit = limit;
  globalThis.djangoPerPage = perPage;
  globalThis.djangoPageNumber = pageNumber;
  globalThis.djangoTotal = total;
  globalThis.djangoRawTotal = rawTotal;

  // Initialize the cookies (basically just the prefix)
  initViewCookies(cookiePrefix)
  if (parseBool(clearCookies)) {
    resetTable();
  }
  if (typeof cookieResets !== "undefined" && cookieResets && cookieResets.length > 0) {
    deleteCookies(cookieResets);
  }

  // Set cookies for the current page and limit that comes from the context and is sent via url params.
  // Everything else is saved in cookies.
  const limitParam = urlParams.get('limit');
  const limitCookie = parseInt(getViewCookie('limit', djangoLimit));
  if (limitParam) {
      setViewCookie('limit', limitParam);
  } else if (limitCookie === 0) {
    // If no limit param was sent and the cookie limit value is 0, set the cookie limit to the default.
    // I.e. only set to unlimited by the URL parameter.
    setViewCookie('limit', djangoLimitDefault);
  } else {
    setViewCookie('limit', djangoLimit);
  }
  setViewCookie('page', djangoPageNumber);

  let collapse = getViewCookie('collapsed', false);
  if (collapse) setCollapse(collapse);

  // Display any warnings received from the server
  displayWarnings(warnings);

  // Set a variable to be able to forgo events from BST during init
  var loading = true;
  $(jqTableID).bootstrapTable({
    onSort: function (orderBy, orderDir) {
      // Sort is just a click, and it appears that sort is not called for each column on load like onColumnSearch
      // is, so we're not going to check 'loading' here.  I was encountering issues with the sort not happening.
      setViewCookie('order-by', orderBy);
      setViewCookie('order-dir', orderDir);
      // BST sorting has 2 issues...
      // 1. BST sort and server side sort sometimes sort differently (c.i.p. imported_timestamp)
      // 2. BST sort completely fails when the number of rows is very large
      // ...so we will always let the sort hit the server to be on the safe side.
      console.log("Sorting by " + orderBy + ", " + orderDir)
      updatePage(1);
    },
    onSearch: function (searchTerm) {
      if (!loading) {
        // NOTE: Turns out that on page load, a global search event is triggered, so we check to see if anything
        // changed before triggering a page update.
        let oldTerm = getViewCookie('search');
        let oldTermDefined = typeof oldTerm === "undefined" || !oldTerm;
        let newTermDefined = typeof searchTerm === "undefined" || !searchTerm;
        if (oldTermDefined !== newTermDefined || (oldTermDefined && newTermDefined && oldTerm !== searchTerm)) {
          setViewCookie('search', searchTerm);
          // No need to hit the server if we're displaying all results. Just let BST do it.
          if (djangoLimit > 0 && djangoLimit < djangoTotal || djangoTotal < djangoRawTotal) {
            updatePage(1);
          }
        }
      }
    },
    onColumnSearch: function (columnName, searchTerm) {
      console.log("Filtering column " + columnName + " with term: " + searchTerm)
      if (!loading) {
        // NOTE: Turns out that on page load, a column search event is triggered, so we check to see if anything
        // changed before triggering a page update.
        let oldTerm = getViewColumnCookie(columnName, 'filter');
        let oldTermDefined = typeof oldTerm !== "undefined" && oldTerm;
        let newTermDefined = typeof searchTerm !== "undefined" && searchTerm;
        if (oldTermDefined !== newTermDefined || (oldTermDefined && newTermDefined && oldTerm !== searchTerm)) {
          setViewColumnCookie(columnName, 'filter', searchTerm);
          // No need to hit the server if we're displaying all results. Just let BST do it.
          if ((djangoLimit > 0 && djangoLimit < djangoTotal) || djangoTotal < djangoRawTotal) {
            updatePage(1);
          }
        }
      }
    },
    onColumnSwitch: function (columnName, visible) {
      updateVisible(visible, columnName);
    },
    onColumnSwitchAll: function (visible) {
      updateVisible(visible);
    },
    onLoadError: function (status, jqXHR) {
      console.error("BootstrapTable Error.  Status: '" + status + "' Data:", jqXHR);
    },
  });
  // Add click listeners on the rows-per-page-option select list items.
  // See DataRepo.widgets.ListViewRowsPerPageSelectWidget.
  let rowsPerPageOptionElems = document.getElementsByName("rows-per-page-option");
  for (let i = 0; i < rowsPerPageOptionElems.length; i++) {
    rowsPerPageOptionElems[i].addEventListener("click", function (event) {
      onRowsPerPageChange($(this).data("value"));
    })
  }
  setTimeout(function () {loading = false}, 2000);
}

/**
 * Displays any warnings from django.
 * @param {*} warningsArray Array of warning strings.
 */
function displayWarnings(warningsArray) {
  if (warningsArray.length > 0) {
    let warningText = "Please note the following warnings that occurred:\n\n";
    for (i=0; i < warningsArray.length; i++) {
      num = i + 1;
      warningText += num + ". " + warningsArray[i] + "\n"
    }
    alert(warningText);
  }
}

/**
 * Retrieves a list of column names.
 * @returns Column names.
 */
function getColumnNames() {
  let columnNames = [];
  $(jqTableID).bootstrapTable('getVisibleColumns').map(function (col) {
    columnNames.push(col.field)
  });
  $(jqTableID).bootstrapTable('getHiddenColumns').map(function (col) {
    columnNames.push(col.field)
  });
  return columnNames
}

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
  def = typeof def === 'boolean' ? def : (typeof def === 'undefined' ? false : def.toLowerCase() === 'true')
  return typeof boolval === 'boolean' ? boolval : (typeof boolval === 'undefined' || boolval === '' ? def : boolval.toLowerCase() === 'true')
}

/**
 * Updates column visibility.
 * @param {*} visible The visible state of the column.
 * @param {*} columnName The column name.
 * @returns Nothing.
 */
function updateVisible(visible, columnName) {
  let columnNames = getColumnNames();
  if (typeof columnName !== "undefined" && columnName) {
    if (columnNames.includes(columnName)) {
      columnNames = [columnName];
    } else if (columnNames.length === 0) {
      console.error(
        "No data-field values could be found.  The table's th tags must have data-field attributes.  ",
        "If they are defined, there must be a problem with the getColumnNames function."
      );
      alert("Error: Unable to save your column visibility selection");
      return
    } else {
      console.error(
        "updateVisible called with invalid data-field:", columnName, "The second argument must match a ",
        "data-field value defined in the table's th tags.  Current data-fields:", columnNames
      );
      alert("Error: Unable to save your column visibility selection");
      return
    }
  }
  for (let i=0; i < columnNames.length; i++) {
    setViewColumnCookie(columnNames[i], 'visible', visible);
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
