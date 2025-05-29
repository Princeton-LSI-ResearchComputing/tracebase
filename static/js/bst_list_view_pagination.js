const urlParams = new URLSearchParams(window.location.search);
const filterSelectOptions = {};
var djangoLimit = null // {{ limit }} eslint-disable-line no-var
var djangoLimitDefault = null // {{ limit_default }} eslint-disable-line no-var
var djangoTableID = null // {{ table_id }} eslint-disable-line no-var
var djangoCookiePrefix = null // {{ cookie_prefix }} eslint-disable-line no-var
var djangoPageNumber = null // {{ page_obj.number }} eslint-disable-line no-var
var djangoPerPage = null // {{ page_obj.paginator.per_page }} eslint-disable-line no-var
var djangoTotal = null // {{ total }} eslint-disable-line no-var
var djangoRawTotal = null // {{ raw_total }} eslint-disable-line no-var
var djangoCurrentURL = null // {% url request.resolver_match.url_name %} eslint-disable-line no-var
let exportTypes = [];

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
 * @param {*} notExported A list of columns that are not included in an export file.
 * @param {*} selectOptions An object containing the options for every column with a select list filter.
 * @param {*} warnings A list of warnings from django.
 */
function initBSTPagination(
    limit,
    limitDefault,
    tableID,
    cookiePrefix,
    pageNumber,
    perPage,
    total,
    rawTotal,
    currentURL,
    notExported,
    selectOptions,
    warnings,
    cookieResets,
    clearCookies,
){
    globalThis.djangoLimit = limit;
    globalThis.djangoLimitDefault = limitDefault;
    globalThis.djangoTableID = tableID;
    globalThis.djangoCookiePrefix = cookiePrefix;
    globalThis.djangoPageNumber = pageNumber;
    globalThis.djangoPerPage = perPage;
    globalThis.djangoTotal = total;
    globalThis.djangoRawTotal = rawTotal;
    globalThis.djangoCurrentURL = currentURL;

    if (typeof clearCookies !== "undefined" && clearCookies) resetTable();
    if (typeof cookieResets !== "undefined" && cookieResets && cookieResets.length > 0) deleteCookies(cookieResets);

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
    const exportFileContentTag = document.getElementById('export_data');
    const exportFileNameTag = document.getElementById('export_filename');
    const exportTypeTag = document.getElementById('export_type');
    if (typeof exportFileContentTag !== 'undefined' && exportFileContentTag) {
        browserDownloadBase64(
            exportFileNameTag.innerHTML,
            exportFileContentTag.innerHTML,
            exportTypeTag.innerHTML
        );
    }
    setViewCookie('page', djangoPageNumber);

    let collapse = getViewCookie('collapsed', false);
    if (collapse) setCollapse(collapse);

    globalThis.filterSelectOptions = selectOptions;
    globalThis.exportTypes = JSON.parse($('#' + djangoTableID).data("export-types").replace(/'/g, '"'));

    displayWarnings(warnings);

    var loading = true;
    $('#' + djangoTableID).bootstrapTable({
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
        onExportStarted: function (event) {
            exportMessage();
        },
        onLoadError: function (status, jqXHR) {
            console.error("BootstrapTable Error.  Status: '" + status + "' Data:", jqXHR);
        },
        exportOptions: {
            ignoreColumn: notExported,
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
 * Gets a cookie specific to this view/page.
 * @param {*} name Cookie name.
 * @param {*} defval Default if cookie not found.
 * @returns Cookie value.
 */
function getViewCookie(name, defval) {
    return getCookie(djangoCookiePrefix + name, defval);
}

/**
 * Sets a cookie specific to this view/page.
 * @param {*} name Cookie name.
 * @param {*} val Cookie value.
 * @returns Cookie value.
 */
function setViewCookie(name, val) {
    return setCookie(djangoCookiePrefix + name, val);
}

/**
 * Deletes a cookie specific to this view/page.
 * @param {*} name Cookie name.
 * @returns Cookie value.
 */
function deleteViewCookie(name) {
    return deleteCookie(djangoCookiePrefix + name);
}

/**
 * Gets a cookie specific to this view/page and column.
 * @param {*} column Column name.
 * @param {*} name Cookie name.
 * @param {*} defval Default if cookie not found.
 * @returns Cookie value.
 */
function getViewColumnCookie(column, name, defval) {
    return getViewCookie(name + '-' + column, defval);
}

/**
 * Sets a cookie specific to this view/page and column.
 * @param {*} column Column name.
 * @param {*} name Cookie name.
 * @param {*} val Cookie value.
 * @returns Cookie value.
 */
function setViewColumnCookie(column, name, val) {
    return setViewCookie(name + '-' + column, val);
}

/**
 * Deletes a cookie specific to this view/page and column.
 * @param {*} column Column name.
 * @param {*} name Cookie name.
 * @returns Cookie value.
 */
function deleteViewColumnCookie(column, name) {
    return deleteViewCookie(name + '-' + column);
}

/**
 * Requests a new page from the server.
 * @param {*} page Page number.
 * @param {*} limit Rows per page.
 * @param {*} exportType Whether to export to a file and the file type.
 */
function updatePage(page, limit, exportType) {
    if (typeof limit === "undefined" || (limit !== 0 && !limit)) {
        limit = getViewCookie("limit", djangoLimit);
    }
    if (typeof page === "undefined" || !page) {
        page = getViewCookie("page", 1);
    }
    url = djangoCurrentURL + "?page=" + page
    if (typeof limit !== "undefined" && (limit === 0 || limit)) {
        url += "&limit=" + limit;
    }
    if (typeof exportType !== "undefined" && exportType) {
        url += "&export=" + exportType;
    }
    window.location.href = url;
}

function toggleCollapse() {
    // Initial state should be not collapsed
    const collapse = !(getViewCookie("collapsed", "false") === "true");
    console.log("Doing collapse: " + collapse);
    setCollapse(collapse);
    setCollapseIcon(!collapse);
    setViewCookie("collapsed", collapse);
}

function setCollapseIcon(collapse) {
    let addIconName = collapse ? 'bi-arrows-collapse' : 'bi-arrows-expand';
    let removeIconName = !collapse ? 'bi-arrows-collapse' : 'bi-arrows-expand';

    // Get the collapse button icon
    let iconElem = document.querySelectorAll("button[name='btnCollapse'] > .bi")[0];

    // Replace the previous icon with the current one
    iconElem.classList.remove(removeIconName);
    iconElem.classList.add(addIconName);
}

function setCollapse(collapse) {
    if (typeof collapse === "undefined") collapse = false
    const cellElems = document.getElementsByClassName("table-cell");
    for (let i = 0; i < cellElems.length; i++) {
        let cellElem = cellElems[i];
        if (collapse) cellElem.classList.add('nobr');
        else cellElem.classList.remove('nobr');
    }
    const wrapElems = document.getElementsByClassName("cell-wrap");
    for (let i = 0; i < wrapElems.length; i++) {
        let wrapElem = wrapElems[i];
        if (collapse) wrapElem.classList.add('d-none');
        else wrapElem.classList.remove('d-none');
    }
}

/**
 * Takes the export options defined in the bootstrap table and generates a drop-down list that calls the custom
 * exportAllPages function when clicked.
 */
function generateExportSelect() {
    let html = `<div class="btn-group">
                    <button type="button"
                            class="btn btn-primary dropdown-toggle"
                            data-bs-toggle="dropdown"
                            aria-expanded="false">
                        <i class="bi bi-download"></i>
                    </button>
                    <ul class="dropdown-menu">\n`
    for (let i=0; i < globalThis.exportTypes.length; i++) {
        let val = globalThis.exportTypes[i];
        let disp;
        if (val === 'csv')
            disp = 'CSV';
        else if (val === 'txt')
            disp = 'TXT';
        else if (val === 'excel')
            disp = 'MS-Excel';
        else
            disp = val;
        html += `        <li><a class="dropdown-item" onclick="exportAllPages('${val}')">${disp}</a></li>\n`;
    }
    html += `    </ul>
            </div>\n`;
    checkBuiltinExport();
    return html
}

/**
 * Validates the BST export settings.
 */
function checkBuiltinExport() {
    let tableElem = document.getElementById(djangoTableID);
    if (tableElem.hasAttribute('data-show-export') && $('#' + djangoTableID).data("show-export"))
        alert('ERROR: data-show-export is custom-handled, so must be false.');
}

/**
 * Triggers a page load and data download.
 * @param {*} format Export format/type. {as specified by BST}
 */
function exportAllPages(format) {
    updatePage(undefined, undefined, format);
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
 * Updates the rows per page and requests a new page from the server.
 * @param {*} numRows Number of rows per page to request.
 */
function onRowsPerPageChange(numRows) {
    let oldLimit = parseInt(getViewCookie('limit', djangoLimit));
    if (isNaN(oldLimit)) {
        oldLimit = djangoPerPage;
    }
    let curPage = parseInt(getViewCookie('page'));
    if (isNaN(curPage)) {
        curPage = 1;
    }
    let curOffset = (curPage - 1) * oldLimit + 1;
    let closestPage = curPage;
    setViewCookie('limit', numRows);
    if (numRows !== 0) {
        closestPage = parseInt(curOffset / numRows) + 1;
        // Reset the page
        setViewCookie("page", closestPage);
    }
    updatePage();
}

/**
 * Advances to a different page.
 * @param {*} page Page number.
 */
function onPageChange(page) {
    setViewCookie('page', page)
    updatePage();
}

/**
 * Retrieves a list of column names.
 * @returns Column names.
 */
function getColumnNames() {
    let columnNames = [];
    $('#' + djangoTableID).bootstrapTable('getVisibleColumns').map(function (col) {
        columnNames.push(col.field)
    });
    $('#' + djangoTableID).bootstrapTable('getHiddenColumns').map(function (col) {
        columnNames.push(col.field)
    });
    return columnNames
}

/**
 * Clears cookies and requests a reinitialized page from the server.
 */
function resetTable() {
    deleteViewCookie('page');
    deleteViewCookie('limit');
    deleteViewCookie('order-by');
    deleteViewCookie('order-dir');
    deleteViewCookie('search');
    deleteViewCookie('collapsed');
    let columnNames = getColumnNames();
    for (i=0; i < columnNames.length; i++) {
        let columnName = columnNames[i];
        deleteViewColumnCookie(columnName, 'filter');
        deleteViewColumnCookie(columnName, 'visible');
    }
    updatePage(1, djangoLimitDefault);
}

/**
 * Initializes settings for custom buttons in the BST toolbar, including a clear button to clear out cookies and a
 * custom export dropdown button..
 * @returns Settings object for BST.
 */
function customButtonsFunction () {
    return {
        btnClear: {
            'text': 'Reset Page to default settings',
            'icon': 'bi-house',
            'event': function btnClearTableSettings () {
                resetTable();
            },
            'attributes': {
                'title': 'Restore default sort, filter, search, column visibility, and pagination'
            }
        },
        btnCollapse: {
            'text': 'Toggle soft-wrap in all table cells',
            'icon': !(getViewCookie("collapsed", "false") === "true") ? 'bi-arrows-collapse' : 'bi-arrows-expand',
            'event': function btnToggleCollapse () {
                toggleCollapse();
            },
            'attributes': {
                'title': 'Toggle soft-wrap in all table cells'
            }
        },
        btnExportAll: {
            html: generateExportSelect
        }
    };
}

/**
 * Fallback export behavior.  If the custom export dropdown button is eliminated, this kicks in to tell the user how to
 * download all results.  If the user decides they want all results, they are instructed to select all rows per page and
 * an error is thrown to stop bootstrap from doing an export of 1 page of data.
 */
function exportMessage() {
    if (djangoLimit !== 0 && djangoLimit < djangoTotal) {
        if (
            confirm(
                "Download 1 page?\n\nTo retrieve all data, cancel and select 'ALL' from the rows per page select list "
                + "below and try again."
            )
        ) {
            console.log("Downloading page " + djangoPageNumber + ".");
        } else {
            throw new Error("Canceling download");
        }
    }
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
 * Prompts the user for a page number to jump to.
 * @param {*} curpage The current page (for autofill into the prompt).
 * @param {*} num_pages The total number of pages to ensure valid input.
 */
function askForPage(curpage, num_pages){
    let valid = false
    let canceled = false
    let errmsg = ""
    let newpagenum = curpage
    while (!valid) {
        var newpagestr = prompt(errmsg + "Enter a page number between 1 and " + num_pages + ":", curpage);
        if(typeof newpagestr === 'undefined' || !newpagestr){
            canceled = true
            valid = true
        } else {
            newpagenum = parseInt(newpagestr);
            if(isNaN(newpagenum)){
                errmsg = "Error: [" + newpagestr + "] is not an integer.\n"
            } else if(newpagenum < 1) {
                errmsg = "Error: [" + newpagestr + "] must be greater than 0.\n"
            } else if(newpagenum > num_pages) {
                errmsg = `Error: [${newpagestr}] must be less than or equal to the number of pages: [${num_pages}].\n`
            } else {
                valid = true
            }
        }
    }
    if (canceled || typeof newpagenum === 'undefined' || !newpagenum) {
        newpagenum = curpage
    }
    if (newpagenum !== curpage) {
        url = "?page=" + newpagenum + "&limit=" + djangoLimit
        window.location.href = url
    }
}
