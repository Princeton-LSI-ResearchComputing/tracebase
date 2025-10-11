/**
 * In order to make Bootstrap Table provide its filtering controls, but make filtering handled server-side by django, we
 * need to provide the name of a filtering method to Bootstrap Table's data-filter-custom-search attribute that effects
 * no actual filtering.  Without this, whenever a page loads, you run the risk of non-matching filtering behavior where
 * a served result is filtered out by the client's browser.  This is that method.
 * @param {*} term Ignored
 * @param {*} colval Ignored
 * @param {*} colname Ignored
 * @param {*} alldata Ignored
 * @returns true
 */
function djangoFilterer (term, colval, colname, alldata) { // eslint-disable-line no-unused-vars
  return true
}

/**
 * When the BSTListView is displaying ALL rows, it is faster and more efficient to allow BST to do the filtering.
 * This method applies a filter using the visible content, ignoring differences inside the HTML elements' attributes.
 * Returns true when the term is contained in the colval (ignoring case differences), false otherwise.
 * @param {*} term The search term
 * @param {*} colval A value from the table column to see if it matches the search term
 * @param {*} colname The name of the column being filtered
 * @param {*} alldata All of the table data
 * @returns true if matches, false otherwise
 */
function containsFilterer (term, colval, colname, alldata) { // eslint-disable-line no-unused-vars
  /* eslint-env jquery */

  colval = getVisibleValue(colval) // eslint-disable-line no-undef
  term = getVisibleValue(term) // eslint-disable-line no-undef

  if (typeof colval === 'undefined' || typeof term === 'undefined') {
    return (typeof term === 'undefined') === (typeof colval === 'undefined')
  }

  colval = colval.toLowerCase()
  term = term.toLowerCase()

  return colval.includes(term)
}

/**
 * When the BSTListView is displaying ALL rows, it is faster and more efficient to allow BST to do the filtering.
 * This method applies a filter using the visible content, ignoring differences inside the HTML elements' attributes.
 * Returns true when the term is equal to the colval (ignoring case differences), false otherwise.
 * @param {*} term The search term
 * @param {*} colval A value from the table column to see if it matches the search term
 * @param {*} colname The name of the column being filtered
 * @param {*} alldata All of the table data
 * @returns true if matches, false otherwise
 */
function strictFilterer (term, colval, colname, alldata) { // eslint-disable-line no-unused-vars
  /* eslint-env jquery */

  term = getVisibleValue(term) // eslint-disable-line no-undef
  colval = getVisibleValue(colval) // eslint-disable-line no-undef

  if (typeof colval === 'undefined' || typeof term === 'undefined') {
    return (typeof term === 'undefined') === (typeof colval === 'undefined')
  }

  term = term.toLowerCase()
  colval = colval.toLowerCase()

  return colval === term
}
