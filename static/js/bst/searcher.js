/**
 * In order to make Bootstrap Table provide its whole-table search input, but make searching handled server-side by
 * django, we need to provide the name of a custom search method to Bootstrap Table's data-custom-search attribute that
 * effects no actual search of the table column values.  Without this, whenever a page loads, you run the risk of non-
 * matching search behavior where a served result is filtered out by the client's browser.  This is particularly true
 * when it comes to many-related columns whose values are truncated by the column object's limit attribute, where a
 * matched term might not actually be visible in the first few delimited items that are displayed.  E.g. search a study
 * for lysine, and you see that the compounds column may show that there were 34 compounds analyzed in the study, but
 * lysine is not among the first 3 shown in the sample.  This is that method.
 * @param {*} data Ignored
 * @param {*} term Ignored
 * @returns true
 */
function djangoSearcher (data, term) { // eslint-disable-line no-unused-vars
  return data.filter(function (row) { return true })
}
