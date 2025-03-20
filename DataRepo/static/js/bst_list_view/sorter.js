/**
 * In order to make Bootstrap Table provide its sorting controls, but make sorting handled server-side by django, we
 * need to provide the name of a sorting method to Bootstrap Table's sorter attribute that effects no actual sort.  This
 * is that method.
 * @param {*} a Ignored
 * @param {*} b Ignored
 * @returns 0
 */
function djangoSorter (a, b) { // eslint-disable-line no-unused-vars
  return 0
}

/**
 * When the BSTListView is displaying ALL rows, it is faster and more efficient to allow BST to do the sorting.
 * This method sorts the visible content, ignoring differences inside the HTML elements' attributes.
 * Compare values a and b as alphanumeric values, ignoring case.
 * @param {*} a First value to compare, potentially containing html evements whose attributes should be ignored.
 * @param {*} b Second value to compare, potentially containing html evements whose attributes should be ignored.
 * @returns -1, 0, or 1 indicating a<b, a==b, or a>b
 */
function alphanumericSorter (a, b) { // eslint-disable-line no-unused-vars
  /* eslint-env jquery */

  a = getSortValue(a)
  b = getSortValue(b)

  // Do not alphabetically sort "None" (special case from Python/Django)
  // Nones should appear first (or last if desc sorting)
  if (typeof a === 'undefined' && typeof b !== 'undefined') return -1
  if (typeof a !== 'undefined' && typeof b === 'undefined') return 1
  if (typeof a === 'undefined' && typeof b === 'undefined') return 0

  a = a.toLowerCase()
  b = b.toLowerCase()

  if (a < b) return -1
  if (a > b) return 1
  return 0
}

/**
 * When the BSTListView is displaying ALL rows, it is faster and more efficient to allow BST to do the sorting.
 * This method sorts the visible content, ignoring differences inside the HTML elements' attributes.
 * Compare values a and b as floats/numbers.
 * @param {*} a First value to compare, potentially containing html evements whose attributes should be ignored.
 * @param {*} b Second value to compare, potentially containing html evements whose attributes should be ignored.
 * @returns -1, 0, or 1 indicating a<b, a==b, or a>b
 */
function numericSorter (a, b) { // eslint-disable-line no-unused-vars
  /* eslint-env jquery */

  a = getSortValue(a)
  b = getSortValue(b)

  console.log("Sort val a: '" + a + "' b: '" + b + "'")

  // Do not alphabetically sort "None" (special case from Python/Django)
  // Nones should appear first (or last if desc sorting)
  if (typeof a === 'undefined' && typeof b !== 'undefined') return -1
  if (typeof a !== 'undefined' && typeof b === 'undefined') return 1
  if (typeof a === 'undefined' && typeof b === 'undefined') return 0

  a = parseFloat(a)
  b = parseFloat(b)

  if (a < b) return -1
  if (a > b) return 1
  return 0
}

/**
 * Take a string possibly containing html and return the content without the html elements.
 * Assumes proper html syntax.
 * @param {*} v String possibly containing html elements.
 * @returns The input v with html elements and leading/trailing whitespace extracted
 */
function getSortValue (v) {
  /* eslint-env jquery */

  // Regular expression to see if the a string starts with an html element
  // See: https://stackoverflow.com/a/23076716/2057516
  isHTMLregex = /^(?:\s*(<[\w\W]+>)[^>]*|#([\w-]*))$/

  // Extract the inner HTML, if the content is inside an HTML element
  if (isHTMLregex.test(v)) v = $(v).text().trim()
  else v = v.trim()

  // Do not alphabetically sort "None" (special case from Python/Django)
  // Nones should appear first (or last if desc sorting)
  if (v === 'None') v = undefined

  return v
}
