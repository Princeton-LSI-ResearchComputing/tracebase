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

  a = getVisibleValue(a) // eslint-disable-line no-undef
  b = getVisibleValue(b) // eslint-disable-line no-undef

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
 * @param {*} x First value to compare, potentially containing html evements whose attributes should be ignored.
 * @param {*} y Second value to compare, potentially containing html evements whose attributes should be ignored.
 * @returns -1, 0, or 1 indicating a<b, a==b, or a>b
 */
function numericSorter (x, y) { // eslint-disable-line no-unused-vars
  /* eslint-env jquery */

  x = getVisibleValue(x) // eslint-disable-line no-undef
  y = getVisibleValue(y) // eslint-disable-line no-undef

  // Do not alphabetically sort "None" (special case from Python/Django)
  // Nones should appear first (or last if desc sorting)
  if (typeof x === 'undefined' && typeof y !== 'undefined') return -1
  if (typeof x !== 'undefined' && typeof y === 'undefined') return 1
  if (typeof x === 'undefined' && typeof y === 'undefined') return 0

  x = parseFloat(x)
  y = parseFloat(y)

  if (x < y) return -1
  if (x > y) return 1
  return 0
}
