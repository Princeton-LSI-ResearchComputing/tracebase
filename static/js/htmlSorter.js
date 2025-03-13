// sort column based on HTML values
// ref: https://github.com/wenzhixin/bootstrap-table/issues/461
function htmlSorter (a, b) { // eslint-disable-line no-unused-vars
  /* eslint-env jquery */
  return alphanumericSorter(a, b)
}

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

function getSortValue (v) {
  /* eslint-env jquery */

  // Regular expression to see if the a string starts with an html element
  // See: https://stackoverflow.com/a/23076716/2057516
  const isHTMLregex = /^(?:\s*(<[\w\W]+>)[^>]*|#([\w-]*))$/

  // Extract the inner HTML, if the content is inside an HTML element
  if (isHTMLregex.test(v)) v = $(v).text().trim()

  // Do not alphabetically sort "None" (special case from Python/Django)
  // Nones should appear first (or last if desc sorting)
  if (v === 'None') v = undefined

  return v
}
