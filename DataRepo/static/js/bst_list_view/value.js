
/**
 * Take a string possibly containing html and return the content without the html elements.
 * Assumes proper html syntax.
 * @param {*} v String possibly containing html elements.
 * @returns The input v with html elements and leading/trailing whitespace extracted
 */
function getVisibleValue (v) { // eslint-disable-line no-unused-vars
  /* eslint-env jquery */

  if (typeof v === 'undefined') return v
  if (typeof v !== 'string') v = v.toString()

  // Regular expression to see if the a string starts with an html element
  // See: https://stackoverflow.com/a/23076716/2057516
  const isHTMLregex = /^(?:\s*(<[\w\W]+>)[^>]*|#([\w-]*))$/

  // Extract the inner HTML, if the content is inside an HTML element
  if (isHTMLregex.test(v)) v = $(v).text().trim()
  else v = v.trim()

  // Do not alphabetically sort "None" (special case from Python/Django)
  // Nones should appear first (or last if desc sorting)
  if (v === 'None') v = undefined

  return v
}
