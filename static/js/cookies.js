/**
 * Get a cookie.  Note, you should name the cookie specific to the page you are working on, unless you want the cookie
 * accessible on every page.
 * @param {*} name Name of the cookie, e.g. "my_cookie"
 * @param {*} defval Default value if cookie not found
 * @returns The value associated with the cookie or an empty string if not found.
 */
function getCookie (name, defval) { // eslint-disable-line no-unused-vars
  if (typeof defval === 'undefined' || !defval) {
    defval = ''
  }

  // Retrieve the cookie value
  const val = document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)')?.pop() || defval

  // If the value is the default, return it
  if (typeof val !== 'undefined' && val && val === defval) {
    return val
  }

  // Account for the possibility that this was a cookie saved before we started encoding the values
  try {
    // If this is not an encoded string, an error can be^ thrown
    const tmpval = decodeURIComponent(val)

    // ^ A regular string can look like an encoded string, in which case, the return value will be invalid, but those
    // cases will eventually flush out.
    return tmpval
  } catch (e) {
    return val
  }
}

/**
 * Set a cookie.  Note, you should name the cookie specific to the page you are working on, unless you want the cookie
 * accessible on every page.
 * @param {*} name Name of the cookie, e.g. "my_cookie"
 * @param {*} val The value associated with the cookie to save.
 */
function setCookie (name, val) { // eslint-disable-line no-unused-vars
  document.cookie = name + '=' + encodeURIComponent(val) + '; path=/'
}

/**
 * Delete a cookie
 * @param {*} name The name of the cookie to delete
 */
function deleteCookie (name) { // eslint-disable-line no-unused-vars
  const curval = getCookie(name)
  if (typeof curval !== 'undefined' && curval) {
    document.cookie = name + '=; path=/'
  }
}

/**
 * Delete a cookie
 * @param {*} name The name of the cookie to delete
 */
function deleteCookies (names) { // eslint-disable-line no-unused-vars
  for (let i = 0; i < names.length; i++) {
    deleteCookie(names[i]);
  }
}
