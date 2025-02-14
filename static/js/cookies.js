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
  return document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)')?.pop() || defval
}

/**
 * Set a cookie.  Note, you should name the cookie specific to the page you are working on, unless you want the cookie
 * accessible on every page.
 * @param {*} name Name of the cookie, e.g. "my_cookie"
 * @param {*} val The value associated with the cookie to save.
 */
function setCookie (name, val) { // eslint-disable-line no-unused-vars
  document.cookie = name + '=' + val + '; path=/'
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
