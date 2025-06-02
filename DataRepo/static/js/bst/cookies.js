var cookieViewPrefix = null // eslint-disable-line no-var

// To use this code, static/js/cookies.js must be imported.

/**
 * Gets a cookie specific to this view/page.
 * @param {*} name Cookie name.
 * @param {*} defval Default if cookie not found.
 * @returns Cookie value.
 */
function initViewCookies (cookieViewPrefix) { // eslint-disable-line no-unused-vars
  globalThis.cookieViewPrefix = cookieViewPrefix
}

/**
 * Gets a cookie specific to this view/page.
 * @param {*} name Cookie name.
 * @param {*} defval Default if cookie not found.
 * @returns Cookie value.
 */
function getViewCookie (name, defval) { // eslint-disable-line no-unused-vars
  return getCookie(cookieViewPrefix + name, defval) // eslint-disable-line no-undef
}

/**
 * Sets a cookie specific to this view/page.
 * @param {*} name Cookie name.
 * @param {*} val Cookie value.
 * @returns Cookie value.
 */
function setViewCookie (name, val) { // eslint-disable-line no-unused-vars
  return setCookie(cookieViewPrefix + name, val) // eslint-disable-line no-undef
}

/**
 * Deletes a cookie specific to this view/page.
 * @param {*} name Cookie name.
 * @returns Cookie value.
 */
function deleteViewCookie (name) { // eslint-disable-line no-unused-vars
  return deleteCookie(cookieViewPrefix + name) // eslint-disable-line no-undef
}

/**
 * Gets a cookie specific to this view/page and column.
 * @param {*} column Column name.
 * @param {*} name Cookie name.
 * @param {*} defval Default if cookie not found.
 * @returns Cookie value.
 */
function getViewColumnCookie (column, name, defval) { // eslint-disable-line no-unused-vars
  return getViewCookie(name + '-' + column, defval)
}

/**
 * Sets a cookie specific to this view/page and column.
 * @param {*} column Column name.
 * @param {*} name Cookie name.
 * @param {*} val Cookie value.
 * @returns Cookie value.
 */
function setViewColumnCookie (column, name, val) { // eslint-disable-line no-unused-vars
  return setViewCookie(name + '-' + column, val)
}

/**
 * Deletes a cookie specific to this view/page and column.
 * @param {*} column Column name.
 * @param {*} name Cookie name.
 * @returns Cookie value.
 */
function deleteViewColumnCookie (column, name) { // eslint-disable-line no-unused-vars
  return deleteViewCookie(name + '-' + column)
}

/**
 * Get all cookie names of a view.
 * @returns List of cookie names belonging to the view.
 */
function getViewCookieNames () { // eslint-disable-line no-unused-vars
  return document.cookie.split(';').filter(function (c) {
    return c.trim().indexOf(cookieViewPrefix) === 0 && c.trim().split('=')[1] !== ''
  }).map(function (c) {
    return c.trim().split('=')[0]
  })
}

/**
 * Deletes all or supplied cookies of a view.
 * @param {*} viewNames The names of cookies in a view (without the prefix - but must contain the column portion of a
 * cookie name, as this method calls deleteViewCookie, not deleteViewColumnCookie).  But note that all view cookies
 * (including column cookies are deleted if viewnames is not supplied or is empty).
 * @returns A list of the names of the deleted cookies.
 */
function deleteViewCookies (viewNames) { // eslint-disable-line no-unused-vars
  var cookieNames = []
  if (typeof viewNames !== 'undefined' && viewNames.length > 0) {
    for (let i = 0; i < viewNames.length; i++) {
      deleteViewCookie(viewNames[i])
      cookieNames.push(globalThis.cookieViewPrefix + viewNames[i])
    }
  } else {
    cookieNames = getViewCookieNames()
    deleteCookies(cookieNames) // eslint-disable-line no-undef
  }
  return cookieNames
}
