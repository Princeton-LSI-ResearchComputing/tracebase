/**
 * Get a cookie.  Note, you should name the cookie specific to the page you are working on, unless you want the cookie
 * accessible on every page.
 * @param {*} name Name of the cookie, e.g. "my_cookie"
 * @returns The value associated with the cookie or an empty string if not found.
 */
const getCookie = (name) => ( // eslint-disable-line no-unused-vars
  document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)')?.pop() || ''
)

/**
 * Set a cookie.  Note, you should name the cookie specific to the page you are working on, unless you want the cookie
 * accessible on every page.
 * @param {*} name Name of the cookie, e.g. "my_cookie"
 * @param {*} val The value associated with the cookie to save.
 */
const setCookie = (name, val) => { // eslint-disable-line no-unused-vars
  document.cookie = name + '=' + val + '; path=/'
}
