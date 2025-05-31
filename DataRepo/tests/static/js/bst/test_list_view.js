/* eslint-disable no-undef */

// NOTE: We cannot test the methods that trigger a redirect, so we test the supporting methods.

QUnit.test('updatePage', function (assert) {
  deleteViewCookies()
  setViewCookie('page', 5)
  setViewCookie('limit', 25)
  const url = getPageURL(2, 10, 'text')
  assert.true(url.includes('page=2'))
  assert.true(url.includes('limit=10'))
  assert.true(url.includes('export=text'))
})

QUnit.test('onRowsPerPageChange', function (assert) {
  deleteViewCookies()
  setViewCookie('page', 5)
  setViewCookie('limit', 25)
  updateRowsPerPage(20)
  // Test indirectly by creating a URL based on the update
  const url = getPageURL()
  // Gets closest page
  assert.true(url.includes('page=6'))
  assert.true(url.includes('limit=20'))
  assert.false(url.includes('export'))
})

QUnit.test('onPageChange', function (assert) {
  deleteViewCookies()
  setViewCookie('page', 5)
  setViewCookie('limit', 25)
  updatePageNum(3)
  // Test indirectly by creating a URL based on the update
  const url = getPageURL()
  assert.true(url.includes('page=3'))
  // limit doesn't change from cookie value
  assert.true(url.includes('limit=25'))
  assert.false(url.includes('export'))
})

QUnit.test('resetTable', function (assert) {
  deleteViewCookies()
  setViewCookie('page', 5)
  setViewCookie('limit', 25)
  resetTableCookies()
  // Test indirectly by creating a URL based on the update
  const url = getPageURL()
  // Defaults to djangoPageNumber = 1
  assert.true(url.includes('page=1'))
  // Defaults to djangoPerPage = 15
  assert.true(url.includes('limit=15'))
  assert.false(url.includes('export'))
})

/* eslint-enable no-undef */
