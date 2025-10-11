/* eslint-disable no-undef */

QUnit.test('initViewCookies', function (assert) {
  initViewCookies('myview-')
  assert.equal(cookieViewPrefix, 'myview-')
})

QUnit.test('getViewCookie', function (assert) {
  initViewCookies('myview-')
  deleteViewCookies()
  const result = getViewCookie('mycookie1', 'y')
  assert.equal(result, 'y')
  setViewCookie('mycookie1', 'x')
  assert.equal(getViewCookie('mycookie1', 'y'), 'x')
})

QUnit.test('setViewCookie', function (assert) {
  const result = setViewCookie('mycookie2', 'x')
  assert.equal(result, 'x')
  assert.equal(getViewCookie('mycookie2', 'y'), 'x')
})

QUnit.test('deleteViewCookie', function (assert) {
  setViewCookie('mycookie3', 'x')
  const result = deleteViewCookie('mycookie3')
  assert.equal(result, 'x')
  assert.equal(getViewCookie('mycookie3', 'y'), 'y')
})

QUnit.test('getViewColumnCookie', function (assert) {
  initViewCookies('myview-')
  deleteViewCookies()
  assert.equal(getViewColumnCookie('col1', 'mycookie4', 'y'), 'y')
  setViewColumnCookie('col1', 'mycookie4', 'x')
  assert.equal(getViewColumnCookie('col1', 'mycookie4', 'y'), 'x')
})

QUnit.test('setViewColumnCookie', function (assert) {
  assert.equal(setViewColumnCookie('col2', 'mycookie5', 'x'), 'x')
  assert.equal(getViewColumnCookie('col2', 'mycookie5', 'y'), 'x')
})

QUnit.test('deleteViewColumnCookie', function (assert) {
  setViewColumnCookie('col3', 'mycookie6', 'x')
  const result = deleteViewColumnCookie('col3', 'mycookie6')
  assert.equal(result, 'x')
  assert.equal(getViewColumnCookie('col3', 'mycookie6', 'y'), 'y')
})

QUnit.test('deleteViewColumnCookies', function (assert) {
  setViewColumnCookie('col3', 'mycookie6', 'x')
  setViewColumnCookie('col4', 'mycookie6', 'y')
  setViewColumnCookie('col5', 'mycookie6', '')
  const result = deleteViewColumnCookies(['col3', 'col4'], 'mycookie6')
  assert.equal(result, 2)
  assert.equal(getViewColumnCookie('col3', 'mycookie6', 'z'), 'z')
  assert.equal(getViewColumnCookie('col4', 'mycookie6', 'z'), 'z')
  assert.equal(getViewColumnCookie('col5', 'mycookie6', 'z'), 'z')
})

QUnit.test('getViewCookieNames', function (assert) {
  initViewCookies('myview-')
  deleteViewCookies()
  setViewCookie('mycookie7', 'x')
  setViewCookie('mycookie8', 'x')
  setViewColumnCookie('col4', 'mycookie9', 'x')
  const result = getViewCookieNames()
  assert.deepEqual(result, ['myview-mycookie7', 'myview-mycookie8', 'myview-mycookie9-col4'])
})

QUnit.test('deleteViewCookies', function (assert) {
  initViewCookies('myview-')
  deleteViewCookies()
  setViewCookie('mycookie7', 'x')
  setViewCookie('mycookie8', 'x')
  setViewColumnCookie('col4', 'mycookie9', 'x')
  const result = deleteViewCookies()
  assert.equal(result.length, 3)
  assert.deepEqual(result, ['myview-mycookie7', 'myview-mycookie8', 'myview-mycookie9-col4'])
})

/* eslint-enable no-undef */
