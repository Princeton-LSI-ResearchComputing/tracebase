/* eslint-disable no-undef */

QUnit.test('djangoFilterer', function (assert) {
  assert.equal(djangoFilterer('a', 'b'), true)
  assert.equal(djangoFilterer('b', 'a'), true)
  assert.equal(djangoFilterer('a', 'a'), true)
})

QUnit.test('containsFilterer', function (assert) {
  assert.equal(containsFilterer(' abc ', 'abcd'), true)
  assert.equal(containsFilterer(' abc ', 'ab'), false)
  assert.equal(containsFilterer(' bc ', 'abcd'), true)
  assert.equal(containsFilterer('xyz', ' <a href="xyz"> abc </a> <br> '), false)
  assert.equal(containsFilterer('bc', ' <a href="xyz"> abc </a> <br> '), true)
  assert.equal(containsFilterer('b', ' <a href="xyz"> xyz </a> <br> '), false)
})

QUnit.test('strictFilterer', function (assert) {
  assert.equal(strictFilterer(' abc ', 'abcd'), false)
  assert.equal(strictFilterer(' abc ', 'abc'), true)
  assert.equal(strictFilterer('bc', 'abcd'), false)
  assert.equal(strictFilterer('abc', ' <a href="xyz"> abc </a> <br> '), true)
  assert.equal(strictFilterer('xyz', ' <a href="xyz"> abc </a> <br> '), false)
})

/* eslint-enable no-undef */
