/* eslint-disable no-undef */

QUnit.test('djangoFilterer defined', function (assert) {
  assert.equal(djangoFilterer('a', 'b'), true)
  assert.equal(djangoFilterer('b', 'a'), true)
  assert.equal(djangoFilterer('a', 'a'), true)
})

QUnit.test('djangoFilterer undefined', function (assert) {
  const undef = undefined
  assert.equal(djangoFilterer('a', undef), true)
  assert.equal(djangoFilterer(undef, undef), true)
  assert.equal(djangoFilterer('a', 'None'), true)
  assert.equal(djangoFilterer('None', undef), true)
  assert.equal(djangoFilterer('None', 'None'), true)
})

QUnit.test('containsFilterer defined', function (assert) {
  assert.equal(containsFilterer(' abc ', 'abcd'), true)
  assert.equal(containsFilterer(' abc ', 'ab'), false)
  assert.equal(containsFilterer(' bc ', 'abcd'), true)
  assert.equal(containsFilterer('xyz', ' <a href="xyz"> abc </a> <br> '), false)
  assert.equal(containsFilterer('bc', ' <a href="xyz"> abc </a> <br> '), true)
  assert.equal(containsFilterer('b', ' <a href="xyz"> xyz </a> <br> '), false)
})

QUnit.test('containsFilterer undefined', function (assert) {
  const undef = undefined
  assert.equal(containsFilterer('a', undef), false)
  assert.equal(containsFilterer('a', 'None'), false)
  assert.equal(containsFilterer(undef, undef), true)
  assert.equal(containsFilterer('None', undef), true)
})

QUnit.test('strictFilterer defined', function (assert) {
  assert.equal(strictFilterer(' abc ', 'abcd'), false)
  assert.equal(strictFilterer(' abc ', 'abc'), true)
  assert.equal(strictFilterer('bc', 'abcd'), false)
  assert.equal(strictFilterer('abc', ' <a href="xyz"> abc </a> <br> '), true)
  assert.equal(strictFilterer('xyz', ' <a href="xyz"> abc </a> <br> '), false)
})

QUnit.test('strictFilterer undefined', function (assert) {
  const undef = undefined
  assert.equal(strictFilterer('a', undef), false)
  assert.equal(strictFilterer('a', 'None'), false)
  assert.equal(strictFilterer(undef, undef), true)
  assert.equal(strictFilterer('None', undef), true)
})

/* eslint-enable no-undef */
