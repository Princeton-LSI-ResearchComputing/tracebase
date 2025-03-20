/* eslint-disable no-undef */

QUnit.test('djangoSorter', function (assert) {
  assert.equal(djangoSorter('a', 'b'), 0)
  assert.equal(djangoSorter('b', 'a'), 0)
  assert.equal(djangoSorter('a', 'a'), 0)
})

QUnit.test('alphanumericSorter', function (assert) {
  assert.equal(alphanumericSorter(' abc ', 'abc'), 0)
  assert.equal(alphanumericSorter(' abc ', 'xyz'), -1)
  assert.equal(alphanumericSorter(' xyz ', 'abc'), 1)
  assert.equal(alphanumericSorter(' <a href="xyz"> abc </a> <br> ', 'xyz abc'), -1)
})

QUnit.test('numericSorter', function (assert) {
  assert.equal(numericSorter(' 2 ', '10'), -1)
  assert.equal(numericSorter('10', '2'), 1)
  assert.equal(numericSorter('xyz', 'abc'), 0)
  assert.equal(numericSorter('<a href="xyz"> 5 </a><br>', '5'), 0)
})

/* eslint-enable no-undef */
