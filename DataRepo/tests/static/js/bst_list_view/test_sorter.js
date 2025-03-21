/* eslint-disable no-undef */

QUnit.test('djangoSorter defined', function (assert) {
  assert.equal(djangoSorter('a', 'b'), 0)
  assert.equal(djangoSorter('b', 'a'), 0)
  assert.equal(djangoSorter('a', 'a'), 0)
})

QUnit.test('djangoSorter undefined', function (assert) {
  const undef = void 0
  assert.equal(djangoSorter('a', undef), 0)
  assert.equal(djangoSorter(undef, undef), 0)
  assert.equal(djangoSorter('a', 'None'), 0)
  assert.equal(djangoSorter('None', undef), 0)
  assert.equal(djangoSorter('None', 'None'), 0)
})

QUnit.test('alphanumericSorter defined', function (assert) {
  assert.equal(alphanumericSorter(' abc ', 'abc'), 0)
  assert.equal(alphanumericSorter(' abc ', 'xyz'), -1)
  assert.equal(alphanumericSorter(' xyz ', 'abc'), 1)
  assert.equal(alphanumericSorter(' <a href="xyz"> abc </a> <br> ', 'xyz abc'), -1)
})

QUnit.test('alphanumericSorter undefined', function (assert) {
  const undef = void 0
  assert.equal(alphanumericSorter(undef, 'abc'), -1)
  assert.equal(alphanumericSorter('abc', undef), 1)
  assert.equal(alphanumericSorter(undef, undef), 0)
  // "None" is a special django case
  assert.equal(alphanumericSorter('None', 'abc'), -1)
  assert.equal(alphanumericSorter('xyz', 'None'), 1)
  assert.equal(alphanumericSorter('None', 'None'), 0)
})

QUnit.test('numericSorter defined', function (assert) {
  assert.equal(numericSorter(' 2 ', '10'), -1)
  assert.equal(numericSorter('10', '2'), 1)
  assert.equal(numericSorter('xyz', 'abc'), 0)
  assert.equal(numericSorter('<a href="xyz"> 5 </a><br>', '5'), 0)
  assert.equal(numericSorter('<a href="xyz"> 5 </a><br>', '5'), 0)
  assert.equal(numericSorter('<a href="xyz"> 5 </a><br>', '5'), 0)
  assert.equal(numericSorter('<a href="xyz"> 5 </a><br>', '5'), 0)
})

QUnit.test('numericSorter undefined', function (assert) {
  const undef = void 0
  assert.equal(numericSorter(undef, '-5'), -1)
  assert.equal(numericSorter('5', undef), 1)
  assert.equal(numericSorter(undef, undef), 0)
  // "None" is a special django case
  assert.equal(numericSorter('None', '-5'), -1)
  assert.equal(numericSorter('5', 'None'), 1)
  assert.equal(numericSorter('None', 'None'), 0)
})

/* eslint-enable no-undef */
