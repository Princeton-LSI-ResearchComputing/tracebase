/* eslint-disable no-undef */

QUnit.test('getVisibleValue', function (assert) {
  assert.equal(getVisibleValue(' abc '), 'abc')
  assert.equal(getVisibleValue(' 123.456 '), '123.456')
  assert.equal(getVisibleValue(' 123.456 789 '), '123.456 789')
  assert.equal(getVisibleValue(' <a href="xyz"> abc </a> <br> '), 'abc')
  // undefined case
  const undef = undefined
  assert.equal(typeof getVisibleValue(undef) === 'undefined', true)
  // Special django case where "None" should be undefined
  assert.equal(typeof getVisibleValue('None') === 'undefined', true)
})

/* eslint-enable no-undef */
