/* eslint-disable no-undef */

QUnit.test('getVisibleValue', function (assert) {
  assert.equal(getVisibleValue(' abc '), 'abc')
  assert.equal(getVisibleValue(' 123.456 '), '123.456')
  assert.equal(getVisibleValue(' 123.456 789 '), '123.456 789')
  assert.equal(getVisibleValue(' <a href="xyz"> abc </a> <br> '), 'abc')
})

/* eslint-enable no-undef */
