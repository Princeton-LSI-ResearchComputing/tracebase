/* eslint-disable no-undef */

// NOTE: We cannot test the methods that trigger a redirect, prompt the user with a dialog, apply event listeners so we
// test the supporting methods.

QUnit.test('validatePageNum', function (assert) {
  // Canceled (implied by first arg being undefined)
  const [newpagenum0, valid0, canceled0, errmsg0] = validatePageNum(undefined, 5)
  assert.true(valid0)
  assert.true(canceled0)
  assert.equal(typeof newpagenum0, 'undefined')
  assert.equal(errmsg0, '')

  const [newpagenum1, valid1, canceled1, errmsg1] = validatePageNum('nonsense', 5)
  assert.false(valid1)
  assert.false(canceled1)
  assert.equal(typeof newpagenum1, 'undefined')
  assert.equal(errmsg1, 'Error: [nonsense] is not an integer.\n')

  const [newpagenum2, valid2, canceled2, errmsg2] = validatePageNum('-2', 5)
  assert.false(valid2)
  assert.false(canceled2)
  assert.equal(typeof newpagenum2, 'undefined')
  assert.equal(errmsg2, 'Error: [-2] must be greater than 0.\n')

  const [newpagenum3, valid3, canceled3, errmsg3] = validatePageNum('0', 5)
  assert.false(valid3)
  assert.false(canceled3)
  assert.equal(typeof newpagenum3, 'undefined')
  assert.equal(errmsg3, 'Error: [0] must be greater than 0.\n')

  const [newpagenum4, valid4, canceled4, errmsg4] = validatePageNum('6', 5)
  assert.false(valid4)
  assert.false(canceled4)
  assert.equal(typeof newpagenum4, 'undefined')
  assert.equal(errmsg4, 'Error: [6] must be less than or equal to the number of pages: [5].\n')

  const [newpagenum5, valid5, canceled5, errmsg5] = validatePageNum('3', 5)
  assert.true(valid5)
  assert.false(canceled5)
  assert.equal(newpagenum5, 3)
  assert.equal(errmsg5, '')

  assert.true(true)
})

/* eslint-enable no-undef */
