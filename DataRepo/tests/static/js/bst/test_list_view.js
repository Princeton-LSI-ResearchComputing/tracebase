/* eslint-disable no-undef */

// NOTE: We cannot test the methods that trigger a redirect, so we test the supporting methods.

// Page update tests

QUnit.test('updatePage', function (assert) {
  // We have to reset the global variables and delete stale cookies
  initGlobalDefaults()
  deleteViewCookies()

  setViewCookie('page', 5)
  setViewCookie('limit', 25)
  const url = getPageURL(2, 10, 'text')
  assert.true(url.includes('page=2'))
  assert.true(url.includes('limit=10'))
  assert.true(url.includes('export=text'))
})

QUnit.test('onRowsPerPageChange', function (assert) {
  // We have to reset the global variables and delete stale cookies
  initGlobalDefaults()
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

QUnit.test('resetTable', function (assert) {
  // We have to reset the global variables and delete stale cookies
  initGlobalDefaults()
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

// Table content collapse tests

QUnit.test('parseBool', function (assert) {
  assert.false(parseBool())
  assert.false(parseBool(''))
  let myUndef
  assert.false(parseBool(myUndef))
  assert.false(parseBool(myUndef, false))
  assert.true(parseBool(myUndef, true))
  assert.false(parseBool(myUndef, myUndef))
  assert.false(parseBool(myUndef, ''))
  assert.true(parseBool(myUndef, 'true'))
  assert.false(parseBool(myUndef, 'false'))
  assert.true(parseBool(myUndef, 'TRUE'))
  assert.false(parseBool(myUndef, 'FALSE'))
  assert.false(parseBool('false'))
  assert.false(parseBool(false))
  assert.true(parseBool('true'))
  assert.true(parseBool(true))
  assert.false(parseBool('FALSE'))
  assert.true(parseBool('TRUE'))
  assert.true(parseBool('True'))
})

function createTestTable () {
  // This is defined in tests.html and cleaned automatically by QUnit
  const fixture = document.querySelector('#qunit-fixture')

  // Create a button with an icon, initially as an expand button
  const button = document.createElement('button')
  button.name = 'btnCollapse'
  const icon = document.createElement('i')
  icon.id = 'test1'
  icon.classList.add('bi')
  icon.classList.add('bi-arrows-expand')
  button.appendChild(icon)
  fixture.append(button)

  // Create a table with a td containing 'table-cell' and a br containing 'cell-wrap'
  const table = document.createElement('table')
  // This is the default ID defined in DataRepo/static/js/bst/list_view.js
  table.id = 'bstlistviewtable'

  // Create a thead row
  const tr1 = document.createElement('tr')
  const th = document.createElement('th')
  th.setAttribute('data-field', 'testfield')
  tr1.appendChild(th)

  // Create a tbody row
  const tr = document.createElement('tr')
  const td = document.createElement('td')
  td.id = 'test2'
  td.classList.add('table-cell')
  td.classList.add('nobr')
  const textOne = document.createTextNode('First line ')
  const br = document.createElement('br')
  br.id = 'test3'
  br.classList.add('cell-wrap')
  br.classList.add('d-none')
  const textTwo = document.createTextNode('Second line')
  td.appendChild(textOne)
  td.appendChild(br)
  td.appendChild(textTwo)
  tr.appendChild(td)

  const thead = document.createElement('thead')
  thead.appendChild(tr1)
  const tbody = document.createElement('tbody')
  tbody.appendChild(tr)

  table.appendChild(thead)
  table.appendChild(tbody)
  fixture.append(table)

  return fixture
}

function assertCollapsed (assert, fixture, collapsed) {
  if (typeof collapsed === 'undefined') {
    // Test initial state is collapsed
    assert.true(fixture.querySelector('#test1').classList.contains('bi-arrows-expand'))
    assert.false(fixture.querySelector('#test1').classList.contains('bi-arrows-collapse'))
    assert.true(fixture.querySelector('#test2').classList.contains('nobr'))
    assert.true(fixture.querySelector('#test3').classList.contains('d-none'))
  } else if (collapsed) {
    // Test collapsed
    assert.true(parseBool(getViewCookie('collapsed')))
    assert.true(fixture.querySelector('#test1').classList.contains('bi-arrows-expand'))
    assert.false(fixture.querySelector('#test1').classList.contains('bi-arrows-collapse'))
    assert.true(fixture.querySelector('#test2').classList.contains('nobr'))
    assert.true(fixture.querySelector('#test3').classList.contains('d-none'))
  } else {
    // Test NOT collapsed
    assert.false(parseBool(getViewCookie('collapsed')))
    assert.true(fixture.querySelector('#test1').classList.contains('bi-arrows-collapse'))
    assert.false(fixture.querySelector('#test1').classList.contains('bi-arrows-expand'))
    assert.false(fixture.querySelector('#test2').classList.contains('nobr'))
    assert.false(fixture.querySelector('#test3').classList.contains('d-none'))
  }
}

QUnit.test('toggleCollapse', function (assert) {
  // We have to reset the global variables and delete stale cookies
  initGlobalDefaults()
  deleteViewCookies()

  const fixture = createTestTable()

  // Test initial state is collapsed
  assertCollapsed(assert, fixture)

  toggleCollapse()
  // Test toggled to NOT collapsed
  assertCollapsed(assert, fixture, false)

  toggleCollapse()
  // Test toggled to collapsed
  assertCollapsed(assert, fixture, true)
})

QUnit.test('setCollapseIcon', function (assert) {
  deleteViewCookies()
  const fixture = createTestTable()

  // Test initial state is collapsed, so the button icon should represent 'expanding'
  assert.true(fixture.querySelector('#test1').classList.contains('bi-arrows-expand'))
  assert.false(fixture.querySelector('#test1').classList.contains('bi-arrows-collapse'))

  // To make the button show the 'collapse' icon, we supply collapse=true
  setCollapseIcon(true)
  // Test toggled to NOT collapsed
  assert.true(fixture.querySelector('#test1').classList.contains('bi-arrows-collapse'))
  assert.false(fixture.querySelector('#test1').classList.contains('bi-arrows-expand'))

  // To make the button show the 'expand' icon, we supply collapse=false
  setCollapseIcon(false)
  // Test toggled to collapsed
  assert.true(fixture.querySelector('#test1').classList.contains('bi-arrows-expand'))
  assert.false(fixture.querySelector('#test1').classList.contains('bi-arrows-collapse'))
})

QUnit.test('setCollapse', function (assert) {
  // We have to reset the global variables and delete stale cookies
  initGlobalDefaults()
  deleteViewCookies()

  const fixture = createTestTable()

  // Test initial state is collapsed
  assertCollapsed(assert, fixture)

  setCollapse(false)
  // Test toggled to NOT collapsed
  assertCollapsed(assert, fixture, false)

  setCollapse(true)
  // Test toggled to collapsed
  assertCollapsed(assert, fixture, true)
})

QUnit.test('customButtonsFunction', function (assert) {
  const buttonsObj = customButtonsFunction()
  assert.true(buttonsObj.btnClear.text.includes('Reset'))
  assert.equal('bi-house', buttonsObj.btnClear.icon)
  assert.true(Object.hasOwn(buttonsObj.btnClear, 'event'))
  assert.true(buttonsObj.btnClear.attributes.title.includes('Restore default'))

  assert.true(buttonsObj.btnCollapse.text.includes('line-wrap'))
  assert.equal('bi-arrows-expand', buttonsObj.btnCollapse.icon)
  assert.true(Object.hasOwn(buttonsObj.btnCollapse, 'event'))
  assert.true(buttonsObj.btnCollapse.attributes.title.includes('line-wrap'))
})

// Initialization tests

/**
 * This takes a javascript array and returns a method that can be used to override window.alert that appends alert
 * messages to the provided array and does not call the overridden method.
 * Inspired by: https://stackoverflow.com/a/41369753
 * @param {*} alerts A javascript array to which to append alert messages.
 * @returns A function to use to set window.alert.  Remember to restore the original alert function when done.
 */
function alertOverride (alerts) {
  function testAlert () {
    // Record the message args
    message = ''
    for (let i = 0; i < arguments.length; i++) {
      message += arguments[i].toString()
    }
    alerts.push(message)
  }
  return testAlert
}

/**
 * This takes a javascript array and returns a method that can be used to override console.error that appends error
 * messages to the provided array and does not call the overridden method.
 * Inspired by: https://stackoverflow.com/a/41369753
 * @param {*} errors A javascript array to which to append error messages.
 * @returns A function to use to set console.error.  Remember to restore the original error function when done.
 */
function errorOverride (errors) {
  function testError () {
    // Record the message args
    message = ''
    for (let i = 0; i < arguments.length; i++) {
      message += arguments[i].toString()
    }
    errors.push(message)
  }
  return testError
}

QUnit.test('displayWarnings', function (assert) {
  // Override the alert function to capture alerts for testing
  // See: https://stackoverflow.com/a/41369753
  const alertBackup = window.alert
  const alerts = []
  window.alert = alertOverride(alerts)

  // Test no alert when warnings is empty
  displayWarnings()
  displayWarnings([])
  assert.equal(alerts.length, 0)

  displayWarnings(['This is a test of displayWarnings.', 'Warning2.'])

  // Restore the original alert function
  window.alert = alertBackup

  // All warnings are listed in a single message
  assert.equal(alerts.length, 1)
  assert.true(alerts[0].includes('This is a test of displayWarnings.'))
  assert.true(alerts[0].includes('Warning2.'))
})

QUnit.test('getColumnNames', function (assert) {
  createTestTable()
  const result = getColumnNames()
  // 'testfield' is the data-field attribute of the single 'th' element added to the table in createTestTable
  assert.deepEqual(result, ['testfield'])
})

QUnit.test('updateVisible', function (assert) {
  // Override the alert and error functions to capture messages for testing
  // See: https://stackoverflow.com/a/41369753
  const alerts = []
  const errors = []
  const alertBackup = window.alert
  const errorBackup = console.error
  window.alert = alertOverride(alerts)
  console.error = errorOverride(errors)

  initViewCookies('testcookieprefix')

  // Test before table is created to assert the error about no th data-field attributes
  updateVisible('false', 'anyname')
  assert.equal(errors.length, 1)
  assert.equal(errors[0], 'No th data-field attributes found.')
  assert.equal(alerts.length, 1)
  assert.equal(alerts[0], 'Error: Unable to save your column visibility selection')

  // Create the table
  createTestTable()

  // Empty the arrays for the next test
  alerts.splice(0, alerts.length)
  errors.splice(0, errors.length)

  updateVisible('false', 'testfield')
  assert.equal(alerts.length, 0)
  assert.equal(errors.length, 0)
  assert.equal(getViewColumnCookie('testfield', 'visible'), 'false')
  updateVisible('true', 'testfield')
  assert.equal(alerts.length, 0)
  assert.equal(errors.length, 0)
  assert.equal(getViewColumnCookie('testfield', 'visible'), 'true')

  // Now test for an invalid column
  updateVisible('false', 'wrongname')
  assert.equal(errors.length, 1)
  assert.equal(
    errors[0],
    "Column 'wrongname' not found.  The second argument must match a th data-field attribute.  " +
    'Current data-fields: [testfield]'
  )
  assert.equal(alerts.length, 1)
  assert.equal(alerts[0], 'Error: Unable to save your column visibility selection')

  // Restore the original functions
  window.alert = alertBackup
  console.error = errorBackup
})

QUnit.test('initBST', function (assert) {
  // Override the alert function to capture alerts for testing
  // See: https://stackoverflow.com/a/41369753
  const alerts = []
  const alertBackup = window.alert
  window.alert = alertOverride(alerts)

  // Set a test cookie to ensure it gets deleted.  Do so without setting the view cookie prefix & providing it manually,
  // because we also want to test that initBST sets the cookie prefix.
  setCookie('PFX-TC', 'xx')
  // Delete cookies that are tested below, in case they were previously set by another test or in a previous test run.
  deleteCookies(['PFX-limit', 'PFX-page', 'PFX-collapsed'])

  // Create the table
  createTestTable()

  // First call - this satisfies most of the tests
  initBST(
    10, // limit
    15, // limitDefault
    'TTID', // tableID
    'PFX-', // cookiePrefix
    2, // pageNumber
    10, // perPage
    100, // total
    120, // rawTotal
    window.location.href.split('?')[0], // currentURL
    ['WX'], // warnings
    ['TC'], // cookieResets
    'false', // clearCookies
    'sortcol', // sort cookie name
    'asc', // asc cookie name
    'search', // search cookie name
    'filter', // filter cookie name
    'visible', // visible cookie name
    'limit', // limit cookie name
    'page' // page cookie name
  )

  // NOTE: No need to test that cookiePrefix is set.  If it is not, none of the cookie tests would work.

  // Test that cookieResets deletes specific cookies
  assert.equal(getViewCookie('TC'), '')

  // Test that cookies are set
  assert.equal(getViewCookie('limit'), '10')
  assert.equal(getViewCookie('page'), '2')

  // Test that the default collapsed state is true
  assert.equal(getViewCookie('collapsed'), 'true')

  // Test that displayWarnings(warnings) is called
  assert.equal(alerts.length, 1)
  // Reset the alerts
  alerts.splice(0, alerts.length)

  // Second call - this satisfies the tests for the limit being 0 and the clearCookies test
  setViewCookie('TC', 'xx')
  initBST(
    0, // limit
    15, // limitDefault
    'TTID', // tableID
    'PFX-', // cookiePrefix
    2, // pageNumber
    10, // perPage
    100, // total
    120, // rawTotal
    window.location.href.split('?')[0], // currentURL
    [], // warnings
    [], // cookieResets
    'true', // clearCookies
    'sortcol', // sort cookie name
    'asc', // asc cookie name
    'search', // search cookie name
    'filter', // filter cookie name
    'visible', // visible cookie name
    'limit', // limit cookie name
    'page' // page cookie name
  )
  // A limit of 0 is allowed when there is no URL parameter override and it's not coming from a cookie.
  assert.equal(getViewCookie('limit'), '0')
  // Test that clearCookies deletes all cookies (before setting limit and page) by testing an invalid one previously set
  // for the view
  assert.equal(getViewCookie('TC'), '')

  // Third call - this satisfies the tests for the limit cookie being 0, which is overridden to be the limitDefault so
  // that a user can't get locked out of the page be requesting 'all' rows, but there are too many to load and it times
  // out.
  setViewCookie('limit', '0')
  initBST(
    10, // limit
    15, // limitDefault
    // The rest of the parameters don't matter for this test, but they are required.
    'TTID', // tableID
    'PFX-', // cookiePrefix
    2, // pageNumber
    10, // perPage
    100, // total
    120, // rawTotal
    window.location.href.split('?')[0], // currentURL
    [], // warnings
    [], // cookieResets
    'false', // clearCookies,
    'sortcol', // sort cookie name
    'asc', // asc cookie name
    'search', // search cookie name
    'filter', // filter cookie name
    'visible', // visible cookie name
    'limit', // limit cookie name
    'page' // page cookie name
  )
  // A limit of 0 is allowed when there is no URL parameter override.
  assert.equal(getViewCookie('limit'), '15')

  // TODO: Figure out how to set a URL parameter to test that the 'limit' arg and cookie is overridden by the URL limit
  // parameter
  // TODO: Add tests for boostrap table events:
  // - onSort
  // - onSearch
  // - onColumnSearch
  // - onColumnSwitch
  // - onColumnSwitchAll
  // - onLoadError
  // TODO: Add tests for onRowsPerPageChange events

  // Restore the original alert function
  window.alert = alertBackup
})

/* eslint-enable no-undef */
