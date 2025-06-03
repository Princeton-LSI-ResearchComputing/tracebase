/* eslint-disable no-undef */

// NOTE: We cannot test the methods that trigger a redirect, so we test the supporting methods.

// Page update tests

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
  table.appendChild(tr)
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

/* eslint-enable no-undef */
