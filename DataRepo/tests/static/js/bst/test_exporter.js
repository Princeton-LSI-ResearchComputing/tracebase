/* eslint-disable no-undef */

const normalize = (s) => s.replace(/\s+/g, ' ').trim()

/**
 * Test that generateExportSelect produces expected html, given:
 * exportTypes = [{name: 'test1', url: '/test1_url'}, {name: 'test2', url: 'test2_url'}]
 * See test design in: https://princeton-university.atlassian.net/wiki/x/HIAVH
 */
QUnit.test('generateExportSelect', function (assert) {
  const html = generateExportSelect([{ name: 'test1', url: '/test1_url' }, { name: 'test2', url: 'test2_url' }])
  const expectedHtml = `
              <div class="btn-group">
                    <button type="button"
                            class="btn btn-primary dropdown-toggle"
                            data-bs-toggle="dropdown"
                            aria-expanded="false">
                        <i class="bi bi-download"></i>
                    </button>
                    <ul class="dropdown-menu">
                        <li><a href="/test1_url" class="dropdown-item">test1</a></li>
                        <li><a href="test2_url" class="dropdown-item">test2</a></li>
                    </ul>
              </div>\n`
  assert.strictEqual(
    normalize(html),
    normalize(expectedHtml)
  )

  const div = document.createElement('div')
  div.innerHTML = html

  assert.strictEqual(
    div.querySelectorAll('li').length,
    2
  )

  assert.strictEqual(
    div.querySelector('a').getAttribute('href'),
    '/test1_url'
  )
})

QUnit.test('checkBuiltinExport alerts when builtin export enabled', function (assert) {
  const originalAlert = window.alert

  let alertMsg = null
  window.alert = function (msg) {
    alertMsg = msg
  }

  const fixture = document.getElementById('qunit-fixture')
  fixture.innerHTML = `
    <table id="testtable" data-show-export="true"></table>
  `

  $('#testtable').data('show-export', true)

  checkBuiltinExport('testtable')

  assert.ok(alertMsg)
  assert.ok(alertMsg.includes('data-show-export must be false'))

  window.alert = originalAlert
})

QUnit.test('initExporter returns export menu html', function (assert) {
  // Stub checkBuiltinExport (as we're not testing this)
  const originalCheckBuiltinExport = window.checkBuiltinExport
  window.checkBuiltinExport = function () {}

  const fixture = document.getElementById('qunit-fixture')

  fixture.innerHTML = `
    <script id="export_types" type="application/json">
      [
        {"name":"CSV","url":"/export/csv"},
        {"name":"Excel","url":"/export/xlsx"}
      ]
    </script>

    <table id="testtable"></table>\n`

  const html = initExporter(
    'export_types',
    'testtable'
  )

  const expectedHtml = `
              <div class="btn-group">
                    <button type="button"
                            class="btn btn-primary dropdown-toggle"
                            data-bs-toggle="dropdown"
                            aria-expanded="false">
                        <i class="bi bi-download"></i>
                    </button>
                    <ul class="dropdown-menu">
                        <li><a href="/export/csv" class="dropdown-item">CSV</a></li>
                        <li><a href="/export/xlsx" class="dropdown-item">Excel</a></li>
                    </ul>
              </div>\n`

  assert.strictEqual(
    normalize(html),
    normalize(expectedHtml)
  )

  window.checkBuiltinExport = originalCheckBuiltinExport
})

/* eslint-enable no-undef */
