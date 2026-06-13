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

/* eslint-enable no-undef */
