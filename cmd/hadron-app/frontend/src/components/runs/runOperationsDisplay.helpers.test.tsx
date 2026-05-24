import assert from 'node:assert/strict'
import test from 'node:test'
import { renderToStaticMarkup } from 'react-dom/server'

import { escapeRegExp, operationFilterKey, renderHighlightedText } from './runOperationsDisplay.helpers'

test('operationFilterKey normalizes empty kind to all', () => {
  assert.equal(operationFilterKey(''), 'all')
  assert.equal(operationFilterKey('mcp_call'), 'mcp_call')
})

test('escapeRegExp escapes regex metacharacters', () => {
  assert.equal(escapeRegExp('a+b?c'), 'a\\+b\\?c')
})

test('renderHighlightedText wraps matches in mark tags', () => {
  const html = renderToStaticMarkup(<>{renderHighlightedText('repo status', 'status')}</>)
  assert.match(html, /<mark[^>]*>status<\/mark>/)
})
