import assert from 'node:assert/strict'
import test from 'node:test'

import { unquote } from './string'

test('unquote removes matching surrounding double quotes', () => {
  assert.equal(unquote('  "hello"  '), 'hello')
})

test('unquote removes matching surrounding single quotes', () => {
  assert.equal(unquote("'world'"), 'world')
})

test('unquote leaves unquoted content alone', () => {
  assert.equal(unquote('plain-value'), 'plain-value')
})
