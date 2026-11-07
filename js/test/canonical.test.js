import { test } from 'node:test';
import assert from 'node:assert/strict';
import { canonicalBytes, canonicalJson, decodeValue, encodeValue, parseJson, pythonFloatRepr } from '../src/canonical.js';
import { toHex } from '../src/bytes.js';
import { fixture } from './helpers.js';

const canonical = fixture('canonical.json');

test('every value encodes to the bytes Python writes', () => {
  for (const { value, hex } of canonical.values) {
    assert.equal(toHex(canonicalBytes(value)), hex, `value ${canonicalJson(value)}`);
  }
});

test('every record envelope encodes as Python writes it, and decodes back', () => {
  for (const { value, hex } of canonical.records) {
    assert.equal(toHex(encodeValue(value)), hex);
    assert.deepEqual(decodeValue(encodeValue(value)), value);
  }
});

test('large integers round-trip exactly through parseJson as BigInt', () => {
  const big = parseJson('{"n": 9223372036854775808, "m": -1180591620717411303424, "s": 9007199254740991}');
  assert.equal(big.n, 9223372036854775808n);
  assert.equal(big.m, -1180591620717411303424n);
  assert.equal(big.s, 9007199254740991);
  assert.equal(canonicalJson(big), '{"m":-1180591620717411303424,"n":9223372036854775808,"s":9007199254740991}');
});

test('integral numbers below 2**53 are written as integers (the documented divergence from Python floats)', () => {
  assert.equal(canonicalJson(1.0), '1');
  assert.equal(canonicalJson(-0), '0');
  assert.equal(canonicalJson(parseJson('1.0')), '1'); // Python wrote "1.0"; JavaScript cannot tell
});

test('other numbers follow Python float repr', () => {
  const cases = [[0.1, '0.1'], [1e-5, '1e-05'], [1e-4, '0.0001'], [1e16, '1e+16'], [1e15 + 0.5, '1000000000000000.5'],
    [2 ** 53, '9007199254740992.0'], [5e-324, '5e-324'], [-1e100, '-1e+100'], [1.5e-300, '1.5e-300'], [123.456, '123.456']];
  for (const [x, text] of cases) assert.equal(pythonFloatRepr(x), text);
});

test('object keys sort by code point, as Python sorts str', () => {
  // UTF-16 order would put the emoji (a surrogate pair, 0xD83D...) before U+FFFF.
  assert.equal(canonicalJson({ '😀': 1, '￿': 2, z: 3, é: 4 }), '{"z":3,"é":4,"￿":2,"😀":1}');
});

test('what JSON cannot represent is refused, as in Python', () => {
  for (const bad of [NaN, Infinity, -Infinity, undefined, () => 1, Symbol('s'), new Date(0), new Map(),
    { a: undefined }, [undefined], '\ud800', { '\udc00': 1 }]) {
    assert.throws(() => canonicalBytes(bad), `${String(bad)} should be refused`);
  }
  assert.throws(() => canonicalBytes([1, , 3])); // eslint-disable-line no-sparse-arrays
});
