// Canonical encoding: equal values => equal bytes, identical to Python's
//
//   json.dumps(obj, sort_keys=True, separators=(",", ":"),
//              ensure_ascii=False, allow_nan=False).encode("utf-8")
//
// Content addressing makes byte identity a correctness requirement: a value
// encoded differently here than in Python would get a different reference,
// and the store a different root, for the same content.
//
// Numbers are the one place the two languages differ. Python has int and
// float; JavaScript has one number type (plus BigInt). The rules:
//
// - A safe integer (|n| <= 2**53 - 1) is written as an integer, like a
//   Python int. So a Python float such as 1.0 ("1.0"), read here, comes back
//   as 1 and would be written "1": different bytes. Store what JSON
//   represents faithfully, as the Python guide says.
// - Any other finite number is written as Python writes a float (its
//   shortest round-trip repr, exponent from 1e16 and below 1e-4): 0.1,
//   1e-05, 1e+16, 9007199254740992.0.
// - A BigInt is written as an integer, like a Python int of any size, and
//   `parseJson` gives back a BigInt for an integer literal that a number
//   cannot hold exactly (where the engine exposes the literal's source text).
// - NaN and the infinities have no JSON form and are refused, as in Python.

import { compareCodePoints, fromUtf8, isWellFormed, utf8 } from './bytes.js';

/** Canonical JSON text of a JSON-compatible value. */
export function canonicalJson(value) {
  return encode(value, 0);
}

/** Canonical bytes (UTF-8 of `canonicalJson`). */
export function canonicalBytes(value) {
  return utf8(canonicalJson(value));
}

const MAX_DEPTH = 1000; // Python's default recursion limit is of this order

function encode(v, depth) {
  if (depth > MAX_DEPTH) throw new RangeError('value nested too deeply');
  if (v === null) return 'null';
  switch (typeof v) {
    case 'boolean':
      return v ? 'true' : 'false';
    case 'number':
      return encodeNumber(v);
    case 'bigint':
      return v.toString();
    case 'string':
      return encodeString(v);
    case 'object':
      break;
    default:
      throw new TypeError(`not JSON-serializable: ${typeof v}`);
  }
  if (Array.isArray(v)) {
    const parts = [];
    for (let i = 0; i < v.length; i++) {
      if (!(i in v) || v[i] === undefined) throw new TypeError(`not JSON-serializable: array hole or undefined at index ${i}`);
      parts.push(encode(v[i], depth + 1));
    }
    return `[${parts.join(',')}]`;
  }
  const proto = Object.getPrototypeOf(v);
  if (proto !== Object.prototype && proto !== null) {
    throw new TypeError(`not JSON-serializable: ${Object.prototype.toString.call(v)} (only plain objects, arrays and primitives)`);
  }
  if (Object.getOwnPropertySymbols(v).length) throw new TypeError('not JSON-serializable: symbol keys');
  const keys = Object.keys(v).sort(compareCodePoints);
  const parts = [];
  for (const k of keys) {
    if (v[k] === undefined) throw new TypeError(`not JSON-serializable: undefined at key ${JSON.stringify(k)}`);
    parts.push(`${encodeString(k)}:${encode(v[k], depth + 1)}`);
  }
  return `{${parts.join(',')}}`;
}

function encodeString(s) {
  // JSON.stringify escapes exactly what Python's encoder escapes with
  // ensure_ascii=False: " \ and the C0 controls, \b \f \n \r \t short and the
  // rest as lowercase \u00XX; U+007F, U+2028 and U+2029 stay raw in both.
  // The one difference is a lone surrogate, which Python refuses to encode.
  if (!isWellFormed(s)) throw new TypeError('string contains a lone surrogate (not encodable as UTF-8)');
  return JSON.stringify(s);
}

function encodeNumber(x) {
  if (!Number.isFinite(x)) throw new RangeError(`${x} has no canonical JSON form`);
  if (Number.isSafeInteger(x)) return String(x); // String(-0) === "0"
  return pythonFloatRepr(x);
}

/**
 * Python's `repr(float)`: the shortest digits that round-trip (which
 * `toExponential()` also gives), written positionally when the decimal
 * exponent is in [-4, 16) and in exponent form otherwise, with a sign and at
 * least two exponent digits; a positional integral value gets ".0".
 */
export function pythonFloatRepr(x) {
  let s = x.toExponential();
  let sign = '';
  if (s[0] === '-') {
    sign = '-';
    s = s.slice(1);
  }
  const [mantissa, expText] = s.split('e');
  const digits = mantissa.replace('.', '');
  const exp = parseInt(expText, 10);
  const decpt = exp + 1; // digits are 0.d1d2... x 10**decpt
  if (decpt <= -4 || decpt > 16) {
    const m = digits.length > 1 ? `${digits[0]}.${digits.slice(1)}` : digits;
    const e = String(Math.abs(exp)).padStart(2, '0');
    return `${sign}${m}e${exp < 0 ? '-' : '+'}${e}`;
  }
  if (decpt <= 0) return `${sign}0.${'0'.repeat(-decpt)}${digits}`;
  if (decpt >= digits.length) return `${sign}${digits}${'0'.repeat(decpt - digits.length)}.0`;
  return `${sign}${digits.slice(0, decpt)}.${digits.slice(decpt)}`;
}

const INTEGER = /^-?(?:0|[1-9][0-9]*)$/;

function reviver(_key, value, context) {
  if (typeof value === 'number' && !Number.isSafeInteger(value) &&
      context && typeof context.source === 'string' && INTEGER.test(context.source)) {
    return BigInt(context.source);
  }
  return value;
}

/**
 * JSON.parse that keeps large integers exact: an integer literal beyond
 * 2**53 - 1 becomes a BigInt, where the engine passes the literal's source
 * text to revivers (Node 21+, Chrome 114+). Elsewhere it becomes the nearest
 * number, as plain JSON.parse gives.
 */
export function parseJson(text) {
  return JSON.parse(text, reviver);
}

// The record envelope, as Python's `_encode_value` / `_decode_value`.
const SCHEMA_VERSION = 1;

/** Bytes of a record value as stored in its blob. */
export function encodeValue(value) {
  return canonicalBytes({ rsv: SCHEMA_VERSION, val: value });
}

/** The record value a blob holds. */
export function decodeValue(bytes) {
  const obj = parseJson(fromUtf8(bytes));
  if (obj === null || typeof obj !== 'object' || obj.rsv !== SCHEMA_VERSION) {
    throw new Error(`unsupported record schema version: ${JSON.stringify(obj && obj.rsv)}`);
  }
  return obj.val;
}

/** A deep copy that is also a validity check: what a stored value reads back as. */
export function canonicalCopy(value) {
  return parseJson(canonicalJson(value));
}
