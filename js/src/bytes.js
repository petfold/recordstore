// Byte, hex and text helpers shared by the other modules.
//
// Trie keys are handled as lowercase hex of their UTF-8 bytes. Hex keeps
// byte order under plain string comparison (so sorted hex is sorted UTF-8 is
// sorted code points, the order Python uses), slices on byte boundaries at
// even offsets, and is the form trie nodes store prefixes in anyway.

const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });

const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'));

/** Lowercase hex of a byte array. */
export function toHex(bytes) {
  let out = '';
  for (let i = 0; i < bytes.length; i++) out += HEX[bytes[i]];
  return out;
}

/** Bytes of an even-length hex string (either case). */
export function fromHex(hex) {
  if (typeof hex !== 'string' || hex.length % 2 !== 0 || /[^0-9a-fA-F]/.test(hex)) {
    throw new TypeError(`not a hex string: ${String(hex).slice(0, 80)}`);
  }
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(hex.slice(2 * i, 2 * i + 2), 16);
  return out;
}

/** Concatenate byte arrays. */
export function concatBytes(...parts) {
  let size = 0;
  for (const p of parts) size += p.length;
  const out = new Uint8Array(size);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

/**
 * Does `s` contain a lone surrogate? Python cannot encode one to UTF-8 and
 * refuses; TextEncoder would silently write U+FFFD instead, which would
 * give a different key or value than the caller passed. So we refuse too.
 */
export function isWellFormed(s) {
  if (typeof s.isWellFormed === 'function') return s.isWellFormed();
  return !/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(s);
}

/** UTF-8 bytes of a string; a lone surrogate is an error. */
export function utf8(s) {
  if (!isWellFormed(s)) throw new TypeError('string contains a lone surrogate (not encodable as UTF-8)');
  return encoder.encode(s);
}

/** String from UTF-8 bytes; invalid UTF-8 is an error. */
export function fromUtf8(bytes) {
  return decoder.decode(bytes);
}

/** Lowercase hex of a string's UTF-8 bytes. */
export function utf8Hex(s) {
  return toHex(utf8(s));
}

/** The string whose UTF-8 bytes `hex` is. */
export function hexUtf8(hex) {
  return fromUtf8(fromHex(hex));
}

/** Bytes from base64 (what Freedom's provider returns for reads). */
export function fromBase64(b64) {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

/**
 * Order two strings by Unicode code point, as Python sorts `str`.
 * JavaScript's default order is by UTF-16 code unit, which puts characters
 * above U+FFFF (surrogate pairs, 0xD800...) before U+E000-U+FFFF.
 */
export function compareCodePoints(a, b) {
  const ia = a[Symbol.iterator]();
  const ib = b[Symbol.iterator]();
  for (;;) {
    const x = ia.next();
    const y = ib.next();
    if (x.done || y.done) return x.done === y.done ? 0 : x.done ? -1 : 1;
    const cx = x.value.codePointAt(0);
    const cy = y.value.codePointAt(0);
    if (cx !== cy) return cx < cy ? -1 : 1;
  }
}
