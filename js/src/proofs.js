// Proofs about committed roots, verifiable with no store, ported from the
// Python package:
//
// - a key proof (format "recordstore-trie-proof"): a key's record, or its
//   absence. The trie is canonical, so a key has exactly one possible place
//   under a root, which is what makes absence provable;
// - a prefix proof ("recordstore-prefix-proof"): every record under a
//   prefix, and no others;
// - an extension proof ("recordstore-extension-proof"): a later root holds
//   every record an earlier root held under some prefixes, unchanged.
//
// An envelope carries the raw node and value blobs; a verifier recomputes
// their references (SHA-256 or Swarm, as the envelope names) and replays the
// walk from the root it trusts. Equal stores give equal envelopes in Python
// and JavaScript. Verifiers return promises (Web Crypto's SHA-256 is async).

import { resolveAddressing } from './address.js';
import { compareCodePoints, fromHex, hexUtf8, isWellFormed, utf8Hex } from './bytes.js';
import { decodeValue } from './canonical.js';
import { commonPrefix, Trie, TrieNode } from './trie.js';

export const PROOF_FORMAT = 'recordstore-trie-proof';
export const PREFIX_FORMAT = 'recordstore-prefix-proof';
export const EXTENSION_FORMAT = 'recordstore-extension-proof';

/** The same symbol as recordstore.js's ABSENT (Symbol.for). */
const ABSENT = Symbol.for('recordstore.ABSENT');

/** A proof (or feed update) that does not verify against the given root. */
export class ProofError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ProofError';
  }
}

function envelope(proof, format) {
  if (!proof || typeof proof !== 'object' || proof.format !== format) throw new ProofError(`not a ${format} envelope`);
  if (proof.version !== 1) throw new ProofError(`unsupported proof version ${JSON.stringify(proof.version)}`);
}

function addressing(name) {
  if (name !== 'sha256' && name !== 'swarm') throw new ProofError(`unknown addressing ${JSON.stringify(name)}`);
  return resolveAddressing(name);
}

function blob(hex, what) {
  try {
    return fromHex(hex);
  } catch {
    throw new ProofError(`malformed ${what} bytes in proof`);
  }
}

function blobs(list, what) {
  if (list === undefined || list === null) return [];
  if (!Array.isArray(list)) throw new ProofError(`malformed ${what} bytes in proof`);
  return list.map((h) => blob(h, what));
}

function decodeNode(bytes, label) {
  try {
    return Trie.decode(bytes);
  } catch {
    throw new ProofError(`${label} is not a valid trie node`);
  }
}

function keyText(hex) {
  try {
    return hexUtf8(hex);
  } catch {
    return hex;
  }
}

function prefixHex(prefix) {
  if (!isWellFormed(prefix)) throw new TypeError('prefix contains a lone surrogate');
  return utf8Hex(prefix);
}

/**
 * The addressing name a proof envelope carries: the bytes store's, which
 * must be one any verifier can recompute ('sha256' or 'swarm').
 */
export function addressingName(store) {
  if (store && (store.addressing === 'sha256' || store.addressing === 'swarm')) return store.addressing;
  throw new TypeError("cannot determine this bytes store's addressing scheme; pass {addressing: 'sha256' | 'swarm'}");
}

// -- key proofs ---------------------------------------------------------------

/**
 * Check a key proof against `root` (the reference the verifier trusts; null
 * for an empty store). Resolves to the proven record, or ABSENT for an
 * absence proof; rejects with ProofError on any mismatch.
 */
export async function verifyProof(proof, root = null) {
  envelope(proof, PROOF_FORMAT);
  if ((proof.root ?? null) !== root) throw new ProofError(`proof is about root ${JSON.stringify(proof.root)}, not ${JSON.stringify(root)}`);
  const refOf = addressing(proof.addressing);
  const { key } = proof;
  if (typeof key !== 'string' || key === '' || !isWellFormed(key)) throw new ProofError('proof carries no key');
  const nodes = blobs(proof.nodes, 'node');

  let expected = root;
  let remaining = utf8Hex(key);
  let verdict = null; // [found, value ref] once the walk concludes
  for (let i = 0; i < nodes.length; i++) {
    if (verdict) throw new ProofError(`node ${i} continues past the walk's conclusion`);
    if ((await refOf(nodes[i])) !== expected) throw new ProofError(`node ${i} does not hash to the expected reference`);
    const node = decodeNode(nodes[i], `node ${i}`);
    if (!remaining.startsWith(node.prefix)) {
      verdict = [false, null]; // diverges inside the prefix
    } else {
      remaining = remaining.slice(node.prefix.length);
      if (remaining === '') {
        verdict = [node.valueRef !== null, node.valueRef];
      } else {
        const child = node.children.get(remaining.slice(0, 2));
        if (child === undefined) {
          verdict = [false, null]; // nowhere to descend
        } else {
          expected = child;
          remaining = remaining.slice(2);
        }
      }
    }
  }
  if (!verdict) {
    if (root === null) verdict = [false, null]; // the empty store holds nothing
    else throw new ProofError('proof ends before the walk does');
  }

  const [found, valueRef] = verdict;
  if (found !== Boolean(proof.present)) throw new ProofError("the proof's claim contradicts its own path");
  if (!found) {
    if (proof.value !== null && proof.value !== undefined) throw new ProofError('absence proof carries a value');
    return ABSENT;
  }
  const value = blob(proof.value, 'value');
  if ((await refOf(value)) !== valueRef) throw new ProofError("value bytes do not hash to the trie's value reference");
  return decodeValue(value);
}

// -- walks shared by prefix and extension proofs ------------------------------

/** Can a subtree whose keys start with `keyPrefix` hold a key starting with `prefix`? */
function meets(keyPrefix, prefix) {
  return keyPrefix.startsWith(prefix) || prefix.startsWith(keyPrefix);
}

/**
 * [key, value ref, null] for every value under `prefix` in the subtree at
 * `node`, in key order; children that cannot hold the prefix are not loaded.
 */
export async function* itemsUnder(load, node, acc, prefix) {
  const stack = [[node, acc]];
  while (stack.length) {
    const [n, a] = stack.pop();
    const full = a + n.prefix;
    if (!meets(full, prefix)) continue;
    if (n.valueRef !== null && full.startsWith(prefix)) yield [full, n.valueRef, null];
    for (const byte of [...n.children.keys()].sort().reverse()) {
      const keyPrefix = full + byte;
      if (meets(keyPrefix, prefix)) stack.push([await load(n.children.get(byte)), keyPrefix]);
    }
  }
}

/**
 * [key, a value ref, b value ref | null] for every record of `a`'s subtree
 * under `prefix` that `b`'s subtree does not hold with the same value: the
 * three shapes two canonical tries can differ in, one way only.
 */
async function* extensionFaults(load, a, b, acc, prefix) {
  if (a === null) return;
  if (!meets(acc + a.prefix, prefix)) return;
  if (b === null) {
    yield* itemsUnder(load, a, acc, prefix);
    return;
  }
  const pa = a.prefix;
  const pb = b.prefix;
  if (pa === pb) {
    const ka = acc + pa;
    if (a.valueRef !== null && ka.startsWith(prefix) && a.valueRef !== b.valueRef) yield [ka, a.valueRef, b.valueRef];
    for (const byte of [...a.children.keys()].sort()) {
      const ca = a.children.get(byte);
      const cb = b.children.get(byte) ?? null;
      const keyPrefix = ka + byte;
      if (ca === cb || !meets(keyPrefix, prefix)) continue; // shared subtree, or outside the prefix
      const na = await load(ca);
      yield* extensionFaults(load, na, cb !== null ? await load(cb) : null, keyPrefix, prefix);
    }
    return;
  }
  const common = commonPrefix(pa, pb);
  if (common.length < pa.length && common.length < pb.length) {
    yield* itemsUnder(load, a, acc, prefix); // disjoint key ranges
  } else if (common.length === pa.length) {
    // a's edge is a proper prefix of b's: b lives under one of a's branches
    const ka = acc + pa;
    const bb = pb.slice(pa.length, pa.length + 2);
    const bSplit = new TrieNode(pb.slice(pa.length + 2), b.valueRef, b.children);
    if (a.valueRef !== null && ka.startsWith(prefix)) yield [ka, a.valueRef, null];
    for (const byte of [...a.children.keys()].sort()) {
      const keyPrefix = ka + byte;
      if (!meets(keyPrefix, prefix)) continue;
      const child = await load(a.children.get(byte));
      if (byte === bb) yield* extensionFaults(load, child, bSplit, keyPrefix, prefix);
      else yield* itemsUnder(load, child, keyPrefix, prefix);
    }
  } else {
    // b's edge is a proper prefix of a's: a lives under one of b's branches
    const kb = acc + pb;
    const ab = pa.slice(pb.length, pb.length + 2);
    const aSplit = new TrieNode(pa.slice(pb.length + 2), a.valueRef, a.children);
    const cb = b.children.get(ab) ?? null;
    yield* extensionFaults(load, aSplit, cb !== null ? await load(cb) : null, kb + ab, prefix);
  }
}

/** The records `base` held under `prefix` (hex) that `root` lacks or changed. */
export async function* extensionWalk(load, base, root, prefix) {
  if (base === null || base === root) return; // nothing held before, or the same state
  const a = await load(base);
  yield* extensionFaults(load, a, root !== null ? await load(root) : null, '', prefix);
}

/** Prefixes as an extension proof names them: unique, in code point order, at least one. */
export function checkPrefixes(prefixes) {
  if (typeof prefixes === 'string') throw new TypeError('prefixes is a list of strings, not one string');
  const list = [...prefixes];
  if (!list.length || !list.every((p) => typeof p === 'string')) {
    throw new TypeError("an extension proof names at least one string prefix ('' for every key)");
  }
  return [...new Set(list)].sort(compareCodePoints);
}

/** A loader over the blobs an envelope carries, indexed by their recomputed references. */
async function carried(list, refOf, what) {
  const byRef = new Map();
  for (const b of blobs(list, what)) byRef.set(await refOf(b), b);
  return byRef;
}

function loaderOver(nodes) {
  return async (ref) => {
    const bytes = nodes.get(ref);
    if (!bytes) throw new ProofError(`the proof lacks node ${ref}`);
    return decodeNode(bytes, `node ${ref}`);
  };
}

// -- prefix proofs ------------------------------------------------------------

/**
 * Check a prefix proof against `root` and resolve to a Map of every record
 * under its prefix, proven complete; ProofError on any mismatch, including a
 * node or value the walk needs and the proof lacks.
 */
export async function verifyPrefix(proof, root = null) {
  envelope(proof, PREFIX_FORMAT);
  if ((proof.root ?? null) !== root) throw new ProofError(`proof is about root ${JSON.stringify(proof.root)}, not ${JSON.stringify(root)}`);
  const { prefix } = proof;
  if (typeof prefix !== 'string' || !isWellFormed(prefix)) throw new ProofError('proof carries no prefix');
  const refOf = addressing(proof.addressing);
  const nodes = await carried(proof.nodes, refOf, 'node');
  const values = await carried(proof.values, refOf, 'value');
  const load = loaderOver(nodes);
  const out = new Map();
  if (root === null) return out; // the empty store holds nothing
  for await (const [key, valueRef] of itemsUnder(load, await load(root), '', utf8Hex(prefix))) {
    const value = values.get(valueRef);
    if (!value) throw new ProofError(`the proof lacks the value of ${JSON.stringify(keyText(key))}`);
    out.set(hexUtf8(key), decodeValue(value));
  }
  return out;
}

// -- extension proofs ---------------------------------------------------------

/**
 * Check that `root` extends `base` under the proof's prefixes (every record
 * `base` held under them is held by `root` with the same value) and resolve
 * to the prefixes proven. `base` and `root` are the references the verifier
 * trusts (null: the empty store).
 */
export async function verifyExtension(proof, base = null, root = null) {
  envelope(proof, EXTENSION_FORMAT);
  if ((proof.base ?? null) !== base || (proof.root ?? null) !== root) {
    throw new ProofError(`proof is about ${JSON.stringify(proof.base)} -> ${JSON.stringify(proof.root)}, not ${JSON.stringify(base)} -> ${JSON.stringify(root)}`);
  }
  const refOf = addressing(proof.addressing);
  let prefixes;
  try {
    prefixes = checkPrefixes(proof.prefixes || []);
  } catch (e) {
    throw new ProofError(e.message);
  }
  if (!Array.isArray(proof.prefixes) || prefixes.length !== proof.prefixes.length || prefixes.some((p, i) => p !== proof.prefixes[i])) {
    throw new ProofError("the proof's prefixes are not in canonical order");
  }
  const load = loaderOver(await carried(proof.nodes, refOf, 'node'));
  for (const prefix of prefixes) {
    if (!isWellFormed(prefix)) throw new ProofError('a prefix contains a lone surrogate');
    for await (const [key, , b] of extensionWalk(load, base, root, utf8Hex(prefix))) {
      throw new ProofError(`${JSON.stringify(keyText(key))} ${b === null ? 'is absent from' : 'changed in'} the later root`);
    }
  }
  return prefixes;
}

export { prefixHex };
