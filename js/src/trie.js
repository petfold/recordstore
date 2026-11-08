// The canonical persistent radix trie, ported from Python's `_Trie`.
//
// Each node is one blob, the canonical JSON of
//   {"c": {<first byte, 2 hex>: <child ref>, ...}, "p": <prefix hex>,
//    "tn": 1, "v": <value ref or null>}
// A child's prefix excludes the byte its parent branched on. Two invariants
// make the trie's shape a pure function of its contents (hence equal content
// => equal root): a node with no value and no children does not exist, and a
// node with no value and one child is merged into that child.
//
// Keys are lowercase hex of their UTF-8 bytes throughout (see bytes.js).
// Operations take a root and return a new root; nodes built during a commit
// are kept as `pending:N` placeholders and written by `flush`, bottom-up,
// one batch per level, only those reachable from the final root.

import { canonicalBytes } from './canonical.js';
import { fromUtf8 } from './bytes.js';

/** Default bound on the decoded-node cache (nodes are immutable and re-fetchable). */
export const DEFAULT_NODE_CACHE_SIZE = 65536;

/** Most nodes a walk loads ahead of where it is. */
const WALK_LOOKAHEAD = 4096;

export class TrieNode {
  /**
   * @param {string} prefix  hex
   * @param {string | null} valueRef
   * @param {Map<string, string>} children  first byte (2 hex) -> child ref
   */
  constructor(prefix, valueRef, children) {
    this.prefix = prefix;
    this.valueRef = valueRef;
    this.children = children;
  }
}

/** Raised by `Trie.delete` for a key the trie does not hold. */
export class KeyMissing extends Error {
  constructor(key) {
    super(`key not in trie: ${key}`);
    this.name = 'KeyMissing';
  }
}

/** Bounded LRU over decoded nodes (Map iteration order is insertion order). */
class NodeCache {
  constructor(maxsize) {
    this.maxsize = Math.max(1, maxsize);
    this.map = new Map();
  }

  get(ref) {
    const node = this.map.get(ref);
    if (node !== undefined) {
      this.map.delete(ref);
      this.map.set(ref, node);
    }
    return node;
  }

  has(ref) {
    return this.map.has(ref);
  }

  set(ref, node) {
    this.map.delete(ref);
    this.map.set(ref, node);
    while (this.map.size > this.maxsize) this.map.delete(this.map.keys().next().value);
  }
}

/** Longest common prefix of two hex strings, on a byte boundary. */
export function commonPrefix(a, b) {
  const n = Math.min(a.length, b.length);
  let i = 0;
  while (i < n && a[i] === b[i]) i++;
  return a.slice(0, i - (i % 2));
}

/** A hex string's prefixes agree up to the shorter of the two. */
function canMeet(full, prefix) {
  const probe = Math.min(full.length, prefix.length);
  return full.slice(0, probe) === prefix.slice(0, probe);
}

const sortedBytes = (children) => [...children.keys()].sort();

export class Trie {
  /**
   * @param {{get: Function, put: Function, getMany?: Function, putMany?: Function}} blobs
   * @param {number} [cacheSize]
   */
  constructor(blobs, cacheSize = DEFAULT_NODE_CACHE_SIZE) {
    this.blobs = blobs;
    this.cache = new NodeCache(cacheSize);
    /** @type {Map<string, TrieNode>} */
    this.pending = new Map();
    this.pn = 0;
  }

  // -- node io --------------------------------------------------------------

  /** Decode a node blob. */
  static decode(bytes) {
    const obj = JSON.parse(fromUtf8(bytes));
    if (obj === null || typeof obj !== 'object' || obj.tn !== 1) {
      throw new Error('not a trie node or unsupported version');
    }
    const children = new Map();
    for (const [k, ref] of Object.entries(obj.c)) {
      const b = parseInt(k, 16);
      if (!(b >= 0 && b <= 255)) throw new Error(`bad child key in trie node: ${k}`);
      children.set(b.toString(16).padStart(2, '0'), ref);
    }
    return new TrieNode(obj.p.toLowerCase(), obj.v, children);
  }

  /** The canonical bytes of a node. */
  static serialize(node) {
    return canonicalBytes({
      tn: 1,
      p: node.prefix,
      v: node.valueRef,
      c: Object.fromEntries(node.children),
    });
  }

  /** At hand without a fetch: a placeholder of the commit being built, or cached. */
  has(ref) {
    return this.pending.has(ref) || this.cache.has(ref);
  }

  /** A node if at hand, `undefined` if not; a `null` side stays `null`. */
  peek(side) {
    if (typeof side !== 'string') return side;
    return this.pending.get(side) || this.cache.get(side);
  }

  async load(ref) {
    const pending = this.pending.get(ref);
    if (pending) return pending;
    let node = this.cache.get(ref);
    if (!node) {
      node = Trie.decode(await this.blobs.get(ref));
      this.cache.set(ref, node);
    }
    return node;
  }

  /** Load several nodes, fetching the uncached ones in one batch. */
  async loadMany(refs) {
    const out = new Map();
    const unique = [...new Set(refs)];
    for (const r of unique) {
      const node = this.pending.get(r) || this.cache.get(r);
      if (node) out.set(r, node);
    }
    const missing = unique.filter((r) => !out.has(r));
    if (missing.length) {
      let blobs;
      if (typeof this.blobs.getMany === 'function') {
        blobs = await this.blobs.getMany(missing);
      } else {
        blobs = new Map();
        for (const r of missing) blobs.set(r, await this.blobs.get(r));
      }
      for (const r of missing) {
        const node = Trie.decode(blobs.get(r));
        this.cache.set(r, node);
        out.set(r, node); // held here even if the LRU drops it
      }
    }
    return out;
  }

  /** Defer a node: a placeholder, resolved to its real reference by `flush`. */
  store(node) {
    const pid = `pending:${this.pn++}`;
    this.pending.set(pid, node);
    return pid;
  }

  /**
   * Write the placeholders reachable from `root`, bottom-up, one batch per
   * level, and return the real root reference. Orphaned intermediates are
   * never written.
   */
  async flush(root) {
    if (root === null || !this.pending.has(root)) return root;
    const reachable = new Set();
    const stack = [root];
    while (stack.length) {
      const pid = stack.pop();
      if (reachable.has(pid)) continue;
      reachable.add(pid);
      for (const c of this.pending.get(pid).children.values()) if (this.pending.has(c)) stack.push(c);
    }
    const resolved = new Map();
    const remaining = new Set(reachable);
    while (!resolved.has(root)) {
      const ready = [...remaining].filter((pid) =>
        [...this.pending.get(pid).children.values()].every((c) => !this.pending.has(c) || resolved.has(c)));
      if (!ready.length) throw new Error('trie flush stalled: no writable nodes');
      const batch = ready.map((pid) => {
        const node = this.pending.get(pid);
        const children = new Map([...node.children].map(([b, c]) => [b, resolved.get(c) ?? c]));
        const real = new TrieNode(node.prefix, node.valueRef, children);
        return [pid, real, Trie.serialize(real)];
      });
      const datas = batch.map((entry) => entry[2]);
      let refs;
      if (typeof this.blobs.putMany === 'function') {
        refs = await this.blobs.putMany(datas);
      } else {
        refs = [];
        for (const d of datas) refs.push(await this.blobs.put(d));
      }
      batch.forEach(([pid, real], i) => {
        resolved.set(pid, refs[i]);
        this.cache.set(refs[i], real);
        remaining.delete(pid);
      });
    }
    return resolved.get(root);
  }

  /** Drop the placeholders of a finished (or failed) build. */
  reset() {
    this.pending.clear();
    this.pn = 0;
  }

  // -- reads ----------------------------------------------------------------

  /** The value reference under `key` (hex), or null. */
  async get(root, key) {
    while (root !== null) {
      const node = await this.load(root);
      if (!key.startsWith(node.prefix)) return null;
      key = key.slice(node.prefix.length);
      if (key === '') return node.valueRef;
      root = node.children.get(key.slice(0, 2)) ?? null;
      key = key.slice(2);
    }
    return null;
  }

  /**
   * `get` for many keys at once, level by level, so a network store pays
   * about one round trip per trie level rather than one per node.
   * @returns {Promise<Map<string, string | null>>} key hex -> value ref or null
   */
  async getMany(root, keys) {
    const found = new Map();
    const unique = [...new Set(keys)];
    if (root === null) {
      for (const k of unique) found.set(k, null);
      return found;
    }
    let walking = unique.map((k) => [k, k, root]);
    while (walking.length) {
      const nodes = await this.loadMany(walking.map((w) => w[2]));
      const descending = [];
      for (const [key, rest0, ref] of walking) {
        const node = nodes.get(ref);
        if (!rest0.startsWith(node.prefix)) {
          found.set(key, null);
          continue;
        }
        const rest = rest0.slice(node.prefix.length);
        if (rest === '') {
          found.set(key, node.valueRef);
          continue;
        }
        const child = node.children.get(rest.slice(0, 2));
        if (child === undefined) found.set(key, null);
        else descending.push([key, rest.slice(2), child]);
      }
      walking = descending;
    }
    return found;
  }

  // -- writes (functional: a root in, a new root out) -------------------------

  async insert(root, key, valueRef) {
    if (root === null) return this.store(new TrieNode(key, valueRef, new Map()));
    const node = await this.load(root);
    const common = commonPrefix(node.prefix, key);

    if (common.length < node.prefix.length) {
      // split: demote the existing node under the diverging byte
      const demoted = new TrieNode(node.prefix.slice(common.length + 2), node.valueRef, new Map(node.children));
      const children = new Map([[node.prefix.slice(common.length, common.length + 2), this.store(demoted)]]);
      const rest = key.slice(common.length);
      if (rest === '') return this.store(new TrieNode(common, valueRef, children));
      children.set(rest.slice(0, 2), this.store(new TrieNode(rest.slice(2), valueRef, new Map())));
      return this.store(new TrieNode(common, null, children));
    }

    const rest = key.slice(node.prefix.length);
    if (rest === '') return this.store(new TrieNode(node.prefix, valueRef, new Map(node.children)));
    const children = new Map(node.children);
    const childRef = children.get(rest.slice(0, 2));
    if (childRef === undefined) {
      children.set(rest.slice(0, 2), this.store(new TrieNode(rest.slice(2), valueRef, new Map())));
    } else {
      children.set(rest.slice(0, 2), await this.insert(childRef, rest.slice(2), valueRef));
    }
    return this.store(new TrieNode(node.prefix, node.valueRef, children));
  }

  async delete(root, key) {
    if (root === null) throw new KeyMissing(key);
    const node = await this.load(root);
    if (!key.startsWith(node.prefix)) throw new KeyMissing(key);
    const rest = key.slice(node.prefix.length);

    if (rest === '') {
      if (node.valueRef === null) throw new KeyMissing(key);
      return this.canonicalize(node.prefix, null, new Map(node.children));
    }

    const childRef = node.children.get(rest.slice(0, 2));
    if (childRef === undefined) throw new KeyMissing(key);
    const newChild = await this.delete(childRef, rest.slice(2));
    const children = new Map(node.children);
    if (newChild === null) children.delete(rest.slice(0, 2));
    else children.set(rest.slice(0, 2), newChild);
    return this.canonicalize(node.prefix, node.valueRef, children);
  }

  /** Restore the canonical-form invariants after a removal. */
  async canonicalize(prefix, valueRef, children) {
    if (valueRef === null && children.size === 0) return null;
    if (valueRef === null && children.size === 1) {
      const [[byte, childRef]] = children;
      const child = await this.load(childRef);
      return this.store(new TrieNode(prefix + byte + child.prefix, child.valueRef, new Map(child.children)));
    }
    return this.store(new TrieNode(prefix, valueRef, children));
  }

  // -- walks ------------------------------------------------------------------

  /**
   * Load what a depth-first walk will visit next, level by level: the
   * unloaded refs on its stack (nearest first) and the children of those
   * already loaded, skipping subtrees that cannot hold `prefix`.
   */
  async prefetch(stack, prefix) {
    const budget = Math.max(1, Math.min(WALK_LOOKAHEAD, Math.floor(this.cache.maxsize / 2)));
    let frontier = [...stack].reverse();
    let fetched = 0;
    let visited = 0;
    while (frontier.length && fetched < budget && visited < 2 * budget) {
      const missing = [];
      for (const [ref] of frontier) if (!this.has(ref) && missing.length < budget - fetched) missing.push(ref);
      if (missing.length) {
        await this.loadMany(missing);
        fetched += missing.length;
      }
      const next = [];
      for (const [ref, acc] of frontier) {
        const node = this.peek(ref);
        if (!node) continue; // beyond this round's budget
        visited++;
        const full = acc + node.prefix;
        if (!canMeet(full, prefix)) continue;
        for (const byte of sortedBytes(node.children)) next.push([node.children.get(byte), full + byte]);
      }
      frontier = next;
    }
  }

  /** All [key hex, value ref] with key under `prefix` (hex), in sorted key order. */
  async *items(root, prefix = '') {
    if (root === null) return;
    const stack = [[root, '']];
    while (stack.length) {
      if (!this.has(stack[stack.length - 1][0])) await this.prefetch(stack, prefix);
      const [ref, acc] = stack.pop();
      const node = await this.load(ref);
      const full = acc + node.prefix;
      if (!canMeet(full, prefix)) continue;
      if (node.valueRef !== null && full.startsWith(prefix)) yield [full, node.valueRef];
      for (const byte of sortedBytes(node.children).reverse()) stack.push([node.children.get(byte), full + byte]);
    }
  }

  /**
   * ['node', ref] and ['value', ref] for every blob needed to read the keys
   * under `prefix`: the nodes on the path and in the subtree, and the values.
   */
  async *refsUnder(root, prefix = '') {
    if (root === null) return;
    const stack = [[root, '']];
    while (stack.length) {
      if (!this.has(stack[stack.length - 1][0])) await this.prefetch(stack, prefix);
      const [ref, acc] = stack.pop();
      const node = await this.load(ref);
      const full = acc + node.prefix;
      if (!canMeet(full, prefix)) continue;
      yield ['node', ref];
      if (node.valueRef !== null && full.startsWith(prefix)) yield ['value', node.valueRef];
      for (const byte of sortedBytes(node.children)) stack.push([node.children.get(byte), full + byte]);
    }
  }

  // -- diff (structural, prunes shared subtrees) ------------------------------

  /**
   * [key hex, a value ref | null, b value ref | null] for every key where the
   * two roots differ. Subtrees with equal refs are pruned, so the cost follows
   * the difference, not the dataset.
   */
  async *diff(aRoot, bRoot) {
    if (aRoot === bRoot) return;
    const stack = [[aRoot, bRoot, '']];
    while (stack.length) {
      if (this.unloaded(stack[stack.length - 1])) await this.prefetchPairs(stack);
      const [a, b, acc] = stack.pop();
      const { values, pairs } = Trie.diffStep(await this.asNode(a), await this.asNode(b), acc);
      yield* values;
      stack.push(...pairs);
    }
  }

  /** A pair's side is a ref, a split node (not itself stored), or null. */
  async asNode(side) {
    return typeof side === 'string' ? this.load(side) : side;
  }

  unloaded(pair) {
    return [pair[0], pair[1]].some((x) => typeof x === 'string' && !this.has(x));
  }

  /**
   * One pair of subtrees: the differing values at their tops, and the pairs
   * still to compare.
   */
  static diffStep(a, b, acc) {
    const values = [];
    const pairs = [];
    if (a === null || b === null) {
      const node = b === null ? a : b;
      if (node === null) return { values, pairs };
      const full = acc + node.prefix;
      if (node.valueRef !== null) values.push(b === null ? [full, node.valueRef, null] : [full, null, node.valueRef]);
      for (const [byte, cref] of node.children) {
        pairs.push(b === null ? [cref, null, full + byte] : [null, cref, full + byte]);
      }
      return { values, pairs };
    }

    const pa = a.prefix;
    const pb = b.prefix;
    if (pa === pb) {
      const ka = acc + pa;
      if (a.valueRef !== b.valueRef) values.push([ka, a.valueRef, b.valueRef]);
      for (const byte of new Set([...a.children.keys(), ...b.children.keys()])) {
        const ca = a.children.get(byte) ?? null;
        const cb = b.children.get(byte) ?? null;
        if (ca !== cb) pairs.push([ca, cb, ka + byte]); // equal refs: a shared subtree
      }
      return { values, pairs };
    }

    const common = commonPrefix(pa, pb);
    if (common.length < pa.length && common.length < pb.length) {
      // prefixes diverge: the two subtrees cover disjoint keys
      pairs.push([a, null, acc]);
      pairs.push([null, b, acc]);
    } else if (common.length === pa.length) {
      // a's key is a proper prefix of b's: b lives under one of a's branches
      const ka = acc + pa;
      const bb = pb.slice(pa.length, pa.length + 2);
      const bSplit = new TrieNode(pb.slice(pa.length + 2), b.valueRef, b.children);
      if (a.valueRef !== null) values.push([ka, a.valueRef, null]);
      for (const [byte, cref] of a.children) pairs.push([cref, byte === bb ? bSplit : null, ka + byte]);
      if (!a.children.has(bb)) pairs.push([null, bSplit, ka + bb]);
    } else {
      // b's key is a proper prefix of a's
      const kb = acc + pb;
      const ab = pa.slice(pb.length, pb.length + 2);
      const aSplit = new TrieNode(pa.slice(pb.length + 2), a.valueRef, a.children);
      if (b.valueRef !== null) values.push([kb, null, b.valueRef]);
      for (const [byte, cref] of b.children) pairs.push([byte === ab ? aSplit : null, cref, kb + byte]);
      if (!b.children.has(ab)) pairs.push([aSplit, null, kb + ab]);
    }
    return { values, pairs };
  }

  /** `prefetch` for a diff: load what the diff will visit, level by level. */
  async prefetchPairs(stack) {
    const budget = Math.max(1, Math.min(WALK_LOOKAHEAD, Math.floor(this.cache.maxsize / 2)));
    let frontier = [...stack].reverse();
    let fetched = 0;
    let visited = 0;
    while (frontier.length && fetched < budget && visited < 2 * budget) {
      const missing = [];
      const seen = new Set();
      for (const [a, b] of frontier) {
        for (const side of [a, b]) {
          if (typeof side === 'string' && !this.has(side) && !seen.has(side) && missing.length < budget - fetched) {
            seen.add(side);
            missing.push(side);
          }
        }
      }
      if (missing.length) {
        await this.loadMany(missing);
        fetched += missing.length;
      }
      const next = [];
      for (const [a, b, acc] of frontier) {
        const na = this.peek(a);
        const nb = this.peek(b);
        if (na === undefined || nb === undefined) continue; // beyond this round's budget
        visited++;
        next.push(...Trie.diffStep(na, nb, acc).pairs);
      }
      frontier = next;
    }
  }
}
