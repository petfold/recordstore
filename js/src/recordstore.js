// RecordStore: a staged, versioned key -> record store over a bytes store,
// ported from Python's `RecordStore`. Same data model and wire format; an
// asynchronous, JavaScript-shaped API:
//
// - reads and commits return promises, iteration is `for await`;
// - `get` resolves to `undefined` for an absent key (records can be null);
// - `delete` resolves to `false` for an absent key instead of raising;
// - `has` is Python's `contains`.

import { canonicalCopy, decodeValue, encodeValue } from './canonical.js';
import { hexUtf8, isWellFormed, utf8Hex } from './bytes.js';
import { DEFAULT_NODE_CACHE_SIZE, KeyMissing, Trie } from './trie.js';

/** A side of a diff or merge where the key is absent. */
export const ABSENT = Symbol.for('recordstore.ABSENT');
/** What a merge resolver returns to drop a key. */
export const DELETE = Symbol.for('recordstore.DELETE');

const TOMBSTONE = Symbol('tombstone');

/** Both sides of a merge changed the same keys to different values. */
export class MergeConflict extends Error {
  constructor(conflicts) {
    const keys = [...conflicts];
    const shown = keys.slice().sort().slice(0, 5).join(', ') + (keys.length > 5 ? ', ...' : '');
    super(`unresolved merge conflict on ${keys.length} key(s): ${shown}`);
    this.name = 'MergeConflict';
    this.conflicts = keys;
  }
}

/**
 * The key exists under the committed root, but a blob on its path or its
 * value cannot be read right now (missing from the store, network failure).
 * Never reported as absence.
 */
export class RecordUnavailable extends Error {
  constructor(message, cause) {
    super(message, { cause });
    this.name = 'RecordUnavailable';
  }
}

/** UTF-8 hex of a key, after checking it is a non-empty well-formed string. */
function keyHex(key) {
  if (typeof key !== 'string' || key === '') throw new TypeError('key must be a non-empty string');
  if (!isWellFormed(key)) throw new TypeError('key contains a lone surrogate (not encodable as UTF-8)');
  return utf8Hex(key);
}

function prefixHex(prefix) {
  if (typeof prefix !== 'string') throw new TypeError("prefix is a string ('' for every key)");
  if (!isWellFormed(prefix)) throw new TypeError('prefix contains a lone surrogate');
  return utf8Hex(prefix);
}

async function fetchBlobs(blobs, refs) {
  if (!refs.length) return new Map();
  if (typeof blobs.getMany === 'function') return blobs.getMany(refs);
  const out = new Map();
  for (const r of refs) out.set(r, await blobs.get(r));
  return out;
}

async function putBlobs(blobs, datas) {
  if (!datas.length) return [];
  if (typeof blobs.putMany === 'function') return blobs.putMany(datas);
  const refs = [];
  for (const d of datas) refs.push(await blobs.put(d));
  return refs;
}

/** Values are fetched in windows of this many while iterating. */
const WINDOW = 256;

export class RecordStore {
  /**
   * @param {object} blobs  a bytes store
   * @param {{root?: string | null, pointer?: object | null, readonly?: boolean,
   *          nodeCacheSize?: number}} [options]
   *   `root`: the committed state to start from (null: empty). With a
   *   `pointer` and no root, use `RecordStore.open`, which reads the
   *   pointer first.
   */
  constructor(blobs, { root = null, pointer = null, readonly = false, nodeCacheSize = DEFAULT_NODE_CACHE_SIZE } = {}) {
    this._blobs = blobs;
    this._trie = new Trie(blobs, nodeCacheSize);
    this._root = root;
    this._pointer = pointer;
    this._readonly = readonly;
    /** @type {Map<string, any>} key -> staged value or TOMBSTONE */
    this._staged = new Map();
  }

  /** A store at its pointer's current root. */
  static async open(blobs, { pointer = null, ...options } = {}) {
    const root = pointer ? await pointer.get() : null;
    return new RecordStore(blobs, { ...options, root, pointer });
  }

  /** A read-only snapshot of any root. */
  static at(root, blobs) {
    return new RecordStore(blobs, { root, readonly: true });
  }

  /** Root of the last committed state (staged changes not included). */
  get root() {
    return this._root;
  }

  /** The underlying bytes store. */
  get blobs() {
    return this._blobs;
  }

  /** Number of staged, uncommitted changes. */
  get stagedCount() {
    return this._staged.size;
  }

  // -- reading --------------------------------------------------------------

  /** The record under `key` (a fresh copy), or `undefined` if absent. */
  async get(key) {
    const kh = keyHex(key);
    if (this._staged.has(key)) {
      const staged = this._staged.get(key);
      return staged === TOMBSTONE ? undefined : canonicalCopy(staged);
    }
    let vref;
    try {
      vref = await this._trie.get(this._root, kh);
    } catch (e) {
      throw new RecordUnavailable(`record ${JSON.stringify(key)}: a trie node on its path is unreachable: ${e.message}`, e);
    }
    if (vref === null) return undefined;
    let blob;
    try {
      blob = await this._blobs.get(vref);
    } catch (e) {
      throw new RecordUnavailable(`record ${JSON.stringify(key)} exists (value ${vref.slice(0, 16)}...) but its bytes are unreachable: ${e.message}`, e);
    }
    return decodeValue(blob);
  }

  /** Is there a record under `key`? */
  async has(key) {
    return (await this.get(key)) !== undefined;
  }

  /**
   * The records under several keys, read together (about one round trip per
   * trie level, and the values in one batch). Absent keys are left out.
   * @returns {Promise<Map<string, any>>}
   */
  async getMany(keys) {
    const out = new Map();
    const walk = new Map(); // key hex -> key
    for (const key of keys) {
      const kh = keyHex(key);
      if (this._staged.has(key)) {
        const staged = this._staged.get(key);
        if (staged !== TOMBSTONE) out.set(key, canonicalCopy(staged));
      } else {
        walk.set(kh, key);
      }
    }
    let wanted;
    let blobs;
    try {
      const vrefs = await this._trie.getMany(this._root, [...walk.keys()]);
      wanted = new Map();
      for (const [kh, vref] of vrefs) if (vref !== null) wanted.set(walk.get(kh), vref);
      blobs = await fetchBlobs(this._blobs, [...new Set(wanted.values())].sort());
    } catch (e) {
      throw new RecordUnavailable(`${walk.size} records: part of their trie or values is unreachable: ${e.message}`, e);
    }
    for (const [key, vref] of wanted) out.set(key, decodeValue(blobs.get(vref)));
    return out;
  }

  /**
   * Committed and staged entries under `prefix`, in sorted key order, as
   * [key, value ref | null, staged value | undefined]; tombstones dropped.
   */
  async *_merged(prefix) {
    const ph = prefixHex(prefix);
    const staged = [...this._staged.keys()]
      .map((k) => [utf8Hex(k), k])
      .filter(([kh]) => kh.startsWith(ph))
      .sort((x, y) => (x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0));
    let si = 0;
    for await (const [kh, vref] of this._trie.items(this._root, ph)) {
      while (si < staged.length && staged[si][0] < kh) {
        const sk = staged[si++][1];
        const v = this._staged.get(sk);
        if (v !== TOMBSTONE) yield [sk, null, v];
      }
      if (si < staged.length && staged[si][0] === kh) {
        const sk = staged[si++][1];
        const v = this._staged.get(sk);
        if (v !== TOMBSTONE) yield [sk, null, v];
      } else {
        yield [hexUtf8(kh), vref, undefined];
      }
    }
    while (si < staged.length) {
      const sk = staged[si++][1];
      const v = this._staged.get(sk);
      if (v !== TOMBSTONE) yield [sk, null, v];
    }
  }

  /** Sorted keys under `prefix` (code point order, as Python), staged changes included. */
  async *keys(prefix = '') {
    for await (const [key] of this._merged(prefix)) yield key;
  }

  /** Sorted [key, value] under `prefix`, staged changes included; values fetched a window at a time. */
  async *items(prefix = '') {
    let buf = [];
    let refs = [];
    const flush = async function* (store) {
      const blobs = await fetchBlobs(store._blobs, refs);
      for (const [key, vref, staged] of buf) {
        yield [key, vref === null ? canonicalCopy(staged) : decodeValue(blobs.get(vref))];
      }
    };
    for await (const entry of this._merged(prefix)) {
      buf.push(entry);
      if (entry[1] !== null) refs.push(entry[1]);
      if (refs.length >= WINDOW) {
        yield* flush(this);
        buf = [];
        refs = [];
      }
    }
    if (buf.length) yield* flush(this);
  }

  /**
   * [key, mine, theirs] for every key whose value differs between this
   * store's committed root and `otherRoot`; a side without the key is
   * `ABSENT`. Cost follows the difference, not the dataset. Keys arrive in
   * no particular order.
   */
  async *diff(otherRoot) {
    let buf = [];
    const flush = async function* (store) {
      const refs = [...new Set(buf.flatMap(([, a, b]) => [a, b]).filter((r) => r !== null))];
      const blobs = await fetchBlobs(store._blobs, refs);
      for (const [kh, mine, theirs] of buf) {
        yield [hexUtf8(kh),
          mine === null ? ABSENT : decodeValue(blobs.get(mine)),
          theirs === null ? ABSENT : decodeValue(blobs.get(theirs))];
      }
    };
    for await (const entry of this._trie.diff(this._root, otherRoot)) {
      buf.push(entry);
      if (buf.length >= WINDOW) {
        yield* flush(this);
        buf = [];
      }
    }
    if (buf.length) yield* flush(this);
  }

  /** ['node', ref] / ['value', ref] for every blob the keys under `prefix` need in the committed root. */
  refsUnder(prefix = '') {
    return this._trie.refsUnder(this._root, prefixHex(prefix));
  }

  // -- writing --------------------------------------------------------------

  /** Stage `value` under `key` (validated and copied now, written on commit). */
  put(key, value) {
    if (this._readonly) throw new TypeError('read-only snapshot');
    keyHex(key);
    this._staged.set(key, canonicalCopy(value));
  }

  /** Stage the removal of `key`; resolves to false (staging nothing) if there is no such record. */
  async delete(key) {
    if (this._readonly) throw new TypeError('read-only snapshot');
    const kh = keyHex(key);
    if (this._staged.has(key)) {
      if (this._staged.get(key) === TOMBSTONE) return false; // already deleted in this batch
    } else if ((await this._trie.get(this._root, kh)) === null) {
      return false;
    }
    this._staged.set(key, TOMBSTONE);
    return true;
  }

  /** Apply the staged changes on top of `base`; return the new root. */
  async _buildRoot(base) {
    const order = [...this._staged.keys()].map((k) => [utf8Hex(k), k])
      .sort((x, y) => (x[0] < y[0] ? -1 : x[0] > y[0] ? 1 : 0));
    const writes = order.filter(([, k]) => this._staged.get(k) !== TOMBSTONE);
    const refs = await putBlobs(this._blobs, writes.map(([, k]) => encodeValue(this._staged.get(k))));
    const vref = new Map(writes.map(([kh], i) => [kh, refs[i]]));
    try {
      let root = base;
      for (const [kh, k] of order) {
        if (this._staged.get(k) === TOMBSTONE) {
          try {
            root = await this._trie.delete(root, kh);
          } catch (e) {
            if (!(e instanceof KeyMissing)) throw e; // deleted a key that never existed
          }
        } else {
          root = await this._trie.insert(root, kh, vref.get(kh));
        }
      }
      return await this._trie.flush(root);
    } finally {
      this._trie.reset();
    }
  }

  /**
   * Write the staged changes and return the new root; then move the pointer,
   * if there is one. With `reconcile`, a pointer that moved since this store
   * read it is three-way merged into instead of overwritten (`resolver`
   * settles conflicts), retrying up to `retries` times.
   */
  async commit({ reconcile = false, resolver = null, retries = 5 } = {}) {
    if (this._readonly) throw new TypeError('read-only snapshot');
    const base = this._root;
    let root = await this._buildRoot(base);
    if (this._pointer) {
      if (reconcile) root = await this._reconcile(base, root, resolver, retries);
      else await this._pointer.set(root);
    }
    this._staged.clear();
    this._root = root;
    return root;
  }

  async _reconcile(base, root, resolver, retries) {
    const pointer = this._pointer;
    const cas = typeof pointer.compareAndSet === 'function' ? pointer.compareAndSet.bind(pointer) : null;
    let expected = base;
    for (let i = 0; i < Math.max(1, retries); i++) {
      const current = await pointer.get();
      if (current === expected) {
        if (!cas) {
          await pointer.set(root); // best effort: no compare-and-set
          return root;
        }
        if (await cas(expected, root)) return root;
        continue; // lost the race: read again
      }
      root = await RecordStore.merge(this._blobs, expected, root, current, resolver);
      expected = current;
    }
    throw new Error(`commit could not reconcile the pointer after ${retries} tries`);
  }

  // -- merge ----------------------------------------------------------------

  /**
   * Three-way merge of two roots that diverged from `base`; resolves to the
   * merged root. A change on one side is taken, the same change on both
   * taken once, different changes to one key are a conflict, settled by
   * `resolver(key, base, ours, theirs)` (values or ABSENT; return a value or
   * DELETE; may be async) or else raised as MergeConflict.
   */
  static async merge(blobs, base, ours, theirs, resolver = null) {
    if (ours === theirs) return ours;
    if (ours === base) return theirs;
    if (theirs === base) return ours;

    const trie = new Trie(blobs);
    const ourDiff = new Map();
    for await (const [k, bv, sv] of trie.diff(base, ours)) ourDiff.set(k, [bv, sv]);
    const theirDiff = new Map();
    for await (const [k, bv, sv] of trie.diff(base, theirs)) theirDiff.set(k, [bv, sv]);

    const changes = new Map(); // key hex -> value ref | TOMBSTONE
    const conflicts = [];
    for (const k of new Set([...ourDiff.keys(), ...theirDiff.keys()])) {
      let bv;
      let merged;
      if (ourDiff.has(k) && theirDiff.has(k)) {
        [bv, merged] = ourDiff.get(k);
        if (merged !== theirDiff.get(k)[1]) {
          conflicts.push(k);
          continue;
        }
      } else if (ourDiff.has(k)) {
        [bv, merged] = ourDiff.get(k);
      } else {
        [bv, merged] = theirDiff.get(k);
      }
      if (merged !== bv) changes.set(k, merged !== null ? merged : TOMBSTONE);
    }

    if (conflicts.length && !resolver) throw new MergeConflict(conflicts.map(hexUtf8));
    if (conflicts.length) {
      const resolved = await RecordStore._resolve(blobs, conflicts.sort(), ourDiff, theirDiff, resolver);
      for (const [k, c] of resolved) changes.set(k, c);
    }

    try {
      let root = base;
      for (const k of [...changes.keys()].sort()) {
        const c = changes.get(k);
        if (c === TOMBSTONE) {
          try {
            root = await trie.delete(root, k);
          } catch (e) {
            if (!(e instanceof KeyMissing)) throw e;
          }
        } else {
          root = await trie.insert(root, k, c);
        }
      }
      return await trie.flush(root);
    } finally {
      trie.reset();
    }
  }

  static async _resolve(blobs, conflicts, ourDiff, theirDiff, resolver) {
    const trio = new Map(conflicts.map((k) => [k, [ourDiff.get(k)[0], ourDiff.get(k)[1], theirDiff.get(k)[1]]]));
    const refs = [...new Set([...trio.values()].flat().filter((r) => r !== null))];
    const values = await fetchBlobs(blobs, refs);
    const decode = (r) => (r === null ? ABSENT : decodeValue(values.get(r)));
    const kept = new Map();
    for (const k of conflicts) {
      const [bv, ov, tv] = trio.get(k);
      kept.set(k, await resolver(hexUtf8(k), decode(bv), decode(ov), decode(tv)));
    }
    const writes = conflicts.filter((k) => kept.get(k) !== DELETE);
    const newRefs = await putBlobs(blobs, writes.map((k) => encodeValue(kept.get(k))));
    const refOf = new Map(writes.map((k, i) => [k, newRefs[i]]));
    const changes = new Map();
    for (const k of conflicts) {
      const merged = refOf.get(k) ?? null; // null: the resolver deleted it
      if (merged !== trio.get(k)[0]) changes.set(k, merged !== null ? merged : TOMBSTONE);
    }
    return changes;
  }
}

/**
 * A "latest root" held in memory, with compare-and-set: what several
 * writers in one process can share, and the test double for a feed.
 */
export class MemoryPointer {
  constructor(root = null) {
    this.root = root;
  }

  async get() {
    return this.root;
  }

  async set(root) {
    this.root = root;
  }

  async compareAndSet(expected, root) {
    if (this.root !== expected) return false;
    this.root = root;
    return true;
  }
}
