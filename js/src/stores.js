// Bytes stores: where blobs live. The interface, as in Python:
//
//   put(data: Uint8Array) -> Promise<reference hex>
//   get(reference)        -> Promise<Uint8Array>
//
// and optionally `putMany(datas)` / `getMany(refs)` (a Map) for batched or
// parallel I/O, which recordstore uses when present.

import { redundancyFor, resolveAddressing, splitData, swarmRef } from './address.js';
import { fromBase64 } from './bytes.js';

/** A reference the store does not hold. */
export class BlobNotFoundError extends Error {
  constructor(ref) {
    super(`reference not found: ${ref}`);
    this.name = 'BlobNotFoundError';
    this.ref = ref;
  }
}

/** Bytes that do not hash to the reference they were fetched or stored under. */
export class VerificationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'VerificationError';
  }
}

/** Run `fn` over `items` with at most `limit` in flight; results in order. */
async function pool(items, limit, fn) {
  const out = new Array(items.length);
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i], i);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}

/**
 * In-memory content-addressed store, the test double for Swarm. Its
 * references are SHA-256 by default, as Python's MemoryBytesStore; with
 * `addressing: 'swarm'` they are the references Swarm would give.
 */
export class MemoryBytesStore {
  /** @param {{addressing?: 'sha256' | 'swarm' | ((data: Uint8Array) => string | Promise<string>)}} [options] */
  constructor({ addressing = 'sha256' } = {}) {
    this.addressing = addressing;
    this._address = resolveAddressing(addressing);
    /** @type {Map<string, Uint8Array>} */
    this.blobs = new Map();
  }

  async put(data) {
    const ref = await this._address(data);
    this.blobs.set(ref, Uint8Array.from(data));
    return ref;
  }

  async get(ref) {
    const data = this.blobs.get(ref);
    if (data === undefined) throw new BlobNotFoundError(ref);
    return Uint8Array.from(data);
  }

  async getMany(refs) {
    const out = new Map();
    for (const ref of refs) out.set(ref, await this.get(ref));
    return out;
  }

  async putMany(datas) {
    const refs = [];
    for (const data of datas) refs.push(await this.put(data));
    return refs;
  }

  get size() {
    return this.blobs.size;
  }
}

/**
 * Blobs on a Bee node (or any server with Bee's `/bytes` API, such as Ant),
 * the counterpart of Python's BeeBytesStore. Reads need no postage batch;
 * writes do. Every blob read is checked against its reference by recomputing
 * the Swarm address, and every write against the address the node returns.
 * That holds for blobs uploaded plainly, as recordstore uploads them (Python
 * and JS both); pass `verify: false` for a store written with erasure coding.
 */
export class BeeBytesStore {
  /**
   * @param {string} url  the node's API, e.g. http://localhost:1633
   * @param {{batch?: string, verify?: boolean, deferred?: boolean,
   *          concurrency?: number, fetch?: typeof fetch}} [options]
   */
  constructor(url, { batch = null, verify = true, deferred = true, concurrency = 8, fetch: fetchFn } = {}) {
    this.url = url.replace(/\/+$/, '');
    this.batch = batch;
    this.verify = verify;
    this.deferred = deferred;
    this.concurrency = concurrency;
    this._fetch = fetchFn || globalThis.fetch.bind(globalThis);
    this.addressing = 'swarm';
  }

  async get(ref) {
    const res = await this._fetch(`${this.url}/bytes/${ref}`);
    if (res.status === 404) throw new BlobNotFoundError(ref);
    if (!res.ok) throw new Error(`GET /bytes/${ref}: HTTP ${res.status}`);
    const data = new Uint8Array(await res.arrayBuffer());
    if (this.verify && swarmRef(data) !== ref) {
      throw new VerificationError(`the node returned bytes that do not hash to ${ref}`);
    }
    return data;
  }

  async put(data) {
    if (!this.batch) throw new Error('writing to Bee needs a postage batch (the `batch` option)');
    const res = await this._fetch(`${this.url}/bytes`, {
      method: 'POST',
      headers: {
        'content-type': 'application/octet-stream',
        'swarm-postage-batch-id': this.batch,
        'swarm-redundancy-level': String(redundancyFor(data.length)),
        'swarm-deferred-upload': String(this.deferred),
      },
      body: data,
    });
    if (!res.ok) throw new Error(`POST /bytes: HTTP ${res.status} ${await res.text().catch(() => '')}`);
    const { reference } = await res.json();
    if (this.verify && reference !== swarmRef(data)) {
      throw new VerificationError(`the node stored the blob under ${reference}, not its plain Swarm address`);
    }
    return reference;
  }

  async getMany(refs) {
    const datas = await pool(refs, this.concurrency, (ref) => this.get(ref));
    return new Map(refs.map((ref, i) => [ref, datas[i]]));
  }

  async putMany(datas) {
    return pool(datas, this.concurrency, (data) => this.put(data));
  }
}

/**
 * Blobs through a browser's `window.swarm` provider (Freedom Browser), where
 * a page cannot reach a node's HTTP API. Reads use `swarm_readData` (the node
 * joins the blob) and are checked against the reference here as well. Writes
 * split the blob into chunks as Bee does and publish each with
 * `swarm_publishChunk`, so references match Bee's and Python's; they need
 * the page to be connected (`swarm_requestAccess`), which the first write
 * asks for. Untested in Freedom so far.
 */
export class SwarmProviderBytesStore {
  /** @param {{request: (args: {method: string, params?: object}) => Promise<any>}} [provider] */
  constructor(provider = globalThis.swarm, { verify = true, concurrency = 4 } = {}) {
    if (!provider || typeof provider.request !== 'function') {
      throw new TypeError('no window.swarm provider (open the page in Freedom Browser)');
    }
    this.provider = provider;
    this.verify = verify;
    this.concurrency = concurrency;
    this.addressing = 'swarm';
    this._access = null;
  }

  async get(ref) {
    let result;
    try {
      result = await this.provider.request({ method: 'swarm_readData', params: { reference: ref } });
    } catch (e) {
      if (e && e.data && e.data.reason === 'chunk_not_found') throw new BlobNotFoundError(ref);
      throw e;
    }
    const data = fromBase64(result.data);
    if (this.verify && swarmRef(data) !== ref) {
      throw new VerificationError(`the provider returned bytes that do not hash to ${ref}`);
    }
    return data;
  }

  async put(data) {
    if (data.length === 0) throw new RangeError('the provider publishes chunks of 1 to 4096 bytes; an empty blob cannot be written');
    this._access ??= this.provider.request({ method: 'swarm_requestAccess' });
    await this._access;
    const { root, chunks } = splitData(data);
    await pool(chunks, this.concurrency, async ({ address, payload, span }) => {
      const { reference } = await this.provider.request({
        method: 'swarm_publishChunk',
        params: span === payload.length ? { data: payload } : { data: payload, span },
      });
      if (reference !== address) {
        throw new VerificationError(`the provider published a chunk as ${reference}, expected ${address}`);
      }
    });
    return root;
  }

  async getMany(refs) {
    const datas = await pool(refs, this.concurrency, (ref) => this.get(ref));
    return new Map(refs.map((ref, i) => [ref, datas[i]]));
  }

  async putMany(datas) {
    return pool(datas, this.concurrency, (data) => this.put(data));
  }
}
