// Swarm feeds as the store's pointer: SwarmFeedPointer, ported from the
// Python package (which builds on swarmfs's feeds), and feed updates
// verifiable offline.
//
// The format is bee-js's:
// - a topic name is hashed: keccak256 of its UTF-8;
// - the identifier of update `index` is keccak256(topic ‖ index as 8 bytes
//   big-endian);
// - the update is a single-owner chunk at keccak256(identifier ‖ owner):
//   identifier (32) ‖ signature (65) ‖ span (8) ‖ payload, the payload
//   being the writer's timestamp (8 bytes big-endian, unix seconds) ‖ root;
// - the signature is Ethereum's personal-message signature, r ‖ s ‖ v with
//   v in {27, 28}, over keccak256(identifier ‖ the wrapped chunk's address).
//
// Signing is RFC 6979 deterministic with low S (noble-secp256k1, as
// libsecp256k1 in Python), so the same key, update and timestamp give the
// same bytes in both languages.

import { getPublicKey, signAsync, Signature } from '../vendor/noble-secp256k1/index.js';
import { chunkAddress, encodeSpan, keccak256Bytes } from './address.js';
import { concatBytes, fromBase64, fromHex, toHex, utf8 } from './bytes.js';
import { VerificationError } from './stores.js';
import { ProofError } from './proofs.js';

export const FEED_UPDATE_FORMAT = 'recordstore-feed-update';

const SOC_IDENTIFIER_SIZE = 32;
const SOC_SIGNATURE_SIZE = 65;
const SOC_SPAN_OFFSET = SOC_IDENTIFIER_SIZE + SOC_SIGNATURE_SIZE; // 97
const SOC_PAYLOAD_OFFSET = SOC_SPAN_OFFSET + 8; // 105
const ETH_PREFIX = utf8('\x19Ethereum Signed Message:\n32');

/** A transport's answer for a chunk or lookup: `status` 404 means absent, 500 an incomplete retrieval. */
export class FeedTransportError extends Error {
  constructor(message, status) {
    super(message);
    this.name = 'FeedTransportError';
    this.status = status;
  }
}

// -- addressing -------------------------------------------------------------

/** The 32-byte topic of a topic name: keccak256 of its UTF-8, always (as SwarmFeedPointer). */
export function topicOf(name) {
  if (typeof name !== 'string') throw new TypeError('a topic name is a string');
  return keccak256Bytes(utf8(name));
}

/** keccak256(topic ‖ index as 8 bytes big-endian). */
export function feedIdentifier(topic, index) {
  const idx = new Uint8Array(8);
  new DataView(idx.buffer).setBigUint64(0, BigInt(index), false);
  return keccak256Bytes(concatBytes(topic, idx));
}

/** Where a single-owner chunk lives: keccak256(identifier ‖ owner). */
export function socAddress(identifier, owner) {
  return keccak256Bytes(concatBytes(identifier, owner));
}

function strip0x(hex) {
  return hex.startsWith('0x') || hex.startsWith('0X') ? hex.slice(2) : hex;
}

/** 20 owner bytes from 40 hex (optional 0x, either case). */
export function ownerBytes(owner) {
  const h = strip0x(String(owner)).toLowerCase();
  if (!/^[0-9a-f]{40}$/.test(h)) throw new TypeError(`${JSON.stringify(owner)} is not a 20-byte owner address`);
  return fromHex(h);
}

function privateKeyBytes(key) {
  const h = typeof key === 'string' ? strip0x(key).toLowerCase() : toHex(key);
  if (!/^[0-9a-f]{64}$/.test(h)) throw new TypeError('a private key is 32 bytes (64 hex characters)');
  return fromHex(h);
}

/** The address (20 bytes) of a secp256k1 private key: the last 20 bytes of keccak256 of the public key. */
export function addressOf(privateKey) {
  const pub = getPublicKey(privateKeyBytes(privateKey), false); // 0x04 ‖ x ‖ y
  return keccak256Bytes(pub.subarray(1)).subarray(12);
}

// -- signatures -------------------------------------------------------------

function messageDigest(digest32) {
  return keccak256Bytes(concatBytes(ETH_PREFIX, digest32));
}

/** 65-byte r ‖ s ‖ v over the personal-message digest of `digest32`. */
export async function signDigest(privateKey, digest32) {
  const sig = await signAsync(messageDigest(digest32), privateKeyBytes(privateKey), { lowS: true });
  return concatBytes(sig.toCompactRawBytes(), new Uint8Array([27 + sig.recovery]));
}

/** The 20-byte address whose key made `signature` over the personal-message digest of `digest32`. */
export function recoverDigest(signature, digest32) {
  if (signature.length !== 65) throw new VerificationError(`a signature is 65 bytes, got ${signature.length}`);
  const v = signature[64];
  const recid = v === 27 || v === 28 ? v - 27 : v;
  if (recid !== 0 && recid !== 1) throw new VerificationError(`unsupported recovery byte ${v}`);
  let pub;
  try {
    pub = Signature.fromCompact(signature.subarray(0, 64)).addRecoveryBit(recid)
      .recoverPublicKey(messageDigest(digest32)).toBytes(false);
  } catch (e) {
    throw new VerificationError(`signature recovers no key: ${e.message || e}`);
  }
  return keccak256Bytes(pub.subarray(1)).subarray(12);
}

/**
 * Check a single-owner chunk as a Bee node does on upload: its address
 * derives from its identifier and `owner`, and its signature over
 * (identifier, wrapped chunk address) recovers to `owner`.
 */
export function verifySoc(soc, owner, address) {
  if (soc.length < SOC_PAYLOAD_OFFSET) throw new VerificationError(`single-owner chunk too short: ${soc.length} bytes`);
  const identifier = soc.subarray(0, SOC_IDENTIFIER_SIZE);
  const signature = soc.subarray(SOC_IDENTIFIER_SIZE, SOC_SPAN_OFFSET);
  const cac = soc.subarray(SOC_SPAN_OFFSET);
  if (toHex(socAddress(identifier, owner)) !== toHex(address)) {
    throw new VerificationError('single-owner chunk does not match its address');
  }
  const recovered = recoverDigest(signature, keccak256Bytes(concatBytes(identifier, chunkAddress(cac))));
  if (toHex(recovered) !== toHex(owner)) throw new VerificationError('feed update signature was not made by the feed owner');
}

/**
 * The root and timestamp a feed update chunk publishes. Tolerates the other
 * payload formats swarmfs reads: a bare reference, or a wrapped root chunk.
 */
export function payloadFromSoc(soc) {
  if (soc.length < SOC_PAYLOAD_OFFSET) throw new VerificationError(`feed update chunk too short: ${soc.length} bytes`);
  const payload = soc.subarray(SOC_PAYLOAD_OFFSET);
  if (payload.length === 40 || payload.length === 72) {
    return { reference: toHex(payload.subarray(8)), timestamp: Number(new DataView(payload.buffer, payload.byteOffset, 8).getBigUint64(0, false)) };
  }
  if (payload.length === 32 || payload.length === 64) return { reference: toHex(payload), timestamp: null };
  return { reference: toHex(chunkAddress(soc.subarray(SOC_SPAN_OFFSET))), timestamp: null };
}

/**
 * The feed update publishing `root` as update `index` of `topic` (bytes),
 * signed with `privateKey`, as swarmfs (and bee-js) write it.
 * @returns {Promise<{identifier: Uint8Array, signature: Uint8Array, data: Uint8Array, soc: Uint8Array, address: Uint8Array}>}
 *   `data` (span ‖ payload) is what Bee's POST /soc takes; `soc` is the whole chunk.
 */
export async function buildFeedUpdate(privateKey, topic, index, root, timestamp) {
  const ts = new Uint8Array(8);
  new DataView(ts.buffer).setBigUint64(0, BigInt(timestamp), false);
  const payload = concatBytes(ts, fromHex(root));
  const data = concatBytes(encodeSpan(payload.length), payload);
  const identifier = feedIdentifier(topic, index);
  const signature = await signDigest(privateKey, keccak256Bytes(concatBytes(identifier, chunkAddress(data))));
  return {
    identifier, signature, data,
    soc: concatBytes(identifier, signature, data),
    address: socAddress(identifier, addressOf(privateKey)),
  };
}

// -- offline verification of published updates ----------------------------

function hexOwner(owner) {
  const h = strip0x(String(owner));
  if (h.length !== 40) throw new ProofError(`${JSON.stringify(owner)} is not a 20-byte owner address`);
  return h.toLowerCase();
}

/** A 32-byte topic in hex, or a topic name as SwarmFeedPointer hashes it. */
function hexTopic(topic) {
  const h = strip0x(String(topic));
  if (/^[0-9a-fA-F]{64}$/.test(h)) return h.toLowerCase();
  return toHex(topicOf(topic));
}

/**
 * Check that `update` (an envelope from `SwarmFeedPointer.update`) is
 * `owner`'s signed update of the feed `topic` at the index it claims, and
 * return what it publishes. Raises ProofError on any mismatch. Needs no node.
 * @returns {{index: number, root: string, timestamp: number}}
 */
export function verifyFeedUpdate(update, owner, topic) {
  if (!update || typeof update !== 'object' || update.format !== FEED_UPDATE_FORMAT) throw new ProofError(`not a ${FEED_UPDATE_FORMAT} envelope`);
  if (update.version !== 1) throw new ProofError(`unsupported feed update version ${JSON.stringify(update.version)}`);
  const ownerHex = hexOwner(owner);
  const topicHex = hexTopic(topic);
  if (update.owner !== ownerHex || update.topic !== topicHex) throw new ProofError("the update is another owner's or another feed's");
  const { index } = update;
  if (!Number.isSafeInteger(index) || index < 0) throw new ProofError('the update names no index');
  const ownerB = fromHex(ownerHex);
  const address = socAddress(feedIdentifier(fromHex(topicHex), index), ownerB);
  let wire;
  try {
    wire = fromHex(typeof update.soc === 'string' ? update.soc : '');
    verifySoc(wire, ownerB, address);
  } catch {
    throw new ProofError("the chunk is not the owner's signed update at this index");
  }
  const payload = wire.subarray(SOC_PAYLOAD_OFFSET);
  if (payload.length !== 40 && payload.length !== 72) throw new ProofError("the update's payload is not a timestamp and a root");
  return payloadToUpdate(index, payload);
}

function payloadToUpdate(index, payload) {
  const timestamp = Number(new DataView(payload.buffer, payload.byteOffset, 8).getBigUint64(0, false));
  return { index, root: toHex(payload.subarray(8)), timestamp };
}

/**
 * Two updates by `owner` of one feed at one index publishing different
 * roots: the owner equivocated, and this returns the index. ProofError when
 * either does not verify, the indices differ, or the roots agree.
 */
export function verifyEquivocation(a, b, owner, topic) {
  const ua = verifyFeedUpdate(a, owner, topic);
  const ub = verifyFeedUpdate(b, owner, topic);
  if (ua.index !== ub.index) throw new ProofError('updates at different indices are a sequence, not an equivocation');
  if (ua.root === ub.root) throw new ProofError('the two updates publish the same root');
  return ua.index;
}

// -- transports -------------------------------------------------------------

/**
 * Feeds over Bee's HTTP API (or Ant's): chunks from GET /chunks, uploads
 * with POST /soc, and Bee's sequence lookup (GET /feeds) as a shortcut.
 */
export class BeeFeedTransport {
  constructor(url, { batch = null, fetch: fetchFn } = {}) {
    this.url = url.replace(/\/+$/, '');
    this.batch = batch;
    this._fetch = fetchFn || globalThis.fetch.bind(globalThis);
  }

  async readSoc(address) {
    const res = await this._fetch(`${this.url}/chunks/${address}`);
    if (!res.ok) throw new FeedTransportError(`GET /chunks/${address}: HTTP ${res.status}`, res.status);
    return new Uint8Array(await res.arrayBuffer());
  }

  async writeSoc(owner, identifier, signature, data, batch = this.batch) {
    if (!batch) throw new Error('writing a feed update to Bee needs a postage batch (the `batch` option)');
    const res = await this._fetch(`${this.url}/soc/${owner}/${identifier}?sig=${signature}`, {
      method: 'POST',
      headers: { 'swarm-postage-batch-id': batch, 'content-type': 'application/octet-stream' },
      body: data,
    });
    if (!res.ok) throw new FeedTransportError(`POST /soc: HTTP ${res.status} ${await res.text().catch(() => '')}`, res.status);
  }

  /** The latest index Bee's lookup finds, or null for an empty feed. */
  async feedHead(owner, topic, after = null) {
    let url = `${this.url}/feeds/${owner}/${topic}?type=sequence`;
    if (after !== null) url += `&after=${after}`;
    const res = await this._fetch(url, { headers: { 'swarm-only-root-chunk': 'true' } });
    if (res.status === 404) return null;
    if (!res.ok) throw new FeedTransportError(`GET /feeds: HTTP ${res.status}`, res.status);
    await res.arrayBuffer().catch(() => {}); // only the headers matter
    const index = res.headers.get('swarm-feed-index');
    return index ? Number(BigInt(`0x${index}`)) : null;
  }
}

/**
 * Feeds through Freedom Browser's window.swarm. Reading works for any
 * owner's feed: `swarm_readSingleOwnerChunk` returns the chunk with its
 * signature, and the pointer checks it again here. Writing a feed with your
 * own key waits for Freedom to accept chunks a page signed
 * (freedom-browser#647).
 */
export class ProviderFeedTransport {
  constructor(provider = globalThis.swarm) {
    if (!provider || typeof provider.request !== 'function') throw new TypeError('no window.swarm provider (open the page in Freedom Browser)');
    this.provider = provider;
  }

  async readSoc(address) {
    let r;
    try {
      r = await this.provider.request({ method: 'swarm_readSingleOwnerChunk', params: { address } });
    } catch (e) {
      const reason = e && e.data && e.data.reason;
      if (reason === 'chunk_not_found') throw new FeedTransportError(`no chunk at ${address}`, 404);
      if (reason === 'rate_limited') throw new FeedTransportError('read budget exhausted', 429);
      throw e;
    }
    const payload = fromBase64(r.data);
    return concatBytes(fromHex(strip0x(r.identifier)), fromHex(strip0x(r.signature)), encodeSpan(r.span ?? payload.length), payload);
  }

  async writeSoc() {
    throw new Error('writing a feed with your own key from a page waits for Freedom to accept chunks the page signed (freedom-browser#647)');
  }
}

// -- the pointer ------------------------------------------------------------

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const TRANSIENT = new Set([404, 429, 500]);

/**
 * The store's mutable "latest root" in a Swarm feed, as Python's
 * SwarmFeedPointer: `set(root)` publishes a signed update, `get()` resolves
 * the latest root. Light-node feed lookups are unreliable (404s, stale-early
 * indexes), so it never trusts one: a read-your-writes cache, a monotonic
 * index floor for writes, the tip found by probing the update chunks
 * themselves, and retries with backoff that never go back to an older index.
 * Every chunk read is verified against the owner.
 *
 * `transport` is a Bee API URL (or a BeeFeedTransport, ProviderFeedTransport,
 * or anything with readSoc/writeSoc and optionally feedHead). Pass `signer`
 * (private key hex) to read and write, or `owner` (address hex) to read.
 * Writing to Bee also needs `batch`. `topic` is a name, hashed to the topic.
 */
export class SwarmFeedPointer {
  constructor(transport, topic, {
    signer = null, owner = null, batch = null, feedTtl = 15, maxLookupRetries = 15,
    retryBackoff = 0.5, retryBackoffCap = 5, sleep: sleepFn = sleep, now = () => Date.now(),
  } = {}) {
    this._transport = typeof transport === 'string' ? new BeeFeedTransport(transport, { batch }) : transport;
    this._topic = topicOf(topic);
    if (signer) {
      this._key = privateKeyBytes(signer);
      this._owner = addressOf(this._key);
    } else if (owner) {
      this._key = null;
      this._owner = ownerBytes(owner);
    } else {
      throw new TypeError('SwarmFeedPointer needs a signer (to read and write) or an owner address (read-only)');
    }
    if (batch !== null) {
      const b = strip0x(String(batch)).toLowerCase();
      if (!/^[0-9a-f]{64}$/.test(b)) throw new TypeError(`${JSON.stringify(batch)} is not a postage batch id`);
      batch = b;
    }
    this._batch = batch;
    this._ttl = feedTtl * 1000;
    this._maxRetries = Math.max(1, maxLookupRetries);
    this._backoff = retryBackoff * 1000;
    this._backoffCap = retryBackoffCap * 1000;
    this._sleep = sleepFn;
    this._now = now;
    // read-your-writes cache + monotonic index floor
    this._cachedRef = null;
    this._nextIndex = 0;
    this._cacheExpiry = 0;
  }

  /** The feed owner's address, hex (no 0x). */
  get owner() {
    return toHex(this._owner);
  }

  /** The 32-byte topic, hex. */
  get topic() {
    return toHex(this._topic);
  }

  async set(root) {
    if (!this._key) throw new Error('SwarmFeedPointer.set needs a signer');
    let index;
    if (this._nextIndex > 0) {
      index = this._nextIndex; // a persistent writer's floor is authoritative
    } else {
      const probed = await this._probeLatestIndex();
      index = probed === null ? 0 : probed + 1;
    }
    const update = await buildFeedUpdate(this._key, this._topic, index, root, Math.floor(this._now() / 1000));
    await this._transport.writeSoc(this.owner, toHex(update.identifier), toHex(update.signature), update.data, this._batch);
    this._cachedRef = root;
    this._nextIndex = index + 1;
    this._cacheExpiry = this._now() + this._ttl;
  }

  async get() {
    if (this._cachedRef !== null && this._now() < this._cacheExpiry) return this._cachedRef;
    let delay = this._backoff;
    for (let attempt = 0; attempt < this._maxRetries; attempt++) {
      try {
        const latest = await this._resolveLatestIndex();
        if (latest === null) return this._cachedRef; // empty feed (definitive)
        const indexNext = latest + 1;
        if (indexNext > this._nextIndex || this._cachedRef === null) {
          const { reference } = await this._atIndex(latest);
          this._cachedRef = reference;
          this._nextIndex = indexNext;
          this._cacheExpiry = this._now() + this._ttl;
          return this._cachedRef;
        }
        if (indexNext === this._nextIndex) {
          this._cacheExpiry = this._now() + this._ttl; // confirmed unchanged
          return this._cachedRef;
        }
        // indexNext below the floor: a stale-early lookup; retry for a fresher one
      } catch (e) {
        if (!(e instanceof FeedTransportError && TRANSIENT.has(e.status))) throw e;
      }
      if (attempt < this._maxRetries - 1) {
        await this._sleep(delay);
        delay = Math.min(delay * 2, this._backoffCap);
      }
    }
    return this._cachedRef; // last known, or null if never resolved
  }

  async _getFresh() {
    this._cacheExpiry = 0;
    return this.get();
  }

  /**
   * Best-effort compare-and-set for reconciling commits: true only if the
   * feed still resolved to `expected` and `root` was written and read back.
   * A feed cannot claim an index atomically (Bee accepts a second update at
   * a used index), so two writers at the same index at the same moment can
   * both believe they won; this narrows the window, it does not close it.
   */
  async compareAndSet(expected, root) {
    if ((await this._getFresh()) !== expected) return false;
    await this.set(root);
    return (await this._getFresh()) === root;
  }

  async _resolveLatestIndex() {
    const hint = this._nextIndex - 2;
    if (hint >= 1 && typeof this._transport.feedHead === 'function') {
      try {
        const head = await this._transport.feedHead(this.owner, this.topic, hint);
        if (head !== null && head !== undefined) return head;
      } catch (e) {
        if (!(e instanceof FeedTransportError && TRANSIENT.has(e.status))) throw e;
      }
    }
    return this._probeLatestIndex();
  }

  /** Highest existing index (null: empty), by exponential + binary search over the update chunks. */
  async _probeLatestIndex() {
    if (!(await this._socExists(0))) return null;
    let lo = 0;
    let hi = 1;
    while (await this._socExists(hi)) {
      lo = hi;
      hi *= 2;
    }
    while (hi - lo > 1) {
      const mid = Math.floor((lo + hi) / 2);
      if (await this._socExists(mid)) lo = mid;
      else hi = mid;
    }
    return lo;
  }

  /** Does the update at `index` exist? 404 is the one definitive no; 500 (and 429) are retried. */
  async _socExists(index) {
    let delay = this._backoff;
    for (let attempt = 0; attempt < this._maxRetries; attempt++) {
      try {
        await this._atIndex(index);
        return true;
      } catch (e) {
        if (!(e instanceof FeedTransportError)) throw e;
        if (e.status === 404) return false;
        if ((e.status !== 500 && e.status !== 429) || attempt === this._maxRetries - 1) throw e;
      }
      await this._sleep(delay);
      delay = Math.min(delay * 2, this._backoffCap);
    }
    return false;
  }

  _address(index) {
    return socAddress(feedIdentifier(this._topic, index), this._owner);
  }

  /** The verified update at `index`: {reference, timestamp}. */
  async _atIndex(index) {
    const address = this._address(index);
    const soc = await this._transport.readSoc(toHex(address));
    verifySoc(soc, this._owner, address);
    return payloadFromSoc(soc);
  }

  /** The update at `index` as a self-contained envelope that `verifyFeedUpdate` checks with no node. */
  async update(index) {
    const soc = await this._transport.readSoc(toHex(this._address(index)));
    return { format: FEED_UPDATE_FORMAT, version: 1, owner: this.owner, topic: this.topic, index, soc: toHex(soc) };
  }

  /** The updates from `start` up to `stop` (default: past the tip, found by probing), in index order. */
  async updates(start = 0, stop = null) {
    if (stop === null) {
      const tip = await this._probeLatestIndex();
      stop = tip === null ? 0 : tip + 1;
    }
    const out = [];
    for (let i = start; i < stop; i++) out.push(await this.update(i));
    return out;
  }
}
