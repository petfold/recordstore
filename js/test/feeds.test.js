import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  FeedTransportError, MemoryBytesStore, MemoryPointer, ProofError, ProviderFeedTransport, RecordStore,
  SwarmFeedPointer, VerificationError, addressOf, feedIdentifier, socAddress, topicOf,
  verifyEquivocation, verifyFeedUpdate,
} from '../src/index.js';
import { buildFeedUpdate, verifySoc } from '../src/feeds.js';
import { concatBytes, fromHex, toHex } from '../src/bytes.js';
import { collect, fixture } from './helpers.js';

const feeds = fixture('feeds.json');
const noWait = async () => {};

// -- cross-language -----------------------------------------------------------

test('owners and topics derive as in Python (a 64-hex name is hashed too)', () => {
  for (const { key, owner } of feeds.owners) assert.equal(toHex(addressOf(key)), owner);
  for (const { name, topic } of feeds.topics) assert.equal(toHex(topicOf(name)), topic);
});

test('identifiers and update addresses match Python', () => {
  for (const a of feeds.addresses) {
    const id = feedIdentifier(fromHex(a.topic), a.index);
    assert.equal(toHex(id), a.identifier);
    assert.equal(toHex(socAddress(id, fromHex(a.owner))), a.address);
  }
});

test('signed updates are byte-identical to the ones swarmfs writes', async () => {
  for (const u of feeds.updates) {
    const built = await buildFeedUpdate(u.key, fromHex(u.topic), u.index, u.root, u.timestamp);
    assert.equal(toHex(built.identifier), u.identifier);
    assert.equal(toHex(built.address), u.address);
    assert.equal(toHex(built.soc), u.soc, `update ${u.index}: same signature, same bytes`);
  }
});

test('updates Python published verify offline to what Python read from them', () => {
  for (const u of feeds.updates) assert.deepEqual(verifyFeedUpdate(u.envelope, u.owner, u.topic), u.verified);
  const first = feeds.updates[0];
  assert.deepEqual(verifyFeedUpdate(first.envelope, `0x${first.owner.toUpperCase()}`, 'recordstore/test'), first.verified);
});

test('an equivocation is proven, and a sequence is not one', () => {
  const { a, b, index } = feeds.equivocation;
  const [ua, ub] = [feeds.updates[a], feeds.updates[b]];
  assert.equal(verifyEquivocation(ua.envelope, ub.envelope, ua.owner, ua.topic), index);
  assert.throws(() => verifyEquivocation(feeds.updates[0].envelope, ua.envelope, ua.owner, ua.topic), ProofError);
  assert.throws(() => verifyEquivocation(ua.envelope, ua.envelope, ua.owner, ua.topic), ProofError);
});

test('every update Python refuses is refused', () => {
  for (const { name, envelope } of feeds.refused) {
    assert.throws(() => verifyFeedUpdate(envelope, envelope.owner, feeds.topics[0].topic), ProofError, name);
  }
});

// -- the pointer, against an in-memory feed -----------------------------------

/** A feed store that checks uploads as Bee does, and can misbehave like a light node. */
class MemoryFeedTransport {
  constructor({ chunkFailures = 0, lookups = false, staleLookups = 0, seed = 1 } = {}) {
    this.chunks = new Map();
    this.chunkFailures = chunkFailures;
    this.staleLookups = staleLookups;
    this.seed = seed;
    this.reads = 0;
    if (lookups) this.feedHead = this._feedHead;
  }

  random() {
    this.seed = (this.seed * 1103515245 + 12345) % 2 ** 31;
    return this.seed / 2 ** 31;
  }

  async readSoc(address) {
    this.reads++;
    if (this.random() < this.chunkFailures) throw new FeedTransportError('read chunk failed', 500);
    const soc = this.chunks.get(address);
    if (!soc) throw new FeedTransportError('not found', 404);
    return soc;
  }

  async writeSoc(owner, identifier, signature, data) {
    const id = fromHex(identifier);
    const ownerB = fromHex(owner);
    const address = socAddress(id, ownerB);
    const soc = concatBytes(id, fromHex(signature), data);
    verifySoc(soc, ownerB, address); // Bee refuses an update the owner did not sign
    this.chunks.set(toHex(address), soc); // and, like Bee, accepts a second one at a used index
  }

  async _feedHead(owner, topic) {
    if (this.random() < this.staleLookups) {
      if (this.random() < 0.5) throw new FeedTransportError('lookup failed', 404);
      return 0; // stale-early
    }
    let i = 0;
    while (this.chunks.has(toHex(socAddress(feedIdentifier(fromHex(topic), i), fromHex(owner))))) i++;
    return i === 0 ? null : i - 1;
  }
}

const KEY = '3b'.repeat(32);
const OTHER = '4c'.repeat(32);
const ROOT = (n) => n.toString(16).padStart(2, '0').repeat(32);

test('an empty feed reads as null; a published root reads back, also to a follower', async () => {
  const feed = new MemoryFeedTransport();
  const writer = new SwarmFeedPointer(feed, 'notes', { signer: KEY, sleep: noWait });
  assert.equal(await writer.get(), null);
  await writer.set(ROOT(1));
  assert.equal(await writer.get(), ROOT(1));
  const follower = new SwarmFeedPointer(feed, 'notes', { owner: writer.owner, sleep: noWait });
  assert.equal(await follower.get(), ROOT(1));
  assert.equal(follower.owner, toHex(addressOf(KEY)));
  await assert.rejects(follower.set(ROOT(2)), /signer/);
});

test('updates go to consecutive indexes; a cold writer finds the tip by probing', async () => {
  const feed = new MemoryFeedTransport();
  const writer = new SwarmFeedPointer(feed, 'notes', { signer: KEY, sleep: noWait });
  for (let i = 1; i <= 5; i++) await writer.set(ROOT(i));
  const cold = new SwarmFeedPointer(feed, 'notes', { signer: KEY, sleep: noWait });
  await cold.set(ROOT(6));
  assert.equal(feed.chunks.size, 6);
  const follower = new SwarmFeedPointer(feed, 'notes', { owner: writer.owner, sleep: noWait });
  assert.equal(await follower.get(), ROOT(6));
  const envelopes = await follower.updates();
  assert.deepEqual(envelopes.map((e) => verifyFeedUpdate(e, writer.owner, 'notes').root), [1, 2, 3, 4, 5, 6].map(ROOT));
});

test('flaky chunk reads and stale or failed lookups still resolve the latest root', async () => {
  const feed = new MemoryFeedTransport({ lookups: true });
  const writer = new SwarmFeedPointer(feed, 'notes', { signer: KEY, sleep: noWait });
  for (let i = 1; i <= 9; i++) await writer.set(ROOT(i));
  feed.chunkFailures = 0.3;
  feed.staleLookups = 0.5;
  let latest = ROOT(9);
  for (let trial = 0; trial < 20; trial++) {
    const follower = new SwarmFeedPointer(feed, 'notes', { owner: writer.owner, sleep: noWait, maxLookupRetries: 30 });
    assert.equal(await follower.get(), latest);
    const next = ROOT(10 + trial);
    await writer.set(next);
    assert.equal(await follower.get(), latest, 'within the cache lifetime the pointer does not look again');
    assert.equal(await follower._getFresh(), next);
    latest = next;
  }
});

test('a tampered update is an error, not a missing one', async () => {
  const feed = new MemoryFeedTransport();
  const writer = new SwarmFeedPointer(feed, 'notes', { signer: KEY, sleep: noWait });
  await writer.set(ROOT(1));
  const [address, soc] = [...feed.chunks][0];
  const forged = Uint8Array.from(soc);
  forged[forged.length - 1] ^= 1;
  feed.chunks.set(address, forged);
  const follower = new SwarmFeedPointer(feed, 'notes', { owner: writer.owner, sleep: noWait });
  await assert.rejects(follower.get(), VerificationError);
});

test('compareAndSet moves the feed only from the expected root', async () => {
  const feed = new MemoryFeedTransport();
  const p = new SwarmFeedPointer(feed, 'notes', { signer: KEY, sleep: noWait });
  await p.set(ROOT(1));
  assert.equal(await p.compareAndSet(ROOT(2), ROOT(3)), false);
  assert.equal(await p.compareAndSet(ROOT(1), ROOT(3)), true);
  assert.equal(await p.get(), ROOT(3));
});

test('a store behind a feed: a follower opens it, two writers reconcile', async () => {
  const blobs = new MemoryBytesStore({ addressing: 'swarm' });
  const feed = new MemoryFeedTransport();
  const pointer = () => new SwarmFeedPointer(feed, 'shared', { signer: KEY, sleep: noWait });
  const a = await RecordStore.open(blobs, { pointer: pointer() });
  const b = await RecordStore.open(blobs, { pointer: pointer() });
  a.put('from/a', 1);
  b.put('from/b', 2);
  await a.commit({ reconcile: true });
  const root = await b.commit({ reconcile: true }); // b's pointer finds a's update and merges
  const follower = await RecordStore.open(blobs, {
    pointer: new SwarmFeedPointer(feed, 'shared', { owner: toHex(addressOf(KEY)), sleep: noWait }),
  });
  assert.equal(follower.root, root);
  assert.deepEqual(await collect(follower.items()), [['from/a', 1], ['from/b', 2]]);
});

test('a feed is its owner’s alone: another key writes another feed', async () => {
  const feed = new MemoryFeedTransport();
  await new SwarmFeedPointer(feed, 'notes', { signer: OTHER, sleep: noWait }).set(ROOT(7));
  const mine = new SwarmFeedPointer(feed, 'notes', { owner: toHex(addressOf(KEY)), sleep: noWait });
  assert.equal(await mine.get(), null);
});

// -- through Freedom's provider ---------------------------------------------

/** window.swarm's swarm_readSingleOwnerChunk over the in-memory feed. */
function providerOver(feed) {
  return {
    async request({ method, params }) {
      assert.equal(method, 'swarm_readSingleOwnerChunk');
      const soc = feed.chunks.get(params.address);
      if (!soc) throw Object.assign(new Error('not found'), { data: { reason: 'chunk_not_found' } });
      const payload = soc.subarray(105);
      return {
        data: btoa(String.fromCharCode(...payload)), encoding: 'base64',
        span: payload.length, reference: params.address,
        identifier: toHex(soc.subarray(0, 32)), signature: `0x${toHex(soc.subarray(32, 97))}`,
      };
    },
  };
}

test('through window.swarm: any feed reads (and is checked again); writing waits for #647', async () => {
  const feed = new MemoryFeedTransport();
  const writer = new SwarmFeedPointer(feed, 'notes', { signer: KEY, sleep: noWait });
  for (let i = 1; i <= 3; i++) await writer.set(ROOT(i));
  const transport = new ProviderFeedTransport(providerOver(feed));
  const reader = new SwarmFeedPointer(transport, 'notes', { owner: writer.owner, sleep: noWait });
  assert.equal(await reader.get(), ROOT(3));
  const own = new SwarmFeedPointer(transport, 'notes', { signer: KEY, sleep: noWait });
  await assert.rejects(own.set(ROOT(4)), /#647/);
});

test('MemoryPointer and SwarmFeedPointer are interchangeable to a store', async () => {
  for (const pointer of [new MemoryPointer(), new SwarmFeedPointer(new MemoryFeedTransport(), 't', { signer: KEY, sleep: noWait })]) {
    const store = await RecordStore.open(new MemoryBytesStore(), { pointer });
    store.put('k', 'v');
    assert.equal(await pointer.get(), null);
    const root = await store.commit();
    assert.equal(await pointer.get(), root);
  }
});
