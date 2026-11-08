// Live checks against Swarm, through a node with Bee's API (Bee, or Ant
// inside Freedom Browser). Skipped unless RECORDSTORE_BEE_URL is set:
//
//   RECORDSTORE_BEE_URL=http://localhost:1633 npm test
//
// Reading needs nothing else. RECORDSTORE_BEE_BATCH (a postage batch of the
// node's) also runs the write check, which spends a few chunks of it.
// Every blob and feed update read is checked against its reference/owner.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { BeeBytesStore, RecordStore, SwarmFeedPointer, swarmStore, verifyFeedUpdate } from '../src/index.js';
import { toHex } from '../src/bytes.js';

const url = process.env.RECORDSTORE_BEE_URL;
const batch = process.env.RECORDSTORE_BEE_BATCH;
const skip = !url && 'set RECORDSTORE_BEE_URL to run';
const root = process.env.RECORDSTORE_LIVE_ROOT
  || '67f6be2688e5fb7ca85a463c3d2929dad2b177bb3edb6bb910e8fa110598cff3'; // OntoDAG's `space` pack

test('reads a store Python published on Swarm', { skip }, async () => {
  const store = RecordStore.at(root, new BeeBytesStore(url, { concurrency: 32 }));
  const started = Date.now();
  const keys = [];
  for await (const key of store.keys()) keys.push(key);
  const sorted = [...keys].sort((a, b) => (Buffer.from(a) < Buffer.from(b) ? -1 : 1));
  assert.ok(keys.length > 0, 'the store has records');
  assert.deepEqual(keys, sorted, 'keys arrive in UTF-8 byte order');
  assert.notEqual(await store.get(keys[Math.floor(keys.length / 2)]), undefined);
  console.log(`# ${keys.length} keys in ${Date.now() - started} ms; e.g. ${JSON.stringify(keys.slice(0, 3))}`);
});

test("follows a store Python keeps behind a feed (loopmarket's test maker book)", { skip }, async () => {
  const started = Date.now();
  const store = await swarmStore('loop-twonode-1791730596', { url, owner: '300f4daa2653102abde4c66f75ea512c0bb81571' });
  assert.match(store.root, /^[0-9a-f]{64}$/);
  const items = [];
  for await (const item of store.items()) items.push(item);
  assert.ok(items.length > 0);
  console.log(`# book root ${store.root.slice(0, 16)}..., ${items.length} records in ${Date.now() - started} ms; keys ${JSON.stringify(items.map(([k]) => k).slice(0, 6))}`);
});

test("reads the OntoDAG page's feed (written by Python's SwarmFeedPointer)", { skip }, async () => {
  const pointer = new SwarmFeedPointer(url, 'ontodag/freedom-page', { owner: 'e6d5bbdd5028b84d5eaf7be5fe07bed1ad73a110' });
  assert.equal(pointer.topic, '19e8777d991f320d8d8158a4016858ad2f9c66b89abf0f9393ef239fabbbf2e5');
  const ref = await pointer.get();
  assert.match(ref, /^[0-9a-f]{64}$/);
  const updates = await pointer.updates();
  const last = verifyFeedUpdate(updates.at(-1), pointer.owner, 'ontodag/freedom-page');
  assert.equal(last.root, ref);
  console.log(`# page feed: ${updates.length} updates, latest ${ref.slice(0, 16)}... at ${new Date(last.timestamp * 1000).toISOString()}`);
});

test('publishes a store behind a new feed and follows it', { skip: skip || (!batch && 'set RECORDSTORE_BEE_BATCH to run the write check') }, async () => {
  const signer = toHex(crypto.getRandomValues(new Uint8Array(32)));
  const topic = `recordstore-js-live-${Date.now()}`;
  const writer = await swarmStore(topic, { url, signer, batch });
  assert.equal(writer.root, null);
  writer.put('a', { n: 1 });
  writer.put('b', 'two');
  const first = await writer.commit();
  writer.put('c', [3]);
  await writer.delete('a');
  const second = await writer.commit();
  const follower = await swarmStore(topic, { url, owner: writer._pointer.owner });
  assert.equal(follower.root, second);
  const items = [];
  for await (const item of follower.items()) items.push(item);
  assert.deepEqual(items, [['b', 'two'], ['c', [3]]]);
  const updates = await follower._pointer.updates();
  assert.deepEqual(updates.map((u) => verifyFeedUpdate(u, writer._pointer.owner, topic).root), [first, second]);
  console.log(`# wrote feed ${topic} (owner ${writer._pointer.owner}): roots ${first.slice(0, 12)}..., ${second.slice(0, 12)}...`);
});
