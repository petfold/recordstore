// Live: read a store that Python published on Swarm, through a node with
// Bee's /bytes API (Bee, or Ant inside Freedom Browser). Skipped unless
//
//   RECORDSTORE_BEE_URL=http://localhost:1633 npm test
//
// RECORDSTORE_LIVE_ROOT picks the root (default: OntoDAG's `space` pack,
// core v14, kept on Swarm by swarm-keep). Every blob is checked against its
// reference as it is read.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { BeeBytesStore, RecordStore } from '../src/index.js';

const url = process.env.RECORDSTORE_BEE_URL;
const root = process.env.RECORDSTORE_LIVE_ROOT
  || '67f6be2688e5fb7ca85a463c3d2929dad2b177bb3edb6bb910e8fa110598cff3';

test('reads a store Python published on Swarm', { skip: !url && 'set RECORDSTORE_BEE_URL to run' }, async () => {
  const store = RecordStore.at(root, new BeeBytesStore(url));
  const started = Date.now();
  const keys = [];
  for await (const key of store.keys()) keys.push(key);
  const sorted = [...keys].sort((a, b) => (Buffer.from(a) < Buffer.from(b) ? -1 : 1));
  assert.ok(keys.length > 0, 'the store has records');
  assert.deepEqual(keys, sorted, 'keys arrive in UTF-8 byte order');
  const sample = await store.get(keys[Math.floor(keys.length / 2)]);
  assert.notEqual(sample, undefined);
  console.log(`# ${keys.length} keys in ${Date.now() - started} ms; e.g. ${JSON.stringify(keys.slice(0, 3))}`);
});
