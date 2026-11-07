// Behaviour of the JavaScript API itself.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  ABSENT, BlobNotFoundError, MemoryBytesStore, MemoryPointer, RecordStore, RecordUnavailable,
} from '../src/index.js';
import { collect } from './helpers.js';

test('reads see staged writes; commit makes them the root', async () => {
  const store = new RecordStore(new MemoryBytesStore());
  store.put('users/alice', { name: 'Alice' });
  assert.deepEqual(await store.get('users/alice'), { name: 'Alice' });
  assert.equal(store.root, null);
  const root = await store.commit();
  assert.match(root, /^[0-9a-f]{64}$/);
  assert.equal(store.stagedCount, 0);
  assert.deepEqual(await RecordStore.at(root, store.blobs).get('users/alice'), { name: 'Alice' });
});

test('absent keys: get is undefined, has is false, delete is false; null is a value', async () => {
  const store = new RecordStore(new MemoryBytesStore());
  store.put('n', null);
  await store.commit();
  assert.equal(await store.get('n'), null);
  assert.equal(await store.has('n'), true);
  assert.equal(await store.get('missing'), undefined);
  assert.equal(await store.has('missing'), false);
  assert.equal(await store.delete('missing'), false);
  assert.equal(store.stagedCount, 0);
});

test('records are copies: changing what get returned or what was put changes nothing stored', async () => {
  const store = new RecordStore(new MemoryBytesStore());
  const value = { list: [1, 2] };
  store.put('k', value);
  value.list.push(3);
  const read = await store.get('k');
  read.list.push(4);
  assert.deepEqual(await store.get('k'), { list: [1, 2] });
});

test('equal content gives an equal root, whatever the history', async () => {
  const a = new RecordStore(new MemoryBytesStore());
  for (const k of ['c', 'a', 'b']) a.put(k, k);
  const b = new RecordStore(new MemoryBytesStore());
  b.put('a', 'a');
  b.put('x', 'temporary');
  await b.commit();
  b.put('b', 'b');
  b.put('c', 'c');
  await b.delete('x');
  assert.equal(await a.commit(), await b.commit());
});

test('keys and items are sorted by code point, filtered by prefix, staged changes included', async () => {
  const store = new RecordStore(new MemoryBytesStore());
  for (const k of ['b', '😀', '￿', 'a/2', 'a/1']) store.put(k, k);
  await store.commit();
  store.put('a/0', 'staged');
  await store.delete('a/2');
  assert.deepEqual(await collect(store.keys()), ['a/0', 'a/1', 'b', '￿', '😀']);
  assert.deepEqual(await collect(store.items('a/')), [['a/0', 'staged'], ['a/1', 'a/1']]);
});

test('getMany reads several records, leaving out absent keys', async () => {
  const store = new RecordStore(new MemoryBytesStore());
  for (let i = 0; i < 50; i++) store.put(`k${i}`, i);
  await store.commit();
  store.put('k1', 'staged');
  const got = await store.getMany(['k0', 'k1', 'k49', 'nope']);
  assert.equal(got.size, 3);
  assert.equal(got.get('k0'), 0);
  assert.equal(got.get('k1'), 'staged');
  assert.equal(got.get('k49'), 49);
});

test('diff names what changed between two roots', async () => {
  const store = new RecordStore(new MemoryBytesStore());
  for (let i = 0; i < 20; i++) store.put(`k${i}`, i);
  const before = await store.commit();
  store.put('k3', 'changed');
  await store.delete('k4');
  store.put('new', true);
  await store.commit();
  const changes = (await collect(RecordStore.at(before, store.blobs).diff(store.root)))
    .sort((x, y) => (x[0] < y[0] ? -1 : 1));
  assert.deepEqual(changes, [['k3', 3, 'changed'], ['k4', 4, ABSENT], ['new', ABSENT, true]]);
});

test('a snapshot is read-only', async () => {
  const snapshot = RecordStore.at(null, new MemoryBytesStore());
  assert.throws(() => snapshot.put('k', 1), TypeError);
  await assert.rejects(snapshot.delete('k'), TypeError);
});

test('keys must be non-empty well-formed strings', () => {
  const store = new RecordStore(new MemoryBytesStore());
  for (const bad of ['', '\ud800', 1, null]) assert.throws(() => store.put(bad, 1), TypeError);
});

test('a missing blob is reported as unavailable, never as absence', async () => {
  const blobs = new MemoryBytesStore();
  const store = new RecordStore(blobs);
  store.put('k', 'v');
  const root = await store.commit();
  const [[, valueRef]] = (await collect(store.refsUnder())).filter(([kind]) => kind === 'value');
  blobs.blobs.delete(valueRef);
  await assert.rejects(RecordStore.at(root, blobs).get('k'), (e) => e instanceof RecordUnavailable && e.cause instanceof BlobNotFoundError);
  blobs.blobs.clear();
  await assert.rejects(RecordStore.at(root, blobs).get('k'), RecordUnavailable);
});

test('two writers sharing a pointer converge with reconcile', async () => {
  const blobs = new MemoryBytesStore();
  const pointer = new MemoryPointer();
  const a = await RecordStore.open(blobs, { pointer });
  const b = await RecordStore.open(blobs, { pointer });
  a.put('from/a', 1);
  b.put('from/b', 2);
  await a.commit({ reconcile: true });
  const root = await b.commit({ reconcile: true }); // the pointer moved under b: merge
  assert.equal(await pointer.get(), root);
  const merged = RecordStore.at(root, blobs);
  assert.deepEqual(await collect(merged.items()), [['from/a', 1], ['from/b', 2]]);
});

test('commit with nothing staged returns the same root', async () => {
  const store = new RecordStore(new MemoryBytesStore());
  store.put('k', 1);
  const root = await store.commit();
  assert.equal(await store.commit(), root);
  store.put('k', 1); // the same value again
  assert.equal(await store.commit(), root);
});
