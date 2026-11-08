// Cross-language: replay the operation sequences Python ran and reach the
// same root at every commit, then read stores Python wrote.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RecordStore } from '../src/recordstore.js';
import { fromHex } from '../src/bytes.js';
import { collect, fixture, storeFor } from './helpers.js';

const { cases } = fixture('stores.json');

for (const c of cases) {
  test(`${c.name}: every commit reaches Python's root`, async () => {
    const store = new RecordStore(storeFor(c.addressing));
    let commit = 0;
    for (const step of c.steps) {
      if (step.op === 'put') {
        store.put(step.key, step.value);
      } else if (step.op === 'delete') {
        const present = await store.has(step.key);
        assert.equal(await store.delete(step.key), present);
      } else {
        commit++;
        assert.equal(await store.commit(), step.root, `commit ${commit}`);
      }
    }
    assert.deepEqual(await collect(store.items()), c.items);
  });
}

test('reads a store Python wrote, from its blobs alone', async () => {
  for (const c of cases.filter((x) => x.blobs)) {
    const blobs = storeFor(c.addressing);
    for (const [ref, hex] of Object.entries(c.blobs)) {
      assert.equal(await blobs.put(fromHex(hex)), ref, 'the blob hashes to its reference');
    }
    const commits = c.steps.filter((s) => s.op === 'commit');
    // The fourth commit holds the unicode keys; read it as a snapshot.
    const snapshot = RecordStore.at(commits[3].root, blobs);
    const items = await collect(snapshot.items());
    assert.deepEqual(items.map(([k]) => k), ['a/b/c', 'users/alice', 'users/bob', 'userz', 'é', '￿', '😀']);
    assert.deepEqual(await snapshot.get('users/alice'), { name: 'Alice', role: 'admin' });
    assert.equal(await snapshot.get('a/b/c'), null);
    assert.equal(await snapshot.get('u'), undefined);
    const last = RecordStore.at(commits.at(-1).root, blobs);
    assert.deepEqual(await collect(last.items()), c.items);
  }
});
