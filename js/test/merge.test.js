// Cross-language: three-way merges reach Python's merged roots, and
// conflicts are the same keys.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ABSENT, DELETE, MergeConflict, RecordStore } from '../src/recordstore.js';
import { apply, fixture, storeFor } from './helpers.js';

const { cases } = fixture('merges.json');

const oursWins = (_key, _base, ours) => (ours === ABSENT ? DELETE : ours);

for (const c of cases) {
  test(`merge ${c.name}`, async () => {
    const blobs = storeFor(c.addressing);
    const baseStore = new RecordStore(blobs);
    await apply(baseStore, c.base);
    assert.equal(baseStore.root, c.roots.base);
    const branch = async (steps) => {
      const s = new RecordStore(blobs, { root: c.roots.base });
      await apply(s, steps);
      return s.commit();
    };
    const ours = await branch(c.ours);
    const theirs = await branch(c.theirs);
    assert.equal(ours, c.roots.ours);
    assert.equal(theirs, c.roots.theirs);

    if (c.merged !== undefined) {
      assert.equal(await RecordStore.merge(blobs, c.roots.base, ours, theirs), c.merged);
      // Commutative without a resolver.
      assert.equal(await RecordStore.merge(blobs, c.roots.base, theirs, ours), c.merged);
    } else {
      await assert.rejects(RecordStore.merge(blobs, c.roots.base, ours, theirs), (e) => {
        assert.ok(e instanceof MergeConflict);
        assert.deepEqual(e.conflicts.slice().sort(), c.conflicts);
        return true;
      });
      assert.equal(await RecordStore.merge(blobs, c.roots.base, ours, theirs, oursWins), c.merged_ours_wins);
    }
  });
}
