import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  ABSENT, MemoryBytesStore, ProofError, RecordStore, verifyExtension, verifyPrefix, verifyProof,
} from '../src/index.js';
import { apply, fixture } from './helpers.js';

const { cases } = fixture('proofs.json');

async function build(c) {
  const blobs = new MemoryBytesStore({ addressing: c.addressing });
  const store = new RecordStore(blobs);
  await apply(store, c.first);
  assert.equal(store.root, c.roots.a);
  await apply(store, c.second);
  assert.equal(store.root, c.roots.b);
  return blobs;
}

for (const c of cases) {
  const opts = { addressing: c.addressing };

  test(`${c.addressing}: key proofs are identical to Python's and verify to the same records`, async () => {
    const blobs = await build(c);
    const empty = new RecordStore(new MemoryBytesStore(opts));
    assert.deepEqual(await empty.prove(c.empty.key, opts), c.empty.proof);
    assert.equal(await verifyProof(c.empty.proof, null), ABSENT);
    for (const k of c.keys) {
      const store = RecordStore.at(k.proof.root, blobs);
      assert.deepEqual(await store.prove(k.key, opts), k.proof, `prove(${JSON.stringify(k.key)})`);
      const verdict = await verifyProof(k.proof, k.proof.root);
      if (k.present) assert.deepEqual(verdict, k.value);
      else assert.equal(verdict, ABSENT);
    }
  });

  test(`${c.addressing}: prefix proofs are identical and complete`, async () => {
    const blobs = await build(c);
    for (const p of c.prefixes) {
      const store = RecordStore.at(p.proof.root, blobs);
      assert.deepEqual(await store.provePrefix(p.prefix, opts), p.proof, `provePrefix(${JSON.stringify(p.prefix)})`);
      assert.deepEqual([...(await verifyPrefix(p.proof, p.proof.root))], p.records);
    }
  });

  test(`${c.addressing}: extension proofs are identical, and the same extensions are refused`, async () => {
    const blobs = await build(c);
    for (const e of c.extensions) {
      const store = RecordStore.at(e.root, blobs);
      assert.equal(await store.extends(e.base, e.prefixes), e.extends);
      if (e.proof) {
        assert.deepEqual(await store.proveExtension(e.base, e.prefixes, opts), e.proof);
        assert.deepEqual(await verifyExtension(e.proof, e.base, e.root), e.proven);
      } else {
        const named = e.error.match(/: '(.+)' is (absent from|changed in) it$/);
        await assert.rejects(store.proveExtension(e.base, e.prefixes, opts), (err) => {
          assert.ok(err.message.includes(`${JSON.stringify(named[1])} is ${named[2]} it`), err.message);
          return true;
        });
      }
    }
  });

  test(`${c.addressing}: every proof Python refuses is refused`, async () => {
    for (const r of c.refused) {
      const check = r.kind === 'key' ? verifyProof(r.envelope, r.root)
        : r.kind === 'prefix' ? verifyPrefix(r.envelope, r.root)
          : verifyExtension(r.envelope, r.base, r.root);
      await assert.rejects(check, ProofError, r.name);
    }
  });
}

test('proofs are about committed roots: staged keys are refused', async () => {
  const store = new RecordStore(new MemoryBytesStore());
  store.put('a/1', 1);
  await store.commit();
  store.put('a/2', 2);
  await assert.rejects(store.prove('a/2'), /staged/);
  await assert.rejects(store.provePrefix('a/'), /staged/);
  await assert.rejects(store.proveExtension(null, ['a/']), /staged/);
  assert.deepEqual(await verifyProof(await store.prove('a/1'), store.root), 1);
});

test('a store with its own addressing function must name the scheme', async () => {
  const blobs = new MemoryBytesStore({ addressing: async (d) => (await crypto.subtle.digest('SHA-256', d), 'x'.repeat(64)) });
  const store = RecordStore.at(null, blobs);
  await assert.rejects(store.prove('k'), /addressing/);
  assert.equal((await store.prove('k', { addressing: 'sha256' })).present, false);
});
