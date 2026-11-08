// SwarmProviderBytesStore against a fake window.swarm provider, offline.
// test/freedom/freedom_check.cjs runs the same store in real Freedom.

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { MemoryBytesStore, RecordStore, SwarmProviderBytesStore, VerificationError } from '../src/index.js';
import { chunkAddress, encodeSpan } from '../src/address.js';
import { concatBytes, toHex } from '../src/bytes.js';
import { collect } from './helpers.js';

/**
 * A provider that publishes chunks under their BMT address and serves
 * whole blobs by joining them, as Freedom's node does. It records the order
 * of requests and how many publishes are in flight at once.
 */
function fakeProvider({ corrupt = false } = {}) {
  const chunks = new Map(); // address -> {span, payload}
  const log = [];
  let inFlight = 0;
  let maxInFlightBeforeFirstDone = 0;
  let firstDone = false;
  const join = (address) => {
    const c = chunks.get(address);
    if (!c) throw Object.assign(new Error('not found'), { data: { reason: 'chunk_not_found' } });
    if (c.span <= 4096) return c.payload;
    const parts = [];
    for (let i = 0; i < c.payload.length; i += 32) parts.push(join(toHex(c.payload.subarray(i, i + 32))));
    return concatBytes(...parts);
  };
  return {
    log,
    get maxInFlightBeforeFirstDone() { return maxInFlightBeforeFirstDone; },
    async request({ method, params }) {
      log.push(method);
      if (method === 'swarm_requestAccess') return { connected: true };
      if (method === 'swarm_publishChunk') {
        inFlight++;
        if (!firstDone) maxInFlightBeforeFirstDone = Math.max(maxInFlightBeforeFirstDone, inFlight);
        await new Promise((r) => setTimeout(r, 1));
        const payload = Uint8Array.from(params.data);
        const span = params.span ?? payload.length;
        const address = toHex(chunkAddress(concatBytes(encodeSpan(span), payload)));
        chunks.set(address, { span, payload });
        inFlight--;
        firstDone = true;
        return { reference: corrupt ? '00'.repeat(32) : address };
      }
      if (method === 'swarm_readData') {
        const data = join(params.reference);
        return { data: btoa(String.fromCharCode(...data)), encoding: 'base64' };
      }
      throw new Error(`unexpected ${method}`);
    },
  };
}

test('a store written through the provider has the offline Swarm root, and reads back', async () => {
  const provider = fakeProvider();
  const records = { small: { a: 1 }, large: 'x'.repeat(9000), 'é😀': [1, 2] };
  const offline = new RecordStore(new MemoryBytesStore({ addressing: 'swarm' }));
  const store = new RecordStore(new SwarmProviderBytesStore(provider));
  for (const [k, v] of Object.entries(records)) {
    offline.put(k, v);
    store.put(k, v);
  }
  const root = await store.commit();
  assert.equal(root, await offline.commit());
  const back = RecordStore.at(root, new SwarmProviderBytesStore(provider));
  assert.deepEqual(await collect(back.items()), [['large', records.large], ['small', { a: 1 }], ['é😀', [1, 2]]]);
  assert.equal(provider.log[0], 'swarm_requestAccess');
});

test('the first chunk is published alone, so one "Always allow" covers the rest', async () => {
  const provider = fakeProvider();
  const store = new RecordStore(new SwarmProviderBytesStore(provider, { concurrency: 4 }));
  for (let i = 0; i < 20; i++) store.put(`k${i}`, 'v'.repeat(5000)); // many chunks, published in parallel
  await store.commit();
  assert.equal(provider.maxInFlightBeforeFirstDone, 1);
  assert.equal(provider.log.filter((m) => m === 'swarm_requestAccess').length, 1);
});

test('a reference the provider got wrong is caught', async () => {
  const blobs = new SwarmProviderBytesStore(fakeProvider({ corrupt: true }));
  await assert.rejects(blobs.put(new Uint8Array([1, 2, 3])), VerificationError);
});

test('a refused connection is asked again on the next write', async () => {
  let asked = 0;
  const provider = {
    async request({ method }) {
      if (method === 'swarm_requestAccess') {
        asked++;
        if (asked === 1) throw Object.assign(new Error('User rejected'), { code: 4001 });
        return { connected: true };
      }
      return fakeProviderPublish(method);
    },
  };
  const fallback = fakeProvider();
  const fakeProviderPublish = (method) => fallback.request({ method, params: { data: new Uint8Array([7]) } });
  const blobs = new SwarmProviderBytesStore(provider);
  await assert.rejects(blobs.put(new Uint8Array([7])), /rejected/);
  assert.match(await blobs.put(new Uint8Array([7])), /^[0-9a-f]{64}$/);
  assert.equal(asked, 2);
});
