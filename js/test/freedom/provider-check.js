// Checks recordstore's SwarmProviderBytesStore inside Freedom Browser.
//
// window.rsCheck.read()  reads OntoDAG's `space` pack (written by Python) through
//                        window.swarm: a few records and the keys under a prefix.
// window.rsCheck.write() writes a small store through window.swarm (one record
//                        over 4096 bytes, so a chunk tree with an intermediate),
//                        checks that its root equals the root computed offline
//                        under Swarm addressing, and reads it back.
// Both resolve to a plain object (also kept in window.rsCheck.results) and
// never throw: an error comes back as {error, reason}.

import { MemoryBytesStore, RecordStore, SwarmProviderBytesStore, canonicalJson } from '../../src/index.js';

const SPACE_PACK = '67f6be2688e5fb7ca85a463c3d2929dad2b177bb3edb6bb910e8fa110598cff3';
const $ = (id) => document.getElementById(id);

function show(name, result) {
  window.rsCheck.results[name] = result;
  $('out').textContent = JSON.stringify(window.rsCheck.results, null, 2);
  $('status').textContent = `${name}: ${result.error ? `failed (${result.reason || result.error})` : 'done'}`;
  return result;
}

function failure(e) {
  return { error: String(e && e.message || e), reason: e && e.data && e.data.reason, code: e && e.code };
}

async function collect(iterable) {
  const out = [];
  for await (const x of iterable) out.push(x);
  return out;
}

/** A provider wrapper that counts calls per method. */
function counting(provider) {
  const calls = {};
  return {
    calls,
    request(args) {
      calls[args.method] = (calls[args.method] || 0) + 1;
      return provider.request(args);
    },
  };
}

async function read(root = SPACE_PACK, prefix = 'ab', keys = ['abbey', 'abalone', 'not-a-key']) {
  $('status').textContent = 'reading…';
  const provider = counting(window.swarm);
  const started = performance.now();
  try {
    const store = RecordStore.at(root, new SwarmProviderBytesStore(provider));
    const values = {};
    for (const k of keys) values[k] = await store.get(k);
    const under = await collect(store.keys(prefix));
    return show('read', {
      root, values, prefix, keysUnderPrefix: under.length, firstKeys: under.slice(0, 5),
      ms: Math.round(performance.now() - started), calls: provider.calls,
    });
  } catch (e) {
    return show('read', { ...failure(e), ms: Math.round(performance.now() - started), calls: provider.calls });
  }
}

const RECORDS = () => ({
  'check/small': { hello: 'Freedom', at: new Date().toISOString().slice(0, 10) },
  'check/unicode/é😀': ['é', '中文', '😀'],
  'check/large': 'x'.repeat(5000), // over one chunk: a leaf pair and an intermediate
});

async function write() {
  $('status').textContent = 'writing… (approve Freedom’s prompts)';
  const provider = counting(window.swarm);
  const started = performance.now();
  const records = RECORDS();
  let expected;
  try {
    const offline = new RecordStore(new MemoryBytesStore({ addressing: 'swarm' }));
    for (const [k, v] of Object.entries(records)) offline.put(k, v);
    expected = await offline.commit();

    const store = new RecordStore(new SwarmProviderBytesStore(provider));
    for (const [k, v] of Object.entries(records)) store.put(k, v);
    const root = await store.commit();
    const wroteMs = Math.round(performance.now() - started);

    // Read back through a fresh store: nothing cached from the write.
    const back = RecordStore.at(root, new SwarmProviderBytesStore(window.swarm));
    const readBack = await collect(back.items());
    // Compare canonically: stored objects come back with their keys sorted.
    const same = canonicalJson(readBack) === canonicalJson(Object.entries(records).sort((a, b) => (a[0] < b[0] ? -1 : 1)));
    return show('write', {
      root, expected, rootMatches: root === expected, readBackMatches: same,
      wroteMs, totalMs: Math.round(performance.now() - started), calls: provider.calls,
    });
  } catch (e) {
    return show('write', { ...failure(e), expected, ms: Math.round(performance.now() - started), calls: provider.calls });
  }
}

window.rsCheck = {
  results: {},
  provider: Boolean(window.swarm && window.swarm.isFreedomBrowser),
  read,
  write,
  ready: true,
};
$('read').onclick = () => read();
$('write').onclick = () => write();
$('status').textContent = window.rsCheck.provider ? 'ready (Freedom provider found)' : 'no window.swarm: open this page in Freedom Browser';
