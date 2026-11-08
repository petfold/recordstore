import { readFileSync } from 'node:fs';
import { parseJson } from '../src/canonical.js';
import { MemoryBytesStore } from '../src/stores.js';

/** A fixture written by tools/make_fixtures.py (big integers come back as BigInt). */
export function fixture(name) {
  return parseJson(readFileSync(new URL(`./fixtures/${name}`, import.meta.url), 'utf8'));
}

/** The deterministic test data the fixtures' Swarm addresses were computed over. */
export function pattern(size) {
  const out = new Uint8Array(size);
  for (let i = 0; i < size; i++) out[i] = (i * 31 + 7) & 0xff;
  return out;
}

export function storeFor(addressing) {
  return new MemoryBytesStore({ addressing });
}

/** Apply fixture steps (put / delete / commit) to a store. */
export async function apply(store, steps) {
  for (const step of steps) {
    if (step.op === 'put') store.put(step.key, step.value);
    else if (step.op === 'delete') await store.delete(step.key);
    else await store.commit();
  }
}

export async function collect(asyncIterable) {
  const out = [];
  for await (const x of asyncIterable) out.push(x);
  return out;
}
