// Content addresses: SHA-256 (the default of MemoryBytesStore) and Swarm's
// own reference (BMT chunk hashes over Bee's chunk tree), ported from
// swarmfs's `bmt` and `splitter` modules.
//
// Swarm tree format (Bee's pkg/file/splitter):
// - a leaf is span (8 bytes, little-endian) + payload, payload <= 4096 bytes;
// - an intermediate is span + up to 128 child references (32 bytes each),
//   span being the content length beneath it;
// - a level with a single entry is promoted, never wrapped;
// - empty content is one chunk with span 0 and an empty payload.
//
// This is the reference of a *plain* upload. A blob over one chunk uploaded
// with erasure coding gets another reference; `redundancyFor` names the level
// to upload with so that the node's reference is this one.

import { keccak256Bytes } from '../vendor/js-sha3/index.js';
import { concatBytes, fromHex, toHex } from './bytes.js';

export const CHUNK_SIZE = 4096;
const SEGMENT_SIZE = 32;
const REF_SIZE = 32;
const BRANCHES = CHUNK_SIZE / REF_SIZE; // 128

/** keccak256 of bytes, as bytes. */
export { keccak256Bytes };

/** Root of the binary Merkle tree over a payload's 32-byte segments (zero-padded to 4096). */
export function bmtRoot(payload) {
  if (payload.length > CHUNK_SIZE) throw new RangeError(`payload of ${payload.length} bytes exceeds a chunk`);
  let level = [];
  for (let i = 0; i < CHUNK_SIZE; i += SEGMENT_SIZE) {
    const seg = new Uint8Array(SEGMENT_SIZE);
    seg.set(payload.subarray(i, Math.min(i + SEGMENT_SIZE, payload.length)));
    level.push(seg);
  }
  while (level.length > 1) {
    const next = [];
    for (let i = 0; i < level.length; i += 2) next.push(keccak256Bytes(concatBytes(level[i], level[i + 1])));
    level = next;
  }
  return level[0];
}

/** Little-endian 8-byte span. */
export function encodeSpan(span) {
  const out = new Uint8Array(8);
  let n = BigInt(span);
  for (let i = 0; i < 8; i++) {
    out[i] = Number(n & 0xffn);
    n >>= 8n;
  }
  return out;
}

/** BMT address of a content-addressed chunk (span + payload). */
export function chunkAddress(chunk) {
  return keccak256Bytes(concatBytes(chunk.subarray(0, 8), bmtRoot(chunk.subarray(8))));
}

/**
 * Split data into Swarm chunks as Bee does.
 * @param {Uint8Array} data
 * @returns {{root: string, chunks: {address: string, payload: Uint8Array, span: number}[]}}
 *   the root reference (hex) and every chunk of the tree, leaves first, each
 *   with the payload and span to upload it with.
 */
export function splitData(data) {
  const chunks = [];
  const store = (payload, span) => {
    const address = toHex(chunkAddress(concatBytes(encodeSpan(span), payload)));
    chunks.push({ address, payload, span });
    return address;
  };
  if (data.length === 0) return { root: store(new Uint8Array(0), 0), chunks };

  let level = []; // [reference hex, content length beneath it]
  for (let i = 0; i < data.length; i += CHUNK_SIZE) {
    const payload = data.subarray(i, i + CHUNK_SIZE);
    level.push([store(payload, payload.length), payload.length]);
  }
  while (level.length > 1) {
    const parents = [];
    for (let i = 0; i < level.length; i += BRANCHES) {
      const group = level.slice(i, i + BRANCHES);
      if (group.length === 1) {
        parents.push(group[0]); // promote, never wrap
        continue;
      }
      const span = group.reduce((sum, [, size]) => sum + size, 0);
      const refs = new Uint8Array(group.length * REF_SIZE);
      group.forEach(([ref], j) => refs.set(fromHex(ref), j * REF_SIZE));
      parents.push([store(refs, span), span]);
    }
    level = parents;
  }
  return { root: level[0][0], chunks };
}

/** Swarm's reference for `data` (plain upload), hex. */
export function swarmRef(data) {
  return splitData(data).root;
}

/** SHA-256 of `data`, hex (MemoryBytesStore's references, as in Python). */
export async function sha256Ref(data) {
  const digest = await globalThis.crypto.subtle.digest('SHA-256', data);
  return toHex(new Uint8Array(digest));
}

/**
 * The redundancy level to upload a blob of `size` bytes with so that the
 * node's reference is `swarmRef`'s: 1 while it fits one chunk (copies of the
 * chunk, same reference), 0 above (parity would change the reference).
 */
export function redundancyFor(size) {
  return size <= CHUNK_SIZE ? 1 : 0;
}

/**
 * An addressing scheme: 'sha256', 'swarm', or a function bytes -> reference
 * (or a promise of one).
 * @param {'sha256' | 'swarm' | ((data: Uint8Array) => string | Promise<string>)} addressing
 */
export function resolveAddressing(addressing) {
  if (typeof addressing === 'function') return addressing;
  if (addressing === 'sha256') return sha256Ref;
  if (addressing === 'swarm') return swarmRef;
  throw new TypeError(`unknown addressing ${JSON.stringify(addressing)}; use 'sha256', 'swarm' or a function`);
}
