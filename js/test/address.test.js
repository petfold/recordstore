import { test } from 'node:test';
import assert from 'node:assert/strict';
import { keccak256Bytes, redundancyFor, sha256Ref, splitData, swarmRef } from '../src/address.js';
import { toHex, utf8 } from '../src/bytes.js';
import { fixture, pattern } from './helpers.js';

test('keccak256 is Ethereum keccak (the vendored js-sha3 sanity vector)', () => {
  assert.equal(toHex(keccak256Bytes(utf8('hello'))), '1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8');
});

test('Swarm references match swarmfs for every tree shape in the fixtures', () => {
  for (const { size, ref } of fixture('swarm-addresses.json').cases) {
    assert.equal(swarmRef(pattern(size)), ref, `${size} bytes`);
  }
});

test('splitData lists every chunk, leaves first, intermediates with their spans', () => {
  const data = pattern(4096 * 3 + 10);
  const { root, chunks } = splitData(data);
  assert.equal(chunks.length, 5); // four leaves and one intermediate
  assert.equal(chunks.at(-1).address, root);
  assert.equal(chunks.at(-1).span, data.length);
  assert.equal(chunks.at(-1).payload.length, 4 * 32);
});

test('sha256 references are hex SHA-256, as Python MemoryBytesStore', async () => {
  assert.equal(await sha256Ref(utf8('abc')), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
});

test('redundancy level: 1 while a blob fits a chunk, 0 above', () => {
  assert.equal(redundancyFor(4096), 1);
  assert.equal(redundancyFor(4097), 0);
});
