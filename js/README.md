# recordstore for JavaScript

A versioned key → record store over content-addressed storage such as
[Ethereum Swarm](https://www.ethswarm.org/): the JavaScript port of the
[Python recordstore](../README.md), and wire-compatible with it. A store
written by either reads in the other, and equal content gives the same root
in both: byte for byte, under SHA-256 or Swarm addressing.

**Status: early (0.1.0, not on npm yet).** The core is complete and tested
against the Python implementation: records, staging, commits, sorted
iteration, diff, three-way merge, and reconciling a moved pointer. Stores for
memory, a Bee node, and Freedom Browser's `window.swarm`; the store's latest
root in a Swarm feed (`SwarmFeedPointer`), with feed updates verifiable
offline. Not yet ported: the other proofs, history and undo, local-first
storage.

Plain JavaScript modules with JSDoc types and no npm dependencies, so a page
can import it with relative URLs only, including a page published on Swarm.
Two files are vendored, the same copies swarmlite uses: js-sha3 0.9.3 for
keccak and @noble/secp256k1 2.3.0 for signing feed updates (see their
`PROVENANCE.md` under [vendor/](vendor/)).
Node 22 or later, or any current browser (SHA-256 addressing uses Web Crypto,
which browsers offer only in secure contexts).

```js
import { RecordStore, MemoryBytesStore } from './recordstore/js/src/index.js';

const store = new RecordStore(new MemoryBytesStore());
store.put('users/alice', { name: 'Alice', role: 'admin' });
store.put('users/bob', { name: 'Bob' });
const root = await store.commit();        // one reference names this version

await store.get('users/alice');            // { name: 'Alice', role: 'admin' }
for await (const key of store.keys('users/')) console.log(key);

const snapshot = RecordStore.at(root, store.blobs);   // read-only, frozen
```

## Reading and writing on Swarm

```js
import { RecordStore, BeeBytesStore } from './recordstore/js/src/index.js';

// Read any published store: no postage batch needed.
const pack = RecordStore.at(
  '67f6be2688e5fb7ca85a463c3d2929dad2b177bb3edb6bb910e8fa110598cff3',
  new BeeBytesStore('http://localhost:1633'),
);
await pack.get('abbey');

// Write: name a postage batch of the node's.
const blobs = new BeeBytesStore('http://localhost:1633', { batch: '<batch id>' });
```

Every blob read is checked against its reference by recomputing its Swarm
address, and every write against the reference the node returns. Writes go up
with redundancy level 1 for a blob that fits one chunk and 0 above, as the
Python store does, so that the node's reference is the plain content
address. A store written with erasure coding by some other tool reads with
`verify: false`.

Measured on 8 November 2026: OntoDAG's `space` pack (5,134 records, written
by Python) listed in full from Swarm through the Ant node inside Freedom
Browser, cold, in 391 s, every blob checked (`test/live.test.js`). Listing
everything walks every trie node; a single `get` touches only the nodes on
its key's path.

## A store behind a feed

A root names one version; a feed gives a store a stable address that moves
with every commit, as Python's `swarm_store` and `SwarmFeedPointer` do. The
two implementations read each other's feeds.

```js
import { swarmStore } from './recordstore/js/src/index.js';

// Publish your own: the feed is owned by your key; writes need a batch.
const mine = await swarmStore('my-notes', { url: 'http://localhost:1633', signer: '<private key hex>', batch: '<batch id>' });
mine.put('note/1', 'hello');
await mine.commit();                     // uploads the blobs, then the signed feed update

// Follow someone else's: read-only, by their address.
const theirs = await swarmStore('loop-twonode-1791730596', { owner: '300f4daa2653102abde4c66f75ea512c0bb81571' });
for await (const [key, record] of theirs.items()) console.log(key);
```

`SwarmFeedPointer(urlOrTransport, topicName, {signer | owner, batch})` is
the pointer by itself; `RecordStore.open(blobs, {pointer})` puts any store
behind it. Light-node feed lookups are unreliable, so, as in Python, it
never trusts one: a read-your-writes cache, a monotonic index floor for
writes, the tip found by probing the update chunks themselves, retries
with backoff that never go back to an older index, and every update
checked against the owner's signature. `compareAndSet` (used by
`commit({reconcile: true})`) is best effort: a feed cannot claim an index
atomically. `pointer.update(i)` and `pointer.updates()` return a feed's
signed updates as envelopes that `verifyFeedUpdate(envelope, owner, topic)`
checks with no node; `verifyEquivocation` proves an owner signed two roots
at one index. Signatures are deterministic (RFC 6979, low S), so the same
key, update and timestamp give the same bytes as Python.

Live on 8 November 2026, through Swarm Desktop's Bee (`test/live.test.js`):
loopmarket's test maker book, a store Python keeps behind a feed, followed
to its root with its 8 records (12 s); the OntoDAG page's feed read with its
11 updates, the latest verified offline; and a store published behind a new
feed from JavaScript, then followed from JavaScript and from Python, which
read the same root and records and verified both updates.

### In Freedom Browser

A page in Freedom cannot reach a node's HTTP API; it has `window.swarm`.

```js
import { RecordStore, SwarmProviderBytesStore } from './recordstore/js/src/index.js';
const blobs = new SwarmProviderBytesStore(window.swarm);
```

Reads use `swarm_readData` and count against Freedom's per-site read budget.
Any feed reads through `ProviderFeedTransport`, which fetches each update
with `swarm_readSingleOwnerChunk` and checks it again here:

```js
const pointer = new SwarmFeedPointer(new ProviderFeedTransport(window.swarm), topic, { owner });
const store = await RecordStore.open(blobs, { pointer });
```

Writing a feed with your own key from a page waits for Freedom to accept
chunks a page signed (freedom-browser#647); until then `set` refuses.
Writes split each blob into chunks as Bee does and publish them with
`swarm_publishChunk`, so the references are Bee's and Python's. The first
write asks for access, and the store's first chunk goes out alone: Freedom
asks before each publish until the user ticks "Always allow this site", so
one approval then covers the rest of a commit (chunks sent in parallel from
the start each waited on a prompt: five for one commit).

Checked in Freedom built from main (0.8.9-dev) on 8 November 2026 with
`test/freedom/freedom_check.cjs`, which runs `test/freedom/provider-check.html`
in a throwaway profile and answers Freedom's prompts:

| | Freedom's node | Result |
|---|---|---|
| Read: 2 records and the 11 keys under a prefix of OntoDAG's `space` pack | its own Ant, just started | 65 reads, 13.5 s, one retry while the node found peers |
| The same read | Swarm Desktop's Bee | 0.4 s |
| Write a 3-record store (9 chunks, one over 4 KB), then read it back | Swarm Desktop's Bee | the root computed offline, read back equal; 3.1 s, one connect and one publish prompt |
| The same write | its own Ant, no postage batch | refused with `node-not-ready` (4900); the commit failed and left the root as it was |
| Follow loopmarket's test maker book through its feed | its own Ant | the same root as through Bee, 8 records; 8 update and 12 blob reads, 8.0 s |

Writing through Freedom's own Ant node is not tested yet: its node needs a
funded wallet and a postage batch. Expect writes there to be slower: one
call per chunk, several seconds each through Ant in swarmtyp's measurements.

## The API

Shaped like the Python one, but asynchronous and JavaScript-idiomatic:

| | |
|---|---|
| `new RecordStore(blobs, {root, pointer, readonly, nodeCacheSize})` | a store at `root` (null: empty) |
| `await RecordStore.open(blobs, {pointer})` | a store at its pointer's current root |
| `RecordStore.at(root, blobs)` | a read-only snapshot |
| `store.put(key, value)` | stage a write (validated and copied now) |
| `await store.delete(key)` | stage a removal; `false` if there is no such record |
| `await store.get(key)` | the record (a fresh copy), or `undefined` if absent; records can be `null` |
| `await store.has(key)` | Python's `contains` |
| `await store.getMany(keys)` | a `Map` of the records present, read together |
| `for await (const k of store.keys(prefix))` | sorted keys, staged changes included |
| `for await (const [k, v] of store.items(prefix))` | sorted records |
| `for await (const [k, mine, theirs] of store.diff(otherRoot))` | what differs; a missing side is `ABSENT` |
| `await store.commit({reconcile, resolver, retries})` | write, return the new root, move the pointer |
| `await RecordStore.merge(blobs, base, ours, theirs, resolver)` | three-way merge; `MergeConflict` without a resolver |
| `store.refsUnder(prefix)` | every blob the keys under `prefix` need (for keeping them alive) |
| `await swarmStore(topic, {url, signer \| owner, batch})` | a store on Swarm behind its feed |
| `new SwarmFeedPointer(urlOrTransport, topic, {signer \| owner, batch})` | the feed pointer: `get`, `set`, `compareAndSet`, `update(i)`, `updates()` |
| `verifyFeedUpdate(envelope, owner, topic)` | `{index, root, timestamp}` of a signed update, checked offline; `ProofError` otherwise |

Errors: `RecordUnavailable` when a record exists but a blob on its path cannot
be read right now (never reported as absence), `MergeConflict`,
`BlobNotFoundError`, `VerificationError`.

Bytes stores: `MemoryBytesStore({addressing: 'sha256' | 'swarm'})`,
`BeeBytesStore(url, {batch, verify, deferred, concurrency})`,
`SwarmProviderBytesStore(provider, {verify, concurrency})`. Anything with
async `put(bytes) → ref` and `get(ref) → bytes` works, optionally with
`putMany` and `getMany` (a `Map`). `MemoryPointer` is a pointer with
compare-and-set for writers in one process. Feed transports:
`BeeFeedTransport(url, {batch})` and `ProviderFeedTransport(provider)`, or
anything with `readSoc(address)` and `writeSoc(owner, identifier, signature,
data, batch)`, optionally `feedHead(owner, topic, after)`.

## Compatibility with Python

The canonical encoding is Python's `json.dumps(sort_keys=True,
separators=(",", ":"), ensure_ascii=False, allow_nan=False)`, reproduced byte
for byte: keys sorted by code point (not JavaScript's UTF-16 order), the same
string escapes, Python's float formatting. Where the languages differ:

- **Numbers.** JavaScript has one number type. A safe integer is written as an
  integer, so a Python float such as `1.0`, read and written back from
  JavaScript, becomes `1`: a different blob, as the Python guide warns. Other
  numbers are written as Python writes floats (`1e-05`, `1e+16`).
- **Large integers.** A `BigInt` is written as an integer of any size, and
  reading gives a `BigInt` back for an integer too large for a number, where
  the engine exposes the literal's source text (Node 21+, Chrome 114+).
  Elsewhere such an integer reads as the nearest number.
- **Lone surrogates** in keys or strings are refused, as Python refuses to
  encode them; `TextEncoder` would otherwise turn them into U+FFFD silently.
- **Not JSON:** `undefined`, functions, symbols, `Date`, `Map` and class
  instances are refused rather than converted.

## Tests

```sh
npm test                                            # Node's own runner, no install
RECORDSTORE_BEE_URL=http://localhost:1633 npm test  # also read live stores and feeds
RECORDSTORE_BEE_URL=... RECORDSTORE_BEE_BATCH=<batch> npm test   # and write one (spends stamps)

# in real Freedom Browser, from a freedom-browser checkout beside this repo
node test/freedom/freedom_check.cjs                 # read through Freedom's own node
node test/freedom/freedom_check.cjs --bee=http://127.0.0.1:1633 --write   # spends stamps
```

`test/fixtures/` is written by `tools/make_fixtures.py`, which runs the Python
implementation in `../src` (not an installed copy) and records canonical
encodings, Swarm addresses, the root after every commit of scripted and random
stores under both addressings, three-way merges, and feeds: topics, owners,
update addresses, signed updates written by swarmfs's own code (so JavaScript
must reproduce the signatures byte for byte), and updates Python refuses. It
needs swarmfs with signing (`pip install "recordstore[bee,feeds]"`). The JavaScript tests
replay the same operations and must reach the same bytes and roots. After a
change to the Python format, `npm run fixtures`; `npm run fixtures:check`
fails while the fixtures are stale.

## Not yet ported

- Proofs (`verify_proof`, `verify_prefix`, `verify_extension`); feed updates
  are ported.
- History, undo and redo (`FilePointer`'s timeline).
- Local-first storage.

## License

BSD-3-Clause, as the Python package. The vendored js-sha3 and
noble-secp256k1 are MIT.
