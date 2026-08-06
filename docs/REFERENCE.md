# recordstore Reference

Compact, definition-first, no narrative. The tutorial lives in the
[User Guide](USER_GUIDE.md); larger design bets in the
[roadmap](../ROADMAP.md); release history in the
[CHANGELOG](../CHANGELOG.md). Tables here are pinned against the code by
`tests/test_reference.py` — if a name or parameter in this file and the
code disagree, the suite fails.

Package version this file describes: `0.20.0`.

## 1. Vocabulary

| term | definition |
|---|---|
| record | Any JSON-compatible value, stored under a string key. Returned values are deep copies. |
| ref | Hex string naming one immutable blob in a bytes store (its content address). |
| root | The ref of the trie root: one root names the entire dataset-at-a-version. |
| canonicity | Equal content ⇒ equal root, regardless of the edit history that produced it. |
| staging | `put`/`delete` buffer in memory; nothing is written until `commit()`. |
| commit | Writes staged changes, returns the new root. All-or-nothing for readers. |
| snapshot | A read-only store pinned to one root (`RecordStore.at`). |
| timeline | The states a pointer has been in, in order, plus where it is now — what `history()`, `undo()` and `redo()` read. An editor's undo line, not the journal's full audit. |
| pointer | A mutable name for the latest root (file, memory, or a signed Swarm feed). |
| addressing | The ref scheme of a bytes store: `sha256` or `swarm` (BMT). Uniform per store. |
| proof | Self-contained evidence that a key has (or provably has not) a value under a root. |
| local-first | Commits land on local disk instantly; a background worker pushes to Swarm and confirms. |
| durability rung | `committed` (local disk) → `pushed` (on the Bee node) → `confirmed` (provably on the network). |
| pinned / evictable | Unconfirmed blobs are pinned locally (never evicted); confirmed blobs may evict and heal by verified re-fetch. |
| journal / reflog | The local-first store's append-only event log: lineage, rungs, pins. |

## 2. Install

| command | gives |
|---|---|
| `pip install recordstore` | core: memory/dir/fsspec-less local stores, merge, diff, proofs |
| `pip install "recordstore[bee]"` | `BeeBytesStore` (requests) |
| `pip install "recordstore[feeds]"` | `SwarmFeedPointer` (swarm-bee) |
| `pip install "recordstore[stamps]"` | `postage_batch_id="auto"`, `batch_status` (swarmfs ≥ 0.4) |
| `pip install "recordstore[fsspec]"` | `FsspecBytesStore` |
| `pip install "recordstore[swarm-addressing]"` | `DirBytesStore(addressing="swarm")` (swarmfs ≥ 0.9 — keccak is in its base install) |
| `pip install "recordstore[swarm-only]"` | the whole store directly on Swarm: `swarm_store` (`BeeBytesStore` + `SwarmFeedPointer` + `"auto"` stamps) |
| `pip install "recordstore[local-first-swarm]"` | `local_first_store`: disk now, Swarm in the background (swarmfs ≥ 0.9) |

## 3. Exports

Everything importable from `recordstore` (exactly `__all__`):

| name | one line |
|---|---|
| `RecordStore` | the store: staging, commit, snapshots, merge, diff, proofs |
| `MemoryBytesStore` | in-memory bytes store (sha256 refs) — tests, scratch |
| `DirBytesStore` | one blob per file under a local directory |
| `FsspecBytesStore` | blobs on any fsspec filesystem (S3, GCS, HTTP, …) |
| `BeeBytesStore` | blobs on a Swarm Bee node via `POST/GET /bytes` |
| `CachedBytesStore` | byte-budgeted in-memory LRU in front of any bytes store |
| `MemoryPointer` | in-process pointer with atomic `compare_and_set` |
| `FilePointer` | pointer in a local file (atomic replace), keeping a timeline |
| `Version` | one row of `RecordStore.history()`: `root`, `at`, `message`, `current` |
| `SwarmFeedPointer` | pointer in an owner-signed Swarm feed |
| `swarm_store` | one call: whole store on Swarm (Bee blobs + feed pointer) |
| `LocalFirstRecordStore` | RecordStore over a local-first store directory |
| `local_first_store` | one call: disk now, Swarm in the background |
| `MergeConflict` | both sides changed a key to different values, no resolver |
| `RecordUnavailable` | key exists; its bytes are unreachable right now (NOT a `KeyError`) |
| `ABSENT` | resolver sentinel: this side lacks the key / diff side lacks the key |
| `DELETE` | resolver sentinel: drop the key from the merge |
| `canonical_bytes` | the deterministic JSON encoding behind canonicity |
| `verify_proof` | check a proof against a root — no store, no network, no trust |
| `ProofError` | the proof does not verify against the given root |
| `PROOF_FORMAT` | proof envelope format name (`"recordstore-trie-proof"`) |

## 4. `RecordStore`

| member | signature | semantics |
|---|---|---|
| `RecordStore` | `(bytes_store, root=None, pointer=None, node_cache_size=65536)` | open at `root`, or at `pointer.get()` when a pointer is given and `root` is None. `node_cache_size` bounds the decoded trie-node LRU. |
| `RecordStore.at` | `(root, bytes_store)` | classmethod: read-only snapshot of any root. Writes raise `TypeError`. |
| `RecordStore.get` | `(key)` | the record (deep copy). `KeyError` if absent; `RecordUnavailable` if it exists but its bytes are unreachable. |
| `RecordStore.put` | `(key, value)` | stage a write. `ValueError`/`TypeError` for non-JSON values, NaN/Infinity, empty key. |
| `RecordStore.delete` | `(key)` | stage a delete. `KeyError` if the key exists nowhere. |
| `RecordStore.contains` | `(key)` | `bool`; propagates `RecordUnavailable` rather than answering falsely. |
| `RecordStore.keys` | `(prefix="")` | sorted key iterator, staged changes visible. |
| `RecordStore.items` | `(prefix="")` | sorted `(key, record)` iterator. |
| `RecordStore.commit` | `(*, message=None, reconcile=False, resolver=None, retries=5)` | flush staged → new root; updates the pointer. `reconcile=True` three-way-merges with a moved pointer and retries. On a local-first backend, also journals the commit. `message` labels the state in the timeline and is **never part of the content** — equal content commits to equal roots whatever the words. |
| `RecordStore.diff` | `(other_root)` | `(key, mine, theirs)` per differing key; `ABSENT` marks a missing side. O(divergence). |
| `RecordStore.merge` | `(bytes_store, base, ours, theirs, resolver=None)` | classmethod: three-way merge → merged root. Unresolved conflicts raise `MergeConflict` (`.conflicts`). |
| `RecordStore.prove` | `(key, addressing=None)` | inclusion-or-absence proof dict for a committed key. `ValueError` on staged keys or unknown addressing. |
| `RecordStore.root` | property | root of the last committed state. |
| `RecordStore.history` | `(limit=None)` | the states this store has been in, newest first, as `Version`s. `[]` when the pointer keeps no timeline. |
| `RecordStore.undo` | `()` | step back one state; `None` at the start of the line. Moves the pointer, drops staged changes, destroys nothing. |
| `RecordStore.redo` | `()` | step forward again; `None` at the tip. A commit after an undo abandons the tail. |
| `RecordStore.checkout` | `(root)` | jump to a state the timeline holds; `KeyError` for any other root. |
| `RecordStore.status` | `()` | `{root, staged, readonly, history, position, undoable, redoable}`. |
| `RecordStore.blobs` | property | the underlying bytes store (for `RecordStore.at(other, s.blobs)`). |

Resolver contract (for `commit(reconcile=True)` and `merge`):
`resolver(key, base, ours, theirs) -> value | DELETE`; sides that lack the
key are passed `ABSENT`.

## 5. Bytes stores

The `BytesStore` protocol is `put(bytes) → ref` and `get(ref) → bytes`;
`put_many`/`get_many` are optional batched forms every shipped store
provides. Blobs are immutable and content-addressed.

| store | signature | notes |
|---|---|---|
| `MemoryBytesStore` | `()` | sha256 refs; forgets on exit. |
| `DirBytesStore` | `(path, addressing="sha256")` | `addressing="swarm"` shares Swarm's address space (needs `[swarm-addressing]`). |
| `FsspecBytesStore` | `(url, addressing="sha256", **storage_options)` | any fsspec filesystem; refuses Swarm URLs (path-addressed ≠ content-addressed). |
| `BeeBytesStore` | `(api_url, postage_batch_id="auto", deferred_upload=True, max_concurrent_reads=16, min_batch_ttl=86400)` | Swarm refs; `"auto"` picks the usable batch with the longest TTL. Reads are **not** verified — trust your own node (User Guide §7). |
| `BeeBytesStore.batch_status` | `(*, buckets=False)` | `(StampInfo, BucketStats \| None)` — batch health for renewal cron jobs. |
| `CachedBytesStore` | `(inner, max_bytes=67108864)` | LRU by bytes; safe (immutable blobs); unknown attributes delegate to `inner`. |

Addressing schemes: `sha256` (hex SHA-256) and `swarm` (Bee BMT reference,
erasure coding off — equals what `POST /bytes` returns).

## 6. Pointers

| pointer | signature | notes |
|---|---|---|
| `MemoryPointer` | `(root=None)` | atomic `compare_and_set` — in-process multi-writer is race-free. |
| `FilePointer` | `(path, keep_history=True)` | atomic file replace; no CAS. Keeps `<path>.timeline` (JSON) so the store can answer `history`/`undo`/`redo`; `keep_history=False` opts out. |
| `SwarmFeedPointer` | `(api_url, topic, *, signer=None, owner=None, postage_batch_id=None, feed_ttl=15.0, max_lookup_retries=15, retry_backoff=0.5, retry_backoff_cap=5.0)` | reads need `owner` only; writes need `signer` (+ batch). Best-effort CAS; feeds are last-write-wins. |

## 7. Assemblers

| function | signature | gives |
|---|---|---|
| `swarm_store` | `(topic, *, api_url="http://localhost:1633", stamp="auto", signer=None, owner=None, feed_ttl=15.0, deferred_upload=True, max_concurrent_reads=16)` | a `RecordStore` with Bee blobs and a feed pointer — the one place Swarm is chosen. |
| `local_first_store` | `(path, api_url=None, *, stamp="auto", max_bytes=None, cache_bytes=67108864, addressing="swarm", sync_policy=None, witness=None, publish_pointer=None, node_cache_size=65536)` | a `LocalFirstRecordStore`. `api_url=None` = local-only (push later). `max_bytes` budgets the directory (soft for pinned data). `publish_pointer` auto-publishes confirmed heads. |

## 8. `LocalFirstRecordStore` (adds to `RecordStore`)

| member | signature | semantics |
|---|---|---|
| `LocalFirstRecordStore.sync` | `(timeout=None)` | block until every commit is network-confirmed; `TimeoutError` (naming the last sync error) otherwise. |
| `LocalFirstRecordStore.sync_status` | `()` | swarmfs `StoreStatus`: pinned/evictable bytes, per-root rungs, `only_on_swarm_count`, `batch_expiries`. |
| `LocalFirstRecordStore.pin` | `(name, prefix="")` | hold every blob needed to read keys under `prefix` against eviction; returns the count. Re-pin after commits to track. |
| `LocalFirstRecordStore.unpin` | `(name)` | release a named pin. |
| `LocalFirstRecordStore.fetch` | `(prefix="")` | warm-up: materialize everything under `prefix` locally; returns blobs fetched. Idempotent. |
| `LocalFirstRecordStore.publish` | `(pointer, remote_name="feed")` | point `pointer` at the newest **network-confirmed** root on the lineage (or its nearest confirmed ancestor); returns it, or None. |
| `LocalFirstRecordStore.squash_history` | `(gc=True)` | collapse local history to the current root; returns `{"roots_dropped", "orphans_deleted", "bytes_freed"}`. Dropped *unpushed* history is gone for good. `ValueError` with staged changes. |
| `LocalFirstRecordStore.close` | `()` | stop the syncer, release the store lock. Also a context manager. |

The store directory holds `blobs/`, `journal.jsonl`, `HEAD`, `lock` —
format specified in swarmfs `docs/localstore-format.md`. Single writer per
directory (flock, POSIX).

## 9. Proofs

| name | signature | semantics |
|---|---|---|
| `RecordStore.prove` | `(key, addressing=None)` | JSON-ready envelope `{format, version, addressing, root, key, present, nodes, value}` carrying raw node blobs; self-verified before return. |
| `verify_proof` | `(proof, root)` | pure function, no store access: returns the record (inclusion) or `ABSENT` (absence); raises `ProofError` on any mismatch. |
| `PROOF_FORMAT` | constant | `"recordstore-trie-proof"`, version 1. Unknown formats must be ignored by readers. |

## 10. Errors

| raised | when |
|---|---|
| `KeyError` | `get`/`delete` of a key that does not exist under the root. |
| `RecordUnavailable` | the key exists but its value bytes are unreachable (evicted + offline; backend blob missing). Deliberately not a `KeyError`. |
| `MergeConflict` | both sides changed a key to different values and no resolver settled it (`.conflicts` lists the keys). |
| `ProofError` | proof/root mismatch — any tampering. |
| `ValueError` | empty/non-string key; NaN/Infinity; `prove` on staged keys or unknown addressing; `squash_history` with staged changes. |
| `TypeError` | non-JSON-encodable value; write on a read-only snapshot. |
| `RuntimeError` | `commit(reconcile=True)` could not land after `retries`; `sync()` on a store opened without `api_url`. |
| `TimeoutError` | `sync(timeout=…)` expired (message names the last sync error). |
