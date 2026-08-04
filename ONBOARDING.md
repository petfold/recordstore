# Onboarding: recordstore + swarmfs (the local-first Swarm stack)

Two sibling repos, developed together, released to PyPI separately:

- **`swarmfs`** (`~/projects/swarmfs`, v0.8.0) — an fsspec backend for
  Ethereum Swarm over a Bee node's HTTP API (`bzz://` immutable,
  `bzzf://` feed-mounted mutable), plus the **local-first storage layer**
  the whole stack now rests on: `swarmfs.localstore` + `swarmfs.localsync`.
- **`recordstore`** (`~/projects/recordstore`, v0.18.2) — a versioned
  key→record store (JSON values, atomic commits, canonical roots,
  three-way merge, verifiable proofs) over any content-addressed
  `BytesStore` — memory, local disk, S3, a Bee node, or the local-first
  store. First consumer of swarmfs's localstore. ontodag builds on it.

## The idea that organizes everything: local-first

You never choose between disk and Swarm anymore. Commits land on local
disk instantly (offline is the normal mode — no network, no postage stamp
at commit time), and a background worker pushes them to Swarm and
**confirms** arrival peer-to-peer. The one invariant everything protects:
**a blob is deleted locally only when the journal proves Swarm holds it**
— unpushed data is pinned (the disk budget is soft for it; you can always
save work), only network-confirmed blobs evict, and evicted reads heal by
verified re-fetch (bytes must hash to their reference).

Entry points:

```python
from recordstore import local_first_store
store = local_first_store("~/.myapp/store", "http://localhost:1633")
store.put("k", {"v": 1}); store.commit()   # local, instant, offline-safe
store.sync()                               # barrier: confirmed ON Swarm

import fsspec
fs = fsspec.filesystem("bzz", local_store="~/.myapp/fsstore", redundancy=0)
```

The durability ladder is `committed → pushed (on-node) → network-confirmed`;
`sync_status()` shows every root's rung, pinned vs evictable bytes, and
per-batch expiry estimates. Working-set controls: `pin(name, prefix)` /
`fetch(prefix)` (recordstore), `squash_history()` for local history
retention, `scrub()` for bitrot (localstore).

## Where the truth lives (read in this order)

| Document | What it is |
|---|---|
| `docs/REFERENCE.md` — in **every** repo of the cluster (recordstore, swarmfs, ontodag, ontodag-fs, swarmlite) | **Start here as an agent**: definition-first tables of every export, signature, error and extra — pinned against the code by each repo's `tests/test_reference.py`, so they cannot rot, and each carries a "version this file describes" line the tests compare to `pyproject.toml`. The user guides are the human tutorials; SPEC/CONTRACT files (ontodag-fs, ontodag) are the semantics contracts. Cross-repo doc pointers always go to these pinned references, never to hand-synced copies (ontodag's `recordstore-interface.md` is only the consumer-side view now). |
| `swarmfs/docs/localstore-design.md` | The design: invariant, ladder, auto-push policy, verification/trust (incl. why confirmation is p2p-native — Bee's stewardship retrieves through the network, verified from its source), performance posture. |
| `swarmfs/docs/localstore-format.md` | **Normative** on-disk format (blobs + append-only JSONL journal + disposable index). The format is the interop contract — a Go/JS implementation works from this file. The lag rule: journal events are appended only after the fact they record is true. |
| `swarmfs/docs/roadmap.md` §v3 | Phase history L0–L4 with findings pinned per phase — read the findings; they are the sharp edges. |
| `swarmfs/CLAUDE.md` | swarmfs's persistent brief: decisions, live-measured Bee facts (stamps, batch sizing), release procedure. |
| `recordstore/docs/USER_GUIDE.md` | recordstore tutorial incl. §3 local-first, §7 limitations/trust model. |
| `recordstore/ROADMAP.md` | The local-first track (R0–R3, all shipped) + the experimental POT track. |
| CHANGELOGs | recordstore keeps a real Keep-a-Changelog; swarmfs's history lives in the roadmap + CLAUDE.md version notes. |

## Sharp edges (each cost us a real bug or design correction)

- **Erasure coding forks the address space.** Local BMT refs equal the
  node's only with redundancy off. Local-first modes force `redundancy=0`
  and every push asserts the node returned the locally computed ref.
- **Canonicity means revisits.** Equal content ⇒ equal root (recordstore
  trie *and* mantaray manifests) — returning to an earlier state re-uses
  its old root, which the append-only journal refuses to re-record. That's
  why a `HEAD` pointer file lives beside the journal.
- **A blob-blind layer must push every root's event list.** A blob the tip
  references may be listed only in an ancestor's event — so
  push-latest-only is impossible in the worker; squashing is app-assisted
  (`squash_history` walks the trie and re-lists the reachable set).
- **Node claims are not network proof.** The push response promotes a root
  to *pushed* only; *confirmed* (which unlocks eviction) takes stewardship
  + sampled retrieve-and-verify. No gateways anywhere — the p2p network is
  the witness; `Syncer(witness=…)` exists only for a distrusted own node.
- **Old fsspec is real.** Debian/Ubuntu freeze dist-packages and pip never
  upgrades a satisfied floor — swarmfs crashed on fsspec < 2024.3.0
  transactions until 0.7.0; CI now pins `fsspec==2024.2.0` in a dedicated
  job so the compatibility is enforced, not remembered.
- **Single writer per store directory** (flock; POSIX-only v1). Within a
  process, app thread + sync worker share a mutex.

## Development workflow

- **Tests**: `python3 -m pytest tests/` in either repo. Offline suites run
  without a node; live tests gate on `SWARMFS_TEST_BEE=http://localhost:1633`
  (+ `SWARMFS_TEST_STAMP`; the one money-spending test additionally wants
  `SWARMFS_TEST_SPEND`). A local Bee 2.8.1 node usually runs on :1633.
  House style: pin live-measured facts in tests with the numbers in
  comments; never mock the trie/manifest formats.
- **Release** (identical across the cluster, and **docs come first** —
  Peter's standing rule): sweep README/user guide/roadmap/CHANGELOG and
  `docs/REFERENCE.md` — its "version this file describes" line is
  test-pinned to pyproject, so a version bump without the reference
  update fails the suite before the tag can ship. Then bump the version
  (swarmfs keeps it in `pyproject.toml` **and** `swarmfs/__init__.py`
  plus the CLAUDE.md narrative; the others in `pyproject.toml` +
  CHANGELOG), commit, `git tag vX.Y.Z && git push origin main vX.Y.Z`.
  The tag triggers publish via PyPI trusted publishing after CI re-runs
  the tests.
- **Dependency floors matter here**: `recordstore[local-first-swarm]` = swarmfs ≥ 0.9
  (keccak in swarmfs's base since 0.9; its `[feeds]` extra is signing-only now);
  `recordstore[swarm-only]` bundles the direct-on-Swarm trio,
  `ontodag-fs` = swarmfs ≥ 0.8 (`read_reference`/`reference_size` — the
  public raw-ref surface that replaced its private `_read_reference`
  reach; the extras' comments explain which floor buys what). swarmfs
  runtime needs only `fsspec>=2023.6` + `aiohttp`.

## What's deliberately NOT done

Foreign-content read caching (use fsspec's `blockcache::`/`simplecache::`
chaining — wrong granularity for the blob store); multi-process writers;
journal signing (single-user disk, threat model in the design doc);
predictive prefetching (explicit `pin`/`fetch` beats clever); a swarmfs
CLI (swarm-cli's job). The experimental canonical-POT track (recordstore
ROADMAP) is research, not scheduled.
