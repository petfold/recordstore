# Onboarding: recordstore + swarmfs (the local-first Swarm stack)

Two sibling repos, developed together, released to PyPI separately:

- **`swarmfs`** (`~/projects/swarmfs`, v0.11.1) — an fsspec backend for
  Ethereum Swarm over a Bee node's HTTP API (`bzz://` immutable,
  `bzzf://` feed-mounted mutable), plus the **local-first storage layer**
  the whole stack now rests on: `swarmfs.localstore` + `swarmfs.localsync`.
  Since 2026-09-11 also: **`swarmfs mount`** — the package's first and only
  console script, a FUSE mount of any reference or feed, read-only by
  default and writable with `--rw` (one commit per saved file, the new
  root printed at unmount) — and **ACT access control** (`act=True` /
  `act_history=`, grantee management), plus encryption (0.9).
- **`recordstore`** (`~/projects/recordstore`, v0.20.2) — a versioned
  key→record store (JSON values, atomic commits, canonical roots,
  three-way merge, verifiable proofs) over any content-addressed
  `BytesStore` — memory, local disk, S3, a Bee node, or the local-first
  store. First consumer of swarmfs's localstore. ontodag builds on it.
- **`ontodag-fs`** (`~/projects/ontodag-fs`, v0.6.0) — a concept lattice as
  a filesystem over swarmfs bytes; since 2026-09-11 it *files* too
  (`put_file`/`rm`/`mv`/`cp`, and `odag-fs mount --rw` through the kernel
  via swarmfs's mounter).

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
retention, `scrub()` for bitrot (localstore). Version navigation is
separate from that ladder: `history()`/`undo()`/`redo()`/`checkout()` walk
the **pointer's timeline** (the branch), while the journal keeps every root
ever committed (the reflog).

## Where the truth lives (read in this order)

| Document | What it is |
|---|---|
| `docs/REFERENCE.md` — in **every** repo of the cluster (recordstore, swarmfs, ontodag, ontodag-fs, swarmlite) | **Start here as an agent**: definition-first tables of every export, signature, error and extra — pinned against the code by each repo's `tests/test_reference.py`, so they cannot rot, and each carries a "version this file describes" line the tests compare to `pyproject.toml`. The user guides are the human tutorials; SPEC/CONTRACT files (ontodag-fs, ontodag) are the semantics contracts. Cross-repo doc pointers always go to these pinned references, never to hand-synced copies (ontodag's `recordstore-interface.md` is only the consumer-side view now). |
| `swarmfs/docs/localstore-design.md` | The design: invariant, ladder, auto-push policy, verification/trust (incl. why confirmation is p2p-native — Bee's stewardship retrieves through the network, verified from its source), performance posture. |
| `swarmfs/docs/localstore-format.md` | **Normative** on-disk format (blobs + append-only JSONL journal + disposable index). The format is the interop contract — a Go/JS implementation works from this file. The lag rule: journal events are appended only after the fact they record is true. |
| `swarmfs/ROADMAP.md` (moved to the repo root 2026-09-11) §v3, § Standalone FUSE mount, § Later/ACT | Phase history with findings pinned per phase — read the findings; they are the sharp edges. |
| `swarmfs/CLAUDE.md` | swarmfs's persistent brief: decisions, live-measured Bee facts (stamps, batch sizing, ACT's real contract, FUSE's release/flush timing), release procedure. |
| `recordstore/docs/USER_GUIDE.md` | recordstore tutorial incl. §3 local-first, §7 limitations/trust model. |
| `recordstore/ROADMAP.md` | The local-first track (R0–R3, all shipped) + the experimental POT track. |
| `ontodag-fs/SPEC.md`, `DESIGN_DECISIONS.md` (#23: the `rm` retraction rule) | The filesystem-projection contract and its reasoning. |
| CHANGELOGs | Every repo keeps a Keep-a-Changelog now (swarmfs's was back-filled 2026-09-11). |

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
- **fsspec's generic FUSE wrapper is not usable raw.** It lets every
  exception but `FileNotFoundError` escape (EINVAL + traceback), reports
  `0777` and a timestamp that changes per `stat`, and its write path
  `seek()`s a write-mode buffered file — it never worked for any fsspec
  backend. swarmfs subclasses it; ontodag-fs mounts through swarmfs's
  mounter (`mount(fs=...)`) rather than the raw wrapper.
- **FUSE `release` is asynchronous** — delivered after `close(2)` returns —
  so commits happen in `flush`/`fsync` (synchronous with close, error
  returned to the caller). And the shell's `> file` closes a duplicated
  descriptor *before* writing, so a created file must not count as dirty
  until bytes arrive, or an empty object lands first (measured on
  ontodag-fs: two objects, one label).
- **ACT wraps only the root reference** and Bee decrypts it with the
  node's own key: reads need history + publisher key (both mandatory),
  work only through your own node, and children resolve without headers
  (a plain ref *with* headers is a 404). Plain ACT leaves content
  plaintext-addressable — the underlying ref is the ETag — so swarmfs's
  `act=True` implies `encrypt=True`. The public gateway's `/health` is
  plain text, not Bee's JSON.
- **`rm` in a lattice projection retracts what the path asserts, never by
  implication** (ontodag-fs #23): removing `rex.jpg` at `/pet` when he is
  filed as `dog` is refused with the paths where he is asserted, because
  retracting `dog` would silently drop `/mammal` too.

## Development workflow

- **Tests**: `python3 -m pytest tests/` in either repo. Offline suites run
  without a node; live tests gate on `SWARMFS_TEST_BEE=http://localhost:1633`
  (+ `SWARMFS_TEST_STAMP`; the one money-spending test additionally wants
  `SWARMFS_TEST_SPEND`). A local Bee 2.8.2 light node usually runs on :1633.
  Kernel-mount tests carry the `fuse` marker (swarmfs runs them by default
  and installs libfuse2 in CI; ontodag-fs deselects them by default —
  `pytest -m fuse`). READMEs quote a test count that a CI-only guard pins.
  House style: pin live-measured facts in tests with the numbers in
  comments; never mock the trie/manifest formats; run the live smoke
  before calling a write path done — it found the `mv` over-retraction and
  the shell double-commit that the fakes could not.
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
  `ontodag-fs` = swarmfs ≥ 0.11.1 (`mount(fs=..., rw=True)`; before that
  0.8 for `read_reference`/`reference_size`). swarmfs runtime needs only
  `fsspec>=2023.6` + `aiohttp` + `eth-hash`; `swarmfs[fuse]` adds fusepy
  (system libfuse **2** required — fusepy does not load libfuse3).

## What's deliberately NOT done

Foreign-content read caching (use fsspec's `blockcache::`/`simplecache::`
chaining — wrong granularity for the blob store); multi-process writers;
journal signing (single-user disk, threat model in the design doc);
predictive prefetching (explicit `pin`/`fetch` beats clever); a swarmfs
CLI for stamps/uploads/feeds (swarm-cli's job — `swarmfs mount` is the one
console script, because a mount is a process); `mkdir` as concept creation
in ontodag-fs (the lattice is edited with `odag`); xattr exposure of
intents (needs the mounter to grow an xattr path). The experimental
canonical-POT track (recordstore ROADMAP) is research, not scheduled.
