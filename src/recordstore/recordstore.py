"""recordstore: a versioned record store over a content-addressed bytes store.

This is the thin database kernel between Swarm (immutable chunks + a mutable
feed pointer) and an application that wants to think in records and versions.
It knows nothing about graphs, edges, or ontologies.

Model
-----
- A *record* is any JSON-compatible value, stored under a string key.
- All records live in a persistent (copy-on-write) compacted radix trie whose
  nodes are each stored as one content-addressed blob; the trie's root
  reference identifies one immutable, self-consistent snapshot of the entire
  dataset.
- Mutations are staged in memory and flushed by `commit()`, which produces a
  single new root reference. Readers pin a root and see a frozen snapshot.
- Encodings are canonical (sorted keys, fixed separators), so equal content
  yields byte-equal blobs and therefore an equal root: same dataset =>
  same root reference, regardless of insertion order or history.

Layering
--------
  BytesStore  : put(bytes) -> ref, get(ref) -> bytes      (Memory / Bee HTTP)
  Trie        : canonical persistent radix trie over the bytes store
  RecordStore : staging, commit, snapshots, prefix iteration
  Pointer     : mutable "latest root" (Memory / File / Swarm feed)

`swarm_store(topic, ...)` is the one call that puts a whole store on Swarm:
blobs in a Bee node, latest-root in a Swarm feed. Everything above stays
backend-neutral.

Nothing above this layer should ever see a stored blob or a trie node.
"""

from __future__ import annotations

import json
import hashlib
import os
import time
import warnings
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, Iterator, List, Optional, Protocol, Tuple

Ref = str  # hex-encoded reference to a stored blob

_SCHEMA_VERSION = 1
_TOMBSTONE = object()

# Merge sentinels (see RecordStore.merge).
ABSENT = object()   # a resolver sees this for a side where the key is absent
DELETE = object()   # a resolver returns this to drop a key from the merge


class MergeConflict(Exception):
    """Raised by `RecordStore.merge` when both sides changed the same key to
    different values and no `resolver` settled it. `.conflicts` is the list of
    conflicting keys."""

    def __init__(self, conflicts):
        self.conflicts = list(conflicts)
        shown = ", ".join(sorted(self.conflicts)[:5])
        if len(self.conflicts) > 5:
            shown += ", ..."
        super().__init__(
            f"unresolved merge conflict on {len(self.conflicts)} key(s): {shown}")


# ---------------------------------------------------------------------------
# Canonical encoding
# ---------------------------------------------------------------------------

def canonical_bytes(obj) -> bytes:
    """Deterministic byte encoding: equal values => equal bytes.

    Content addressing makes this a correctness requirement, not a style
    choice. Rejects NaN/Infinity (not canonical in JSON).
    """
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False,
    ).encode("utf-8")


def _common_prefix(a: bytes, b: bytes) -> bytes:
    """Longest shared byte prefix of `a` and `b`. Leaner than
    `os.path.commonprefix` (no list/min/max wrapping) — this is on the trie's
    hot insert path."""
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return a[:i]


def _encode_value(value) -> bytes:
    return canonical_bytes({"rsv": _SCHEMA_VERSION, "val": value})


def _decode_value(data: bytes):
    obj = json.loads(data.decode("utf-8"))
    if obj.get("rsv") != _SCHEMA_VERSION:
        raise ValueError(f"unsupported record schema version: {obj.get('rsv')!r}")
    return obj["val"]


# ---------------------------------------------------------------------------
# Bytes store backends
# ---------------------------------------------------------------------------

class BytesStore(Protocol):
    # A backend may also implement `get_many(refs) -> {ref: bytes}` and
    # `put_many(datas) -> [ref]` for batched/parallel I/O; recordstore uses them
    # when present and falls back to serial `get`/`put` otherwise.
    def put(self, data: bytes) -> Ref: ...
    def get(self, ref: Ref) -> bytes: ...


class MemoryBytesStore:
    """In-memory content-addressed store; the test double for Swarm."""

    def __init__(self):
        self.blobs: Dict[Ref, bytes] = {}

    def put(self, data: bytes) -> Ref:
        ref = hashlib.sha256(data).hexdigest()
        self.blobs[ref] = data
        return ref

    def get(self, ref: Ref) -> bytes:
        try:
            return self.blobs[ref]
        except KeyError:
            raise KeyError(f"reference not found: {ref}") from None

    def get_many(self, refs: Iterable[Ref]) -> Dict[Ref, bytes]:
        return {ref: self.get(ref) for ref in refs}

    def put_many(self, datas: Iterable[bytes]) -> List[Ref]:
        return [self.put(d) for d in datas]

    def __len__(self):
        return len(self.blobs)


def _sha256_ref(data: bytes) -> Ref:
    return hashlib.sha256(data).hexdigest()


def _swarm_ref(data: bytes) -> Ref:
    """Swarm's own reference for `data`, computed locally via swarmfs.

    Choosing this makes a local store share Swarm's address space, so the same
    dataset has the *same* root whether it lives in a directory or on Swarm —
    develop offline, publish later, nothing re-addressed. It costs a dependency
    (`swarmfs[feeds]`, for keccak256) and is slower than sha256, since it
    builds the whole chunk tree.

    Caveat inherited from Swarm: this is the reference for a *plain* upload.
    A Bee node that adds erasure coding (many default to it) returns a
    different root for the same bytes, so an offline mirror only stays
    address-compatible if you upload with redundancy disabled.
    """
    try:
        from swarmfs.splitter import content_address
    except ImportError:
        raise ImportError(
            "addressing='swarm' needs swarmfs with keccak256: "
            'pip install "swarmfs[feeds]"'
        ) from None
    return content_address(data).hex()


_ADDRESSING = {"sha256": _sha256_ref, "swarm": _swarm_ref}


def _resolve_addressing(addressing):
    """`'sha256'`, `'swarm'`, or any `bytes -> str` callable."""
    if callable(addressing):
        return addressing
    try:
        return _ADDRESSING[addressing]
    except KeyError:
        raise ValueError(
            f"unknown addressing {addressing!r}; use "
            f"{sorted(_ADDRESSING)} or a bytes->str callable"
        ) from None


class DirBytesStore:
    """Durable content-addressed blobs in a local directory.

    The gap this fills: `MemoryBytesStore` forgets everything on exit and
    `BeeBytesStore` needs a node and a postage batch, so there was no way to
    keep a versioned store on ordinary disk. Here the file *name* is the
    reference, which is all content addressing needs.

    ```python
    store = RecordStore(DirBytesStore("~/.myapp/blobs"),
                        pointer=FilePointer("~/.myapp/root"))
    ```

    - **Addressing** is `"sha256"` by default (matching `MemoryBytesStore`, so
      roots are portable between the two). Pass `addressing="swarm"` to name
      blobs by their Swarm reference instead, making the directory an offline
      mirror of Swarm's address space — see `_swarm_ref` for the trade-off.
    - **Writes are atomic and idempotent**: content goes to a temp file that is
      `os.replace`d into place, so a crash never leaves a torn blob, and
      re-putting existing content skips the write entirely.
    - **Names are fanned out** two hex characters deep (`ab/cdef…`), so a store
      with a million blobs does not become one unlistable directory.
    """

    def __init__(self, path: str, addressing="sha256"):
        self.path = os.path.abspath(os.path.expanduser(path))
        self._ref_of = _resolve_addressing(addressing)
        os.makedirs(self.path, exist_ok=True)

    def _blob_path(self, ref: Ref) -> str:
        return os.path.join(self.path, ref[:2], ref[2:])

    def put(self, data: bytes) -> Ref:
        ref = self._ref_of(data)
        target = self._blob_path(ref)
        if os.path.exists(target):
            return ref                      # content-addressed: already correct
        os.makedirs(os.path.dirname(target), exist_ok=True)
        tmp = target + f".tmp{os.getpid()}"
        with open(tmp, "wb") as fh:
            fh.write(data)
        os.replace(tmp, target)
        return ref

    def get(self, ref: Ref) -> bytes:
        try:
            with open(self._blob_path(ref), "rb") as fh:
                return fh.read()
        except FileNotFoundError:
            raise KeyError(f"reference not found: {ref}") from None

    def get_many(self, refs: Iterable[Ref]) -> Dict[Ref, bytes]:
        return {ref: self.get(ref) for ref in refs}

    def put_many(self, datas: Iterable[bytes]) -> List[Ref]:
        return [self.put(d) for d in datas]

    def __len__(self):
        return sum(len(files) for _, _, files in os.walk(self.path))


class FsspecBytesStore:
    """Content-addressed blobs on any [fsspec](https://filesystem-spec.readthedocs.io)
    filesystem: a local directory, S3, GCS, Azure, HTTP, SFTP, memory…

    ```python
    store = RecordStore(FsspecBytesStore("s3://my-bucket/blobs"))
    ```

    **Not for `bzz://`.** fsspec is *path*-addressed — you choose the key — while
    Swarm is *content*-addressed: the reference is the result of the write, not
    an input to it. Pointing this at Swarm would store blobs at
    `bzz://…/<sha256>` paths inside a manifest, discarding Swarm's own
    addressing. Use `BeeBytesStore` (or `swarm_store`) instead; this class
    refuses the protocol rather than silently doing the wrong thing.

    Addressing and the fan-out layout match `DirBytesStore`.
    """

    def __init__(self, url: str, addressing="sha256", **storage_options):
        try:
            import fsspec
        except ImportError:
            raise ImportError(
                'FsspecBytesStore needs fsspec: pip install "recordstore[fsspec]"'
            ) from None
        protocol = url.split("://", 1)[0] if "://" in url else "file"
        if protocol in ("bzz", "bzzf"):
            raise ValueError(
                "FsspecBytesStore cannot address Swarm: fsspec is path-addressed, "
                "but a Swarm reference is produced *by* the write. Use "
                "BeeBytesStore(api_url, batch) — or swarm_store(topic, ...) for a "
                "whole store on Swarm — instead."
            )
        self.fs, self.base = fsspec.core.url_to_fs(url, **storage_options)
        self._ref_of = _resolve_addressing(addressing)
        self.fs.makedirs(self.base, exist_ok=True)

    def _blob_path(self, ref: Ref) -> str:
        return f"{self.base.rstrip('/')}/{ref[:2]}/{ref[2:]}"

    def put(self, data: bytes) -> Ref:
        ref = self._ref_of(data)
        target = self._blob_path(ref)
        if self.fs.exists(target):
            return ref
        parent = target.rsplit("/", 1)[0]
        self.fs.makedirs(parent, exist_ok=True)
        with self.fs.open(target, "wb") as fh:
            fh.write(data)
        return ref

    def get(self, ref: Ref) -> bytes:
        try:
            return self.fs.cat_file(self._blob_path(ref))
        except FileNotFoundError:
            raise KeyError(f"reference not found: {ref}") from None

    def get_many(self, refs: Iterable[Ref]) -> Dict[Ref, bytes]:
        refs = list(refs)
        if not refs:
            return {}
        paths = {self._blob_path(r): r for r in refs}
        # fsspec's cat() fetches many paths concurrently where the backend can
        got = self.fs.cat(list(paths))
        if isinstance(got, bytes):          # single-path calls return raw bytes
            got = {next(iter(paths)): got}
        out = {}
        for path, data in got.items():
            key = paths.get(path) or paths.get("/" + path.lstrip("/"))
            if key is None:                 # backends may normalise differently
                key = paths[next(p for p in paths if p.endswith(path.split("/")[-1]))]
            out[key] = data
        missing = set(refs) - set(out)
        if missing:
            raise KeyError(f"reference not found: {sorted(missing)[0]}")
        return out

    def put_many(self, datas: Iterable[bytes]) -> List[Ref]:
        return [self.put(d) for d in datas]


#: Minimum remaining validity a batch must have to be picked by ``"auto"``.
#: swarmfs's own floor is 60 s, which is right for a one-shot upload and
#: wrong for a record store: a batch with a minute left would be selected
#: and everything written under it would die with it. A day is the smallest
#: span the network itself will sell.
AUTO_MIN_BATCH_TTL = 86400

#: Below this much remaining validity, selecting a batch warns. Renewal is
#: the only cure and it must happen *before* expiry — the node drops an
#: expired batch, a topup against it fails, and the chunks it paid for
#: become the first candidates for eviction.
WARN_BATCH_TTL = 7 * 86400

#: Fullest-bucket occupancy (0-1) above which an immutable batch warns.
#: Chunks land in 65536 buckets by their address; when one fills, further
#: chunks hashing there are refused (HTTP 402 "batch is overissued") even
#: though the batch has capacity elsewhere. A record store keeps appending,
#: so it walks into this rather than hitting it all at once.
WARN_BUCKET_RATIO = 0.8


def _stamp_manager(api_url: str, min_ttl: int):
    """``(client, StampManager)`` from swarmfs, imported lazily like requests."""
    try:
        from swarmfs._client import SwarmClient
        from swarmfs.stamps import StampManager
    except ImportError:
        raise ImportError(
            "postage_batch_id='auto' needs swarmfs for stamp selection — "
            "install it (pip install 'recordstore[stamps]') or pass an "
            "explicit batch id (see GET /stamps on your node)"
        ) from None
    client = SwarmClient(api_url)
    mgr = StampManager(client, min_ttl=min_ttl)
    # probe the newest thing this module depends on, not just any new name
    if not hasattr(mgr, "buckets"):
        raise ImportError(
            "stamp inspection needs swarmfs >= 0.4.0 (this one lacks "
            "StampManager.buckets); upgrade with "
            "pip install -U 'swarmfs>=0.4.0'"
        )
    return client, mgr


def batch_status(api_url: str, batch_id: str, *, buckets: bool = False):
    """A batch's health: ``(StampInfo, BucketStats | None)``.

    The two numbers that decide whether this store keeps working are the
    remaining validity (``info.ttl``) and how full its fullest bucket is
    (``info.utilization`` of ``info.bucket_capacity``). Pass
    ``buckets=True`` for the node's exact per-bucket histogram instead of
    the summary — authoritative, but a ~2 MB response.

    Read-only, and spends nothing. Renewal is deliberately not offered
    here: see the module docstring on why a library must not spend the
    node wallet's xBZZ.
    """
    import asyncio

    async def inspect():
        client, mgr = _stamp_manager(api_url, AUTO_MIN_BATCH_TTL)
        try:
            info = await mgr.get_batch(batch_id)
            stats = await mgr.buckets(batch_id) if buckets else None
            return info, stats
        finally:
            await client.close()

    return asyncio.run(inspect())


def _warn_about(info) -> None:
    """Warn when a selected batch is heading for a failure the caller can
    still prevent. Both conditions are silent until they bite otherwise."""
    if 0 <= info.ttl < WARN_BATCH_TTL:
        warnings.warn(
            f"postage batch {info.batch_id[:8]}… has {info.ttl / 86400:.1f} "
            "days of validity left; everything written under it stops being "
            "paid for at expiry, and an expired batch cannot be revived. "
            "Renew it now (swarmfs: StampManager.plan_topup/topup, or "
            "'swarmlite stamps topup <id> --for 4w' if you have swarmlite).",
            stacklevel=3,
        )
    ratio = info.utilization_ratio
    if info.immutable and ratio is not None and ratio >= WARN_BUCKET_RATIO:
        warnings.warn(
            f"postage batch {info.batch_id[:8]}… is {ratio:.0%} through its "
            f"bucket capacity ({info.utilization} of {info.bucket_capacity} "
            "chunks in the fullest of 65536 buckets). Further writes risk "
            "HTTP 402 'batch is overissued'. That does not lose what is "
            "already stored: dilute one depth to double every bucket "
            "(swarmfs: StampManager.dilute) and top up afterwards, since "
            "dilution halves the remaining validity.",
            stacklevel=3,
        )


def _auto_batch(api_url: str, min_ttl: int = AUTO_MIN_BATCH_TTL) -> str:
    """Resolve 'auto' to a validated usable batch id via swarmfs's
    StampManager (an optional dependency, imported lazily like requests).

    Rejects batches with less than ``min_ttl`` seconds left, and warns when
    the one it picks is close to expiry or to a full bucket — a record store
    outlives the one-shot upload swarmfs's 60 s floor is written for.

    Selection only, never purchase: a library must not spend the node
    wallet's xBZZ on its own. To buy programmatically use swarmfs
    (``StampManager.plan``/``buy``) and pass the resulting id here; to renew
    one, ``plan_topup``/``topup``.
    """
    import asyncio

    async def resolve() -> str:
        client, mgr = _stamp_manager(api_url, min_ttl)
        try:
            batch_id = await mgr.resolve("auto")
            _warn_about(await mgr.get_batch(batch_id))
            return batch_id
        finally:
            await client.close()

    return asyncio.run(resolve())


class BeeBytesStore:
    """BytesStore over a Bee node's `/bytes` endpoint.

    Named for the endpoint it actually uses: `/bytes` is Bee's blob-level
    API, not the raw `/chunks/{address}` single-chunk primitive. Values of
    any length are handled transparently — Bee's splitter turns the payload
    into a chunk tree server-side and returns one reference. Requires a
    usable postage batch id for writes; ``"auto"`` picks one via swarmfs
    (validated, longest TTL — see ``_auto_batch``; selection only, buying
    is deliberately left to the caller).
    """

    def __init__(self, api_url: str, postage_batch_id: str = "auto",
                 deferred_upload: bool = True, max_concurrent_reads: int = 16,
                 min_batch_ttl: int = AUTO_MIN_BATCH_TTL):
        import requests  # lazy: only needed for the real backend
        self.api_url = api_url.rstrip("/")
        if postage_batch_id in (None, "auto"):
            postage_batch_id = _auto_batch(self.api_url, min_batch_ttl)
        self.batch = postage_batch_id
        self.deferred = deferred_upload
        self.max_concurrent_reads = max(1, max_concurrent_reads)
        # A persistent session with a connection pool: keep-alive avoids a fresh
        # TCP (and TLS) handshake on every blob op — the dominant per-op cost on
        # a high-latency link — and gives the read pool reusable connections.
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=1, pool_maxsize=self.max_concurrent_reads
        )
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def batch_status(self, *, buckets: bool = False):
        """This store's postage batch health — ``(StampInfo, BucketStats |
        None)``. Cron this to learn that the batch needs renewing while
        renewal is still possible; see :func:`batch_status`."""
        return batch_status(self.api_url, self.batch, buckets=buckets)

    def put(self, data: bytes) -> Ref:
        r = self._session.post(
            f"{self.api_url}/bytes",
            data=data,
            headers={
                "Content-Type": "application/octet-stream",
                "Swarm-Postage-Batch-Id": self.batch,
                "Swarm-Deferred-Upload": "true" if self.deferred else "false",
            },
            timeout=120,
        )
        if r.status_code == 402:
            # This store owns its transport, so it does not inherit swarmfs's
            # 402 handling. The two 402s mean different things and only one is
            # recoverable, so say which: "overissued" is a full bucket, not a
            # dead stamp, and nothing already stored is lost.
            detail = r.text[:200]
            if "overissued" in detail:
                raise RuntimeError(
                    f"postage batch {self.batch[:8]}… refused this chunk: a "
                    f"bucket is full ({detail}). Nothing already stored is "
                    "lost. Dilute the batch one depth to double every "
                    "bucket's capacity and retry (swarmfs: "
                    "StampManager.dilute, or 'swarmlite stamps dilute <id> "
                    "--depth N'), then top up — dilution halves the "
                    "remaining validity."
                )
            raise RuntimeError(
                f"the node did not accept postage batch {self.batch[:8]}… "
                f"({detail}). Check it with GET /stamps/{self.batch}; if it "
                "expired, a new batch is the only option — expired batches "
                "cannot be revived."
            )
        r.raise_for_status()
        return r.json()["reference"]

    def get_many(self, refs: Iterable[Ref]) -> Dict[Ref, bytes]:
        """Fetch many references concurrently — the fast path for hydrating a
        store over a network backend, where each read is otherwise one serial
        HTTP round trip (painful on a high-latency link). Reads are safe to
        parallelise freely: everything here is immutable and content-addressed,
        so there is nothing to lock."""
        refs = list(refs)
        if not refs:
            return {}
        workers = min(self.max_concurrent_reads, len(refs))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return dict(zip(refs, pool.map(self.get, refs)))

    def put_many(self, datas: Iterable[bytes]) -> List[Ref]:
        """Upload independent blobs concurrently, preserving order. Used for a
        commit's value blobs, which have no dependencies on one another."""
        datas = list(datas)
        if not datas:
            return []
        workers = min(self.max_concurrent_reads, len(datas))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(self.put, datas))

    def get(self, ref: Ref) -> bytes:
        r = self._session.get(f"{self.api_url}/bytes/{ref}", timeout=120)
        if r.status_code == 404:
            raise KeyError(f"reference not found: {ref}")
        r.raise_for_status()
        return r.content


class CachedBytesStore:
    """A byte-budgeted in-memory LRU cache in front of any BytesStore.

    Closes the gap that made every re-read of a committed record a fresh
    backend fetch: value blobs were never cached (only decoded trie nodes
    were). Wrap any backend —
    ``RecordStore(CachedBytesStore(BeeBytesStore(...)))`` — and repeat
    reads are served from memory. Safe by construction: blobs are
    immutable and content-addressed, so a cached entry can never go stale.

    ``max_bytes`` bounds the cache, LRU-evicted (this is a transparent
    accelerator, not a replica — it may drop anything; the inner store is
    authoritative). A blob larger than the whole budget is served but
    never cached. Unknown attributes delegate to the inner store, so
    backend extras (``batch_status``, a local-first store's
    ``commit_root``/``status``) keep working through the wrapper.
    """

    def __init__(self, inner: BytesStore, max_bytes: int = 64 * 1024 * 1024):
        self.inner = inner
        self.max_bytes = max_bytes
        self._cache: "OrderedDict[Ref, bytes]" = OrderedDict()
        self._bytes = 0

    def _remember(self, ref: Ref, data: bytes) -> None:
        if len(data) > self.max_bytes:
            return
        if ref in self._cache:
            self._bytes -= len(self._cache.pop(ref))
        self._cache[ref] = data
        self._bytes += len(data)
        while self._bytes > self.max_bytes:
            _, dropped = self._cache.popitem(last=False)
            self._bytes -= len(dropped)

    def put(self, data: bytes) -> Ref:
        ref = self.inner.put(data)
        self._remember(ref, data)
        return ref

    def get(self, ref: Ref) -> bytes:
        data = self._cache.get(ref)
        if data is not None:
            self._cache.move_to_end(ref)
            return data
        data = self.inner.get(ref)
        self._remember(ref, data)
        return data

    def put_many(self, datas: Iterable[bytes]) -> List[Ref]:
        datas = list(datas)
        put_many = getattr(self.inner, "put_many", None)
        refs = (put_many(datas) if put_many
                else [self.inner.put(d) for d in datas])
        for ref, data in zip(refs, datas):
            self._remember(ref, data)
        return refs

    def get_many(self, refs: Iterable[Ref]) -> Dict[Ref, bytes]:
        refs = list(refs)
        out, missing = {}, []
        for ref in refs:
            data = self._cache.get(ref)
            if data is not None:
                self._cache.move_to_end(ref)
                out[ref] = data
            else:
                missing.append(ref)
        if missing:
            get_many = getattr(self.inner, "get_many", None)
            fetched = (get_many(missing) if get_many
                       else {r: self.inner.get(r) for r in missing})
            for ref, data in fetched.items():
                self._remember(ref, data)
            out.update(fetched)
        return {r: out[r] for r in refs}

    def __getattr__(self, name):
        return getattr(self.inner, name)


# ---------------------------------------------------------------------------
# Persistent compacted radix trie (canonical)
#
# Node wire format (canonical JSON):
#   {"tn": 1, "p": "<hex prefix>", "v": "<value ref>"|null, "c": {"<hex byte>": "<ref>", ...}}
#
# Canonical-form invariants (make the structure a pure function of content):
#   - a node with no value and no children does not exist (empty map => root None)
#   - a node with no value and exactly one child is merged into that child
# ---------------------------------------------------------------------------

class _Node:
    __slots__ = ("prefix", "value_ref", "children")

    def __init__(self, prefix: bytes, value_ref: Optional[Ref],
                 children: Dict[int, Ref]):
        self.prefix = prefix
        self.value_ref = value_ref
        self.children = children  # first-byte -> child node ref


#: Default bound on the decoded-node cache: ~65k nodes at a few hundred
#: bytes each is tens of MB — plenty of locality, safe for stores whose
#: node count dwarfs RAM. Raise it for hot huge stores, lower it for tight
#: memory; correctness never depends on it (nodes are immutable and
#: re-fetchable).
DEFAULT_NODE_CACHE_SIZE = 65536


class _NodeCache:
    """Bounded LRU over decoded trie nodes. Any entry may be dropped —
    except commit-scoped `pending:` placeholders, which exist only here
    until `_flush` resolves them; evicting one mid-commit would lose the
    node, so they are exempt (and `_reset_buffer` removes them)."""

    def __init__(self, maxsize: int):
        self.maxsize = max(1, maxsize)
        self._d: "OrderedDict[Ref, _Node]" = OrderedDict()

    def get(self, ref: Ref) -> Optional["_Node"]:
        node = self._d.get(ref)
        if node is not None:
            self._d.move_to_end(ref)
        return node

    def __contains__(self, ref: Ref) -> bool:
        return ref in self._d

    def __len__(self) -> int:
        return len(self._d)

    def __setitem__(self, ref: Ref, node: "_Node") -> None:
        d = self._d
        if ref in d:
            d.move_to_end(ref)
        d[ref] = node
        while len(d) > self.maxsize:
            victim = next((k for k in d if not k.startswith("pending:")),
                          None)
            if victim is None:
                break  # only pending placeholders left: keep them all
            del d[victim]

    def pop(self, ref: Ref, default=None):
        return self._d.pop(ref, default)


class _Trie:
    def __init__(self, bytes_store: BytesStore,
                 cache_size: int = DEFAULT_NODE_CACHE_SIZE):
        self._blobs = bytes_store
        self._cache = _NodeCache(cache_size)  # nodes are immutable => safe
        # Commit-scoped write buffer. While buffering, `_store` defers to
        # placeholder refs instead of uploading; `_flush` then writes only the
        # nodes surviving in the final root, bottom-up and one level per batch.
        self._buffering = False
        self._pending: Dict[Ref, _Node] = {}
        self._pn = 0

    # -- node io -----------------------------------------------------------

    @staticmethod
    def _decode(data: bytes) -> _Node:
        obj = json.loads(data.decode("utf-8"))
        if obj.get("tn") != 1:
            raise ValueError("not a trie node or unsupported version")
        return _Node(
            bytes.fromhex(obj["p"]),
            obj["v"],
            {int(k, 16): v for k, v in obj["c"].items()},
        )

    def _load(self, ref: Ref) -> _Node:
        node = self._cache.get(ref)
        if node is None:
            node = self._decode(self._blobs.get(ref))
            self._cache[ref] = node
        return node

    def _load_many(self, refs: List[Ref]) -> Dict[Ref, _Node]:
        """Load several nodes, fetching the uncached ones in one batch so a
        network store can parallelise the round trips (falls back to serial
        `get` if the store has no `get_many`)."""
        out: Dict[Ref, _Node] = {}
        for r in set(refs):
            node = self._cache.get(r)
            if node is not None:
                out[r] = node
        missing = [r for r in set(refs) if r not in out]
        if missing:
            get_many = getattr(self._blobs, "get_many", None)
            blobs = (get_many(missing) if get_many
                     else {r: self._blobs.get(r) for r in missing})
            for r in missing:
                node = self._decode(blobs[r])
                self._cache[r] = node
                out[r] = node  # held here even if the LRU evicts it
        return {r: out[r] for r in refs}

    @staticmethod
    def _serialize(prefix: bytes, value_ref: Optional[Ref],
                   children: Dict[int, Ref]) -> bytes:
        return canonical_bytes({
            "tn": 1,
            "p": prefix.hex(),
            "v": value_ref,
            "c": {format(b, "02x"): r for b, r in sorted(children.items())},
        })

    def _store(self, node: _Node) -> Ref:
        if self._buffering:
            # Defer: hand back a placeholder. The real (server-assigned) ref is
            # resolved bottom-up in `_flush`, once this node's children are real.
            pid = f"pending:{self._pn}"
            self._pn += 1
            self._pending[pid] = node
            self._cache[pid] = node  # so `_load` serves it during the build
            return pid
        ref = self._blobs.put(
            self._serialize(node.prefix, node.value_ref, node.children))
        self._cache[ref] = node
        return ref

    def _flush(self, root: Optional[Ref]) -> Optional[Ref]:
        """Write the buffered nodes reachable from `root`, bottom-up with one
        concurrent batch per level, and return the real root ref. Nodes not
        reachable from the final root (orphaned intermediates left by
        one-key-at-a-time insertion) are simply never written."""
        if root is None or root not in self._pending:
            return root  # empty result, or the root subtree was unchanged
        reachable = set()
        stack = [root]
        while stack:
            pid = stack.pop()
            if pid in reachable:
                continue
            reachable.add(pid)
            for cref in self._pending[pid].children.values():
                if cref in self._pending:
                    stack.append(cref)
        put_many = getattr(self._blobs, "put_many", None)
        resolved: Dict[Ref, Ref] = {}
        remaining = set(reachable)
        while root not in resolved:
            ready = [pid for pid in remaining
                     if all(c not in self._pending or c in resolved
                            for c in self._pending[pid].children.values())]
            if not ready:  # impossible for an acyclic trie; guard against a hang
                raise RuntimeError("trie flush stalled: no writable nodes")
            batch = []  # (pid, node-with-real-children, bytes)
            for pid in ready:
                node = self._pending[pid]
                children = {b: resolved.get(c, c) for b, c in node.children.items()}
                real = _Node(node.prefix, node.value_ref, children)
                batch.append((pid, real, self._serialize(
                    real.prefix, real.value_ref, real.children)))
            datas = [b[2] for b in batch]
            refs = put_many(datas) if put_many else [self._blobs.put(d) for d in datas]
            for (pid, real, _), ref in zip(batch, refs):
                resolved[pid] = ref
                self._cache[ref] = real  # cache with resolved children for reads
                remaining.discard(pid)
        return resolved[root]

    def _reset_buffer(self) -> None:
        for pid in self._pending:
            self._cache.pop(pid, None)
        self._pending.clear()
        self._buffering = False
        self._pn = 0

    # -- operations (functional: take a root ref, return a new root ref) ----

    def get(self, root: Optional[Ref], key: bytes) -> Optional[Ref]:
        while root is not None:
            node = self._load(root)
            if not key.startswith(node.prefix):
                return None
            key = key[len(node.prefix):]
            if key == b"":
                return node.value_ref
            root = node.children.get(key[0])
            key = key[1:]
        return None

    # -- diff (structural, prunes shared subtrees) --------------------------

    def _node_or_none(self, ref: Optional[Ref]) -> Optional[_Node]:
        return self._load(ref) if ref is not None else None

    def _node_items(self, node: _Node, acc: bytes):
        """(key, value_ref) for every value in the subtree rooted at `node`
        (which may be synthetic, i.e. not itself stored)."""
        stack = [(node, acc)]
        while stack:
            n, a = stack.pop()
            full = a + n.prefix
            if n.value_ref is not None:
                yield (full, n.value_ref)
            for byte, cref in n.children.items():
                stack.append((self._load(cref), full + bytes([byte])))

    def _diff(self, a_root: Optional[Ref], b_root: Optional[Ref]):
        """Yield (key, a_value_ref|None, b_value_ref|None) for every key where
        `a_root` and `b_root` differ. Subtrees with equal refs are pruned, so
        the cost is proportional to the difference, not the dataset."""
        if a_root == b_root:
            return
        yield from self._diff_nodes(
            self._node_or_none(a_root), self._node_or_none(b_root), b"")

    def _diff_nodes(self, a: Optional[_Node], b: Optional[_Node], acc: bytes):
        if a is None:
            if b is not None:
                for k, v in self._node_items(b, acc):
                    yield (k, None, v)
            return
        if b is None:
            for k, v in self._node_items(a, acc):
                yield (k, v, None)
            return

        pa, pb = a.prefix, b.prefix
        if pa == pb:
            ka = acc + pa
            if a.value_ref != b.value_ref:
                yield (ka, a.value_ref, b.value_ref)
            for byte in set(a.children) | set(b.children):
                ca, cb = a.children.get(byte), b.children.get(byte)
                if ca == cb:
                    continue  # shared subtree
                yield from self._diff_nodes(
                    self._node_or_none(ca), self._node_or_none(cb),
                    ka + bytes([byte]))
            return

        common = _common_prefix(pa, pb)
        if len(common) < len(pa) and len(common) < len(pb):
            # prefixes diverge => the two subtrees cover disjoint keys
            for k, v in self._node_items(a, acc):
                yield (k, v, None)
            for k, v in self._node_items(b, acc):
                yield (k, None, v)
        elif len(common) == len(pa):
            # a's key is a proper prefix of b's: b lives under one of a's branches
            ka = acc + pa
            bb = pb[len(pa)]
            b_split = _Node(pb[len(pa) + 1:], b.value_ref, b.children)
            if a.value_ref is not None:
                yield (ka, a.value_ref, None)  # no key at ka on b's side
            for byte, cref in a.children.items():
                if byte == bb:
                    yield from self._diff_nodes(
                        self._load(cref), b_split, ka + bytes([byte]))
                else:
                    for k, v in self._node_items(self._load(cref), ka + bytes([byte])):
                        yield (k, v, None)
            if bb not in a.children:
                for k, v in self._node_items(b_split, ka + bytes([bb])):
                    yield (k, None, v)
        else:
            # symmetric: b's key is a proper prefix of a's
            kb = acc + pb
            ab = pa[len(pb)]
            a_split = _Node(pa[len(pb) + 1:], a.value_ref, a.children)
            if b.value_ref is not None:
                yield (kb, None, b.value_ref)
            for byte, cref in b.children.items():
                if byte == ab:
                    yield from self._diff_nodes(
                        a_split, self._load(cref), kb + bytes([byte]))
                else:
                    for k, v in self._node_items(self._load(cref), kb + bytes([byte])):
                        yield (k, None, v)
            if ab not in b.children:
                for k, v in self._node_items(a_split, kb + bytes([ab])):
                    yield (k, v, None)

    def insert(self, root: Optional[Ref], key: bytes, value_ref: Ref) -> Ref:
        if root is None:
            return self._store(_Node(key, value_ref, {}))
        node = self._load(root)
        common = _common_prefix(node.prefix, key)

        if len(common) < len(node.prefix):
            # split: demote the existing node under the diverging byte
            demoted = _Node(node.prefix[len(common) + 1:], node.value_ref,
                            dict(node.children))
            children = {node.prefix[len(common)]: self._store(demoted)}
            rest = key[len(common):]
            if rest == b"":
                return self._store(_Node(common, value_ref, children))
            leaf = self._store(_Node(rest[1:], value_ref, {}))
            children[rest[0]] = leaf
            return self._store(_Node(common, None, children))

        rest = key[len(node.prefix):]
        if rest == b"":
            return self._store(_Node(node.prefix, value_ref, dict(node.children)))
        children = dict(node.children)
        child_ref = children.get(rest[0])
        if child_ref is None:
            children[rest[0]] = self._store(_Node(rest[1:], value_ref, {}))
        else:
            children[rest[0]] = self.insert(child_ref, rest[1:], value_ref)
        return self._store(_Node(node.prefix, node.value_ref, children))

    def delete(self, root: Optional[Ref], key: bytes) -> Optional[Ref]:
        if root is None:
            raise KeyError(key)
        node = self._load(root)
        if not key.startswith(node.prefix):
            raise KeyError(key)
        rest = key[len(node.prefix):]

        if rest == b"":
            if node.value_ref is None:
                raise KeyError(key)
            return self._canonicalize(node.prefix, None, dict(node.children))

        child_ref = node.children.get(rest[0])
        if child_ref is None:
            raise KeyError(key)
        new_child = self.delete(child_ref, rest[1:])
        children = dict(node.children)
        if new_child is None:
            del children[rest[0]]
        else:
            children[rest[0]] = new_child
        return self._canonicalize(node.prefix, node.value_ref, children)

    def _canonicalize(self, prefix: bytes, value_ref: Optional[Ref],
                      children: Dict[int, Ref]) -> Optional[Ref]:
        """Restore canonical-form invariants after a removal."""
        if value_ref is None and not children:
            return None
        if value_ref is None and len(children) == 1:
            (byte, child_ref), = children.items()
            child = self._load(child_ref)
            merged = _Node(prefix + bytes([byte]) + child.prefix,
                           child.value_ref, dict(child.children))
            return self._store(merged)
        return self._store(_Node(prefix, value_ref, children))

    def items(self, root: Optional[Ref],
              prefix: bytes = b"") -> Iterator[Tuple[bytes, Ref]]:
        """All (key, value_ref) with key under `prefix`, in sorted key order."""
        if root is None:
            return
        # Sorted pre-order DFS: a node's own key precedes its descendants',
        # children visited in byte order, so keys come out sorted with no final
        # sort and no result-set-sized buffer. Each node's children are
        # prefetched in one batch so a network store still parallelises sibling
        # loads (children pop from the stack as cache hits).
        self._load_many([root])  # route the root through the batch path too
        stack = [(root, b"")]
        while stack:
            ref, acc = stack.pop()
            node = self._load(ref)
            full = acc + node.prefix
            # prune subtrees that cannot contain the prefix
            probe = min(len(full), len(prefix))
            if full[:probe] != prefix[:probe]:
                continue
            if node.value_ref is not None and full.startswith(prefix):
                yield (full, node.value_ref)
            child_bytes = sorted(node.children)
            if child_bytes:
                self._load_many([node.children[b] for b in child_bytes])
                for byte in reversed(child_bytes):  # reverse: smallest pops first
                    stack.append((node.children[byte], full + bytes([byte])))


# ---------------------------------------------------------------------------
# Proofs: verifiable inclusion and absence against a root
#
# The trie is canonically encoded, so a key has exactly ONE possible location
# under a given root — which is what makes *absence* provable (exhibit the
# path where the key would live and show the walk dies there), not just
# inclusion. A proof is a self-describing JSON-ready dict carrying the RAW
# node blobs (hex); verification is hash-chain recomputation over those
# exact bytes — never re-serialization — so it needs no bytes store, no
# network, and no trust in the prover: just the root reference and the
# addressing scheme named in the envelope. New proof formats version by
# *name* (readers ignore formats they don't know); the layout below is
# format "recordstore-trie-proof", version 1.
# ---------------------------------------------------------------------------

PROOF_FORMAT = "recordstore-trie-proof"


class ProofError(Exception):
    """The proof does not verify against the given root."""


def _addressing_name(blobs) -> str:
    """The addressing-scheme *name* a proof envelope can carry (it must be
    resolvable by any verifier, so callables have no place in it)."""
    if isinstance(blobs, MemoryBytesStore):
        return "sha256"
    if isinstance(blobs, BeeBytesStore):
        return "swarm"
    ref_of = getattr(blobs, "_ref_of", None)
    for name, fn in _ADDRESSING.items():
        if ref_of is fn:
            return name
    raise ValueError(
        "cannot determine this bytes store's addressing scheme; pass "
        "prove(key, addressing='sha256'|'swarm') explicitly")


def verify_proof(proof, root: Optional[Ref]):
    """Check `proof` against `root` (the reference the *verifier* trusts —
    None for an empty store) and return the proven record for an inclusion
    proof, or the ``ABSENT`` sentinel for an absence proof. Raises
    ``ProofError`` on any mismatch. Pure: reads no store, replays the walk
    over the raw bytes carried in the envelope.
    """
    if not isinstance(proof, dict) or proof.get("format") != PROOF_FORMAT:
        raise ProofError(f"not a {PROOF_FORMAT} envelope")
    if proof.get("version") != 1:
        raise ProofError(f"unsupported proof version {proof.get('version')!r}")
    if proof.get("root") != root:
        raise ProofError(
            f"proof is about root {proof.get('root')!r}, not {root!r}")
    ref_of = _resolve_addressing(proof.get("addressing"))
    key = proof.get("key")
    if not isinstance(key, str) or key == "":
        raise ProofError("proof carries no key")
    try:
        nodes = [bytes.fromhex(blob) for blob in proof.get("nodes", [])]
    except (TypeError, ValueError):
        raise ProofError("malformed node bytes in proof") from None

    expected = root
    remaining = key.encode("utf-8")
    verdict = None  # (found, value_ref) once the walk concludes
    for i, blob in enumerate(nodes):
        if verdict is not None:
            raise ProofError(f"node {i} continues past the walk's conclusion")
        if ref_of(blob) != expected:
            raise ProofError(
                f"node {i} does not hash to the expected reference")
        try:
            node = _Trie._decode(blob)
        except (ValueError, KeyError):
            raise ProofError(f"node {i} is not a valid trie node") from None
        if not remaining.startswith(node.prefix):
            verdict = (False, None)          # diverges inside the prefix
        else:
            remaining = remaining[len(node.prefix):]
            if remaining == b"":
                verdict = (node.value_ref is not None, node.value_ref)
            else:
                child = node.children.get(remaining[0])
                if child is None:
                    verdict = (False, None)  # nowhere to descend
                else:
                    expected, remaining = child, remaining[1:]
    if verdict is None:
        if root is None:
            verdict = (False, None)          # the empty store holds nothing
        else:
            raise ProofError("proof ends before the walk does")

    found, value_ref = verdict
    if found != bool(proof.get("present")):
        raise ProofError("the proof's claim contradicts its own path")
    if not found:
        if proof.get("value") is not None:
            raise ProofError("absence proof carries a value")
        return ABSENT
    try:
        value_blob = bytes.fromhex(proof["value"])
    except (TypeError, ValueError, KeyError):
        raise ProofError("malformed value bytes in proof") from None
    if ref_of(value_blob) != value_ref:
        raise ProofError(
            "value bytes do not hash to the trie's value reference")
    return _decode_value(value_blob)


# ---------------------------------------------------------------------------
# Pointers ("latest root")
# ---------------------------------------------------------------------------

class Pointer(Protocol):
    def get(self) -> Optional[Ref]: ...
    def set(self, root: Ref) -> None: ...


class MemoryPointer:
    def __init__(self, root: Optional[Ref] = None):
        self._root = root

    def get(self) -> Optional[Ref]:
        return self._root

    def set(self, root: Ref) -> None:
        self._root = root

    def compare_and_set(self, expected: Optional[Ref], new: Optional[Ref]) -> bool:
        """Atomic in-process compare-and-set — lets reconciling commits over a
        shared in-process pointer converge without a lost-update race."""
        if self._root == expected:
            self._root = new
            return True
        return False


class FilePointer:
    """Local-file pointer, useful during development."""

    def __init__(self, path: str):
        self.path = path

    def get(self) -> Optional[Ref]:
        try:
            with open(self.path) as f:
                content = f.read().strip()
                return content or None
        except FileNotFoundError:
            return None

    def set(self, root: Ref) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            f.write(root)
        os.replace(tmp, self.path)  # atomic on POSIX


class SwarmFeedPointer:
    """Mutable "latest root" backed by a Swarm feed.

    A Swarm feed is an owner-signed, mutable pointer: each update is a
    single-owner chunk (SOC), BMT-hashed and secp256k1-signed with the feed
    owner's key, posted to Bee's ``/soc/{owner}/{id}`` endpoint; readers
    resolve "the latest" by sequence-index lookup at
    ``GET /feeds/{owner}/{topic}``. This maps a feed onto the `Pointer`
    protocol: ``set(root)`` publishes a new signed update, ``get()`` resolves
    the latest root.

    Requires the ``swarm-bee`` package (``pip install "recordstore[feeds]"``),
    which performs the SOC/secp256k1 signing correctly — independently verified
    against a live Bee 2.8.1 node (2026-07). It is imported lazily, so the
    recordstore core stays stdlib-only.

    Reliability. Swarm feed *lookups* are unreliable per call on a light node,
    especially over a high-latency link: a lookup can 404 ("lookup failed";
    ~10/12 calls in one hotspot measurement) or return a *stale-early* index
    instead of the latest (ethersphere/bee#5251). The SOC *writes* are fine and
    the chunks are individually retrievable; it is the lookup — which fetches
    candidate index chunks from the network — that flakes. So this class never
    trusts a single lookup:

    - **Read-your-writes cache.** After ``set(ref)``, ``ref`` is served from a
      local cache for ``feed_ttl`` seconds with no network round-trip, so a
      writer never waits on a flaky lookup to see its own commit.
    - **Monotonic index floor.** The next write index is
      ``max(network_next, local_floor)``. Without the floor, back-to-back
      commits would reuse an index while the first SOC is still propagating,
      and the second update would be silently dropped.
    - **Reliable index discovery.** The tip index is found by probing the feed's
      SOC chunks directly (exponential + binary search) rather than trusting the
      flaky /feeds lookup — the SOC chunks are individually retrievable even when
      the lookup 404s. This is what makes a *cold* read (no cached index to hint
      from) reliable. The warm path still tries the cheaper ``after``-hinted
      lookup first and only falls back to probing when it flakes.
    - **Retry-until-stable reads.** ``get()`` retries with exponential backoff on
      transient chunk-fetch errors and never adopts a result whose index
      regresses below what it has already seen (the stale-early guard).

    This policy follows swarmfs's ``bzzf://`` feed layer, the reference
    implementation for this Swarm characteristic. Once a feed has been resolved
    at least once, ``get()`` also passes Bee's ``after`` index hint
    (``GET /feeds/...?after=N``) so the lookup resumes just below the last
    confirmed index instead of probing from scratch — much cheaper and less
    flaky as the feed grows. swarm-bee's typed API does not expose ``after``
    (see bee-py#2), so it is sent through the client transport directly, and
    falls back to the plain lookup when that transport is unavailable or when
    there is no confirmed index yet to resume from. This is a Swarm/light-node
    characteristic, not a swarm-bee defect — any client hits it identically.

    Construction. Pass a ``signer`` (32-byte secp256k1 private key, hex) to read
    *and* write; the owner address is derived from it. For a read-only pointer,
    pass ``owner`` (20-byte address, hex) instead. Writing also needs
    ``postage_batch_id``. ``topic`` is a namespace string, hashed to the 32-byte
    feed topic.
    """

    def __init__(
        self,
        api_url: str,
        topic: str,
        *,
        signer: Optional[str] = None,
        owner: Optional[str] = None,
        postage_batch_id: Optional[str] = None,
        feed_ttl: float = 15.0,
        max_lookup_retries: int = 15,
        retry_backoff: float = 0.5,
        retry_backoff_cap: float = 5.0,
    ):
        try:
            from bee import Bee
            from bee.feeds import make_feed_identifier
            from bee.swarm.keys import PrivateKey
            from bee.swarm.typed_bytes import BatchId, EthAddress, Reference, Topic
            from bee.swarm.errors import BeeResponseError
        except ImportError as e:  # pragma: no cover - only without the extra
            raise ImportError(
                "SwarmFeedPointer requires the 'swarm-bee' package; install it "
                'with: pip install "recordstore[feeds]"'
            ) from e

        self._Reference = Reference
        self._make_feed_identifier = make_feed_identifier
        self._BeeResponseError = BeeResponseError
        self._bee = Bee(api_url)
        self._topic = Topic.from_string(topic)

        self._signer = PrivateKey.from_hex(signer) if signer else None
        if self._signer is not None:
            self._owner = self._signer.public_key().address()
        elif owner is not None:
            self._owner = EthAddress.from_hex(owner)
        else:
            raise ValueError(
                "SwarmFeedPointer needs a signer (to read and write) or an "
                "owner address (read-only)"
            )
        self._batch = BatchId.from_hex(postage_batch_id) if postage_batch_id else None

        # Bee honours GET /feeds/...?after=N (resume a lookup from a known
        # index); swarm-bee's typed API can't pass it, so hint via the client
        # transport when present (bee-py#2), falling back cleanly otherwise.
        self._can_hint = hasattr(getattr(self._bee.feeds, "_inner", None), "send")

        self._ttl = feed_ttl
        self._max_retries = max(1, max_lookup_retries)
        self._backoff = retry_backoff
        self._backoff_cap = retry_backoff_cap

        # read-your-writes cache + monotonic index floor
        self._cached_ref: Optional[Ref] = None
        self._next_index = 0
        self._cache_expiry = 0.0

    def set(self, root: Ref) -> None:
        if self._signer is None or self._batch is None:
            raise RuntimeError(
                "SwarmFeedPointer.set requires both a signer and a "
                "postage_batch_id"
            )
        # A persistent writer's floor is authoritative (single-writer model);
        # only a cold instance has to discover where the feed currently ends,
        # and it does so by probing SOC chunks — reliable even when the /feeds
        # lookup flakes on a high-latency link.
        if self._next_index > 0:
            index = self._next_index
        else:
            probed = self._probe_latest_index()
            index = probed + 1 if probed is not None else 0
        self._bee.feeds.update_feed_with_reference(
            batch_id=self._batch,
            signer=self._signer,
            topic=self._topic,
            reference=self._Reference.from_hex(root),
            index=index,
        )
        self._cached_ref = root
        self._next_index = index + 1
        self._cache_expiry = time.monotonic() + self._ttl

    def get(self) -> Optional[Ref]:
        if self._cached_ref is not None and time.monotonic() < self._cache_expiry:
            return self._cached_ref  # read-your-writes / fresh cache

        delay = self._backoff
        for attempt in range(self._max_retries):
            try:
                latest_index = self._resolve_latest_index()
                if latest_index is None:
                    return self._cached_ref  # feed is empty (definitive)
                index_next = latest_index + 1
                if index_next > self._next_index or self._cached_ref is None:
                    # A newer update (or we've never resolved): read the
                    # reference from the feed's single-owner chunk — NOT from a
                    # plain feed GET, which Bee dereferences to the pointed-to
                    # content rather than returning the reference.
                    identifier = self._make_feed_identifier(self._topic, latest_index)
                    soc = self._bee.file.download_soc(self._owner, identifier)
                    self._cached_ref = self._soc_reference(soc)
                    self._next_index = index_next
                    self._cache_expiry = time.monotonic() + self._ttl
                    return self._cached_ref
                if index_next == self._next_index:
                    # confirmed unchanged; serve cache and refresh the TTL.
                    self._cache_expiry = time.monotonic() + self._ttl
                    return self._cached_ref
                # index_next < floor: stale-early lookup; retry for a fresher one.
            except self._BeeResponseError as e:
                if getattr(e, "status", None) not in (404, 500):
                    raise
                # transient flake or empty feed; fall through to backoff/retry.
            if attempt < self._max_retries - 1:
                time.sleep(delay)
                delay = min(delay * 2, self._backoff_cap)
        return self._cached_ref  # last-known ref, or None if never resolved

    def _get_fresh(self) -> Optional[Ref]:
        """Resolve the latest root from the network, bypassing the
        read-your-writes/TTL cache — used by `compare_and_set` so the check
        reflects other writers, not our own cached value."""
        self._cache_expiry = 0.0
        return self.get()

    def compare_and_set(self, expected: Optional[Ref], new: Ref) -> bool:
        """Best-effort compare-and-set for reconciling commits. Returns True
        only if the feed still resolved to `expected` and `new` was written and
        read back as the latest.

        Caveat: a Swarm feed has no atomic index claim — Bee accepts (and
        overwrites with) a second update at an already-used index. So this
        cannot be a true CAS. It reads the current head *fresh* (so it reliably
        detects a feed that already advanced — the common case) and verifies its
        own write read-back, which resolves most races; but two writers hitting
        the exact same index simultaneously can still both believe they won.
        This narrows the window rather than closing it (see the limitations in
        the user guide)."""
        if self._get_fresh() != expected:
            return False
        self.set(new)
        return self._get_fresh() == new

    def _resolve_latest_index(self) -> Optional[int]:
        """Latest feed index, or ``None`` for an empty feed.

        Warm path: resume the (flaky) /feeds lookup near the tip via Bee's
        ``after`` hint — one round trip when it works. Cold path, or when the
        hinted lookup flakes: probe the feed's SOC chunks directly, which are
        individually retrievable even when the /feeds lookup does not resolve.
        Raises ``BeeResponseError`` only on transient chunk-fetch errors, which
        the retry loop in ``get`` absorbs."""
        hint = self._next_index - 2  # one below our last-confirmed index
        if self._can_hint and hint >= 1:
            try:
                resp = self._bee.feeds._inner.send(
                    "GET",
                    f"feeds/{self._owner.to_hex()}/{self._topic.to_hex()}",
                    params={"after": str(hint)},
                    headers=[("Swarm-Only-Root-Chunk", "true")],
                )
                idx_hex = resp.headers.get("swarm-feed-index")
                if idx_hex is not None:
                    return int(idx_hex, 16)
            except self._BeeResponseError as e:
                if getattr(e, "status", None) not in (404, 500):
                    raise
                # hinted lookup flaked; fall through to the reliable probe.
        return self._probe_latest_index()

    def _probe_latest_index(self) -> Optional[int]:
        """Highest existing feed index (``None`` if the feed is empty), found by
        probing single-owner-chunk addresses. Sequential feeds have no gaps, so
        an exponential + binary search over SOC existence pins the tip in
        O(log n) reliable chunk fetches — no /feeds lookup involved."""
        if not self._soc_exists(0):
            return None
        lo, hi = 0, 1
        while self._soc_exists(hi):
            lo, hi = hi, hi * 2
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if self._soc_exists(mid):
                lo = mid
            else:
                hi = mid
        return lo

    def _soc_exists(self, index: int) -> bool:
        identifier = self._make_feed_identifier(self._topic, index)
        try:
            self._bee.file.download_soc(self._owner, identifier)
            return True
        except self._BeeResponseError as e:
            if getattr(e, "status", None) == 404:
                return False
            raise  # transient (e.g. 500): let the caller retry

    @staticmethod
    def _soc_reference(soc) -> Ref:
        # SOC payload is timestamp(8 BE) || reference; strip the timestamp.
        payload = soc.payload
        payload = payload.as_bytes() if hasattr(payload, "as_bytes") else bytes(payload)
        return payload[8:].hex()


# ---------------------------------------------------------------------------
# RecordStore
# ---------------------------------------------------------------------------

def swarm_store(
    topic: str,
    *,
    api_url: str = "http://localhost:1633",
    stamp: str = "auto",
    signer: Optional[str] = None,
    owner: Optional[str] = None,
    feed_ttl: float = 15.0,
    deferred_upload: bool = True,
    max_concurrent_reads: int = 16,
) -> "RecordStore":
    """A `RecordStore` that lives entirely on Ethereum Swarm.

    This is the one place in the stack where Swarm is chosen: blobs go to a
    Bee node (`BeeBytesStore`) and the mutable "latest root" is a Swarm feed
    (`SwarmFeedPointer`), so a published store has a *stable address* rather
    than a root hash you have to pass around by hand. Everything above —
    `RecordStore` itself and its consumers — stays backend-neutral.

        store = swarm_store("my-notes", signer=key)   # publish your own
        store = swarm_store("my-notes", owner=addr)   # follow someone else's

    Pass `signer` (32-byte secp256k1 private key, hex) to read *and* write;
    the owner address is derived from it. Pass `owner` instead for a
    read-only view of somebody else's feed. Writes need a postage batch:
    `stamp="auto"` picks a usable one (see `_auto_batch`).

    Needs the `bee` and `feeds` extras:
    `pip install "recordstore[bee,feeds]"`.
    """
    if signer is None and owner is None:
        raise ValueError(
            "swarm_store needs either signer=<private key hex> (to publish) "
            "or owner=<address hex> (to follow someone else's feed)"
        )
    blobs = BeeBytesStore(
        api_url,
        stamp,
        deferred_upload=deferred_upload,
        max_concurrent_reads=max_concurrent_reads,
    )
    pointer = SwarmFeedPointer(
        api_url,
        topic,
        signer=signer,
        owner=owner,
        # the batch was already resolved (possibly from "auto") by the store,
        # so the feed's SOC writes and the blob writes share one batch
        postage_batch_id=blobs.batch,
        feed_ttl=feed_ttl,
    )
    return RecordStore(blobs, pointer=pointer)


class RecordStore:
    """Staged, versioned key->record store over a BytesStore.

    Reads are read-your-writes (staged changes shadow the committed trie).
    `commit()` flushes staged changes and returns the new root reference;
    `RecordStore.at(root, bytes_store)` opens a read-only snapshot of any root.
    Returned records are deep copies: mutating them never mutates the store.
    """

    def __init__(self, bytes_store: BytesStore, root: Optional[Ref] = None,
                 pointer: Optional[Pointer] = None, _readonly: bool = False,
                 node_cache_size: int = DEFAULT_NODE_CACHE_SIZE):
        self._blobs = bytes_store
        self._trie = _Trie(bytes_store, cache_size=node_cache_size)
        self._root = pointer.get() if (pointer and root is None) else root
        self._pointer = pointer
        self._staged: Dict[str, object] = {}
        self._readonly = _readonly

    # -- snapshots -----------------------------------------------------------

    @classmethod
    def at(cls, root: Optional[Ref], bytes_store: BytesStore) -> "RecordStore":
        return cls(bytes_store, root=root, _readonly=True)

    @property
    def root(self) -> Optional[Ref]:
        """Root of the last committed state (staged changes not included)."""
        return self._root

    @property
    def blobs(self) -> BytesStore:
        """The underlying bytes store — so a caller holding one store can open
        *another* root over the same blobs (`RecordStore.at(other, s.blobs)`)
        without having kept a separate reference to the backend."""
        return self._blobs

    # -- record operations -----------------------------------------------------

    @staticmethod
    def _check_key(key: str) -> bytes:
        if not isinstance(key, str) or key == "":
            raise ValueError("key must be a non-empty string")
        return key.encode("utf-8")

    def get(self, key: str):
        kb = self._check_key(key)
        if key in self._staged:
            staged = self._staged[key]
            if staged is _TOMBSTONE:
                raise KeyError(key)
            return json.loads(canonical_bytes(staged))  # deep copy
        vref = self._trie.get(self._root, kb)
        if vref is None:
            raise KeyError(key)
        return _decode_value(self._blobs.get(vref))

    def contains(self, key: str) -> bool:
        try:
            self.get(key)
            return True
        except KeyError:
            return False

    def prove(self, key: str, addressing: Optional[str] = None) -> dict:
        """A verifiable inclusion-or-absence proof for `key` against the
        committed root: the raw trie-node blobs along the key's one possible
        path (canonical encoding makes absence provable), plus the value
        blob when present. The result is a JSON-ready dict that
        ``verify_proof(proof, root)`` checks with no store access.

        Proofs are statements about a committed root, so a key with staged
        changes is refused — commit first. `addressing` names the scheme a
        verifier should recompute references with; it is detected from the
        bytes store when omitted. Every proof is self-verified before being
        returned, so a mismatched addressing scheme (e.g. a Bee node that
        added erasure coding, whose references are not the plain content
        address) fails loudly here rather than silently at the verifier.
        """
        kb = self._check_key(key)
        if key in self._staged:
            raise ValueError(
                f"{key!r} has staged, uncommitted changes — proofs are "
                "statements about a committed root; commit() first")
        name = addressing or _addressing_name(self._blobs)
        nodes: List[str] = []
        present = False
        value_hex = None
        ref, remaining = self._root, kb
        while ref is not None:
            blob = self._blobs.get(ref)   # the exact bytes behind the ref
            nodes.append(blob.hex())
            node = _Trie._decode(blob)
            if not remaining.startswith(node.prefix):
                break
            remaining = remaining[len(node.prefix):]
            if remaining == b"":
                if node.value_ref is not None:
                    present = True
                    value_hex = self._blobs.get(node.value_ref).hex()
                break
            ref = node.children.get(remaining[0])
            remaining = remaining[1:]
        proof = {
            "format": PROOF_FORMAT,
            "version": 1,
            "addressing": name,
            "root": self._root,
            "key": key,
            "present": present,
            "nodes": nodes,
            "value": value_hex,
        }
        verify_proof(proof, self._root)   # never hand out a broken proof
        return proof

    def put(self, key: str, value) -> None:
        if self._readonly:
            raise TypeError("read-only snapshot")
        self._check_key(key)
        # One canonical encode both validates (rejects non-JSON values and
        # NaN/Infinity) and, via the round trip, detaches from the caller's
        # object — no need to encode twice.
        self._staged[key] = json.loads(canonical_bytes(value))

    def delete(self, key: str) -> None:
        if self._readonly:
            raise TypeError("read-only snapshot")
        kb = self._check_key(key)
        if key not in self._staged and self._trie.get(self._root, kb) is None:
            raise KeyError(key)
        self._staged[key] = _TOMBSTONE

    def _merged(self, prefix: str):
        """Lazily yield `(key, vref, staged)` in sorted key order, merging the
        committed trie stream with the staged overlay. For a committed record
        `vref` is set and `staged` is None; for a staged put `vref` is None and
        `staged` is the raw staged value; tombstones are dropped. Both inputs
        are already sorted, so this is a streaming merge — nothing proportional
        to the result set is buffered (only the small staged overlay)."""
        pb = prefix.encode("utf-8")
        committed = self._trie.items(self._root, pb)  # lazy, sorted
        staged = sorted(k for k in self._staged if k.startswith(prefix))
        si, ns = 0, len(staged)
        for kb, vref in committed:
            ck = kb.decode("utf-8")
            while si < ns and staged[si] < ck:
                sk = staged[si]; si += 1
                if self._staged[sk] is not _TOMBSTONE:
                    yield sk, None, self._staged[sk]
            if si < ns and staged[si] == ck:  # staged entry shadows the trie
                if self._staged[ck] is not _TOMBSTONE:
                    yield ck, None, self._staged[ck]
                si += 1
            else:
                yield ck, vref, None
        while si < ns:
            sk = staged[si]; si += 1
            if self._staged[sk] is not _TOMBSTONE:
                yield sk, None, self._staged[sk]

    def keys(self, prefix: str = "") -> Iterator[str]:
        """Sorted keys under `prefix`, staged overlay included, yielded lazily
        (no result-set-sized buffer)."""
        for key, _vref, _staged in self._merged(prefix):
            yield key

    def items(self, prefix: str = ""):
        """Sorted `(key, value)` pairs under `prefix`, staged overlay included.

        Streams in windows: value blobs are fetched a window at a time, so over
        a network store that implements `get_many` the reads parallelise within
        each window (the fast path for hydrating a store) while memory stays
        bounded to one window rather than the whole result set. Values are
        deep-copied, exactly like `get`."""
        window = max(1, getattr(self._blobs, "max_concurrent_reads", 256))
        buf: list = []
        refs: List[Ref] = []
        for key, vref, staged in self._merged(prefix):
            buf.append((key, vref, staged))
            if vref is not None:
                refs.append(vref)
            if len(refs) >= window:
                yield from self._flush_items(buf, refs)
                buf, refs = [], []
        if buf:
            yield from self._flush_items(buf, refs)

    def diff(self, other_root: Optional[Ref]):
        """Yield ``(key, mine, theirs)`` for every key whose value differs
        between this store's committed root and `other_root` — "what
        changed between two published versions?" as a first-class question.

        A side that lacks the key gets the ``ABSENT`` sentinel (a stored
        value can legitimately be ``None``/null, so ``None`` cannot mean
        "missing" — the same convention merge resolvers see). Values are
        decoded fresh, like `get`; keys arrive in no particular order.

        Cost is proportional to the DIFFERENCE, not the dataset: the walk
        is the same structural trie diff `merge` uses, pruning every
        subtree whose refs are equal — which canonical roots guarantee for
        equal content, so diffing a store against itself reads nothing.
        Staged, uncommitted changes are part of no root and therefore of
        no diff; commit first. To compare two arbitrary published roots,
        open one as a snapshot: ``RecordStore.at(a, blobs).diff(b)``.
        """
        for kb, mine_ref, theirs_ref in self._trie._diff(self._root,
                                                         other_root):
            yield (
                kb.decode("utf-8"),
                ABSENT if mine_ref is None
                else _decode_value(self._blobs.get(mine_ref)),
                ABSENT if theirs_ref is None
                else _decode_value(self._blobs.get(theirs_ref)),
            )

    def _flush_items(self, buf, refs: List[Ref]):
        blobs = self._fetch_blobs(refs) if refs else {}
        for key, vref, staged in buf:
            if vref is None:
                yield key, json.loads(canonical_bytes(staged))  # deep copy
            else:
                yield key, _decode_value(blobs[vref])

    def _fetch_blobs(self, refs: List[Ref]) -> Dict[Ref, bytes]:
        get_many = getattr(self._blobs, "get_many", None)
        if get_many is not None:
            return get_many(refs)
        return {r: self._blobs.get(r) for r in refs}

    # -- commit ---------------------------------------------------------------

    def _build_root(self, base: Optional[Ref]) -> Optional[Ref]:
        """Apply the staged changes on top of `base` and return the new root.
        Value blobs go up front (concurrently if supported); trie nodes are
        buffered and flushed bottom-up, one batch per level."""
        writes = [(k, self._staged[k]) for k in sorted(self._staged)
                  if self._staged[k] is not _TOMBSTONE]
        put_many = getattr(self._blobs, "put_many", None)
        datas = [_encode_value(v) for _, v in writes]
        refs = put_many(datas) if put_many is not None else [self._blobs.put(d) for d in datas]
        vref = {k: r for (k, _), r in zip(writes, refs)}

        self._trie._buffering = True
        try:
            root = base
            for key in sorted(self._staged):  # deterministic write order
                staged = self._staged[key]
                kb = key.encode("utf-8")
                if staged is _TOMBSTONE:
                    try:
                        root = self._trie.delete(root, kb)
                    except KeyError:
                        pass  # deleted a key that never existed in the trie
                else:
                    root = self._trie.insert(root, kb, vref[key])
            return self._trie._flush(root)
        finally:
            self._trie._reset_buffer()

    def commit(self, *, reconcile: bool = False, resolver=None,
               retries: int = 5) -> Optional[Ref]:
        """Flush staged changes; return the new root and update the pointer.

        The root/pointer changes only after every blob write has succeeded, so
        a reader following the pointer sees all of a commit or none of it.

        With `reconcile=True` and a pointer attached, the commit converges with
        concurrent writers instead of overwriting them: if the pointer has moved
        past the root this commit built on, the two versions are three-way
        merged (see `merge`; `resolver` settles conflicts) and the merge is
        retried up to `retries` times until the pointer lands. A pointer that
        exposes `compare_and_set` gets race-free updates; otherwise the
        read-then-set is best-effort (there is no lower-level CAS)."""
        if self._readonly:
            raise TypeError("read-only snapshot")
        base = self._root
        new = self._build_root(base)
        if self._pointer is not None:
            if reconcile:
                new = self._reconcile(base, new, resolver, retries)
            else:
                self._pointer.set(new)
        self._staged.clear()
        self._root = new
        return new

    def _reconcile(self, base, new, resolver, retries):
        pointer = self._pointer
        cas = getattr(pointer, "compare_and_set", None)
        expected = base
        for _ in range(max(1, retries)):
            current = pointer.get()
            if current == expected:
                if cas is None:
                    pointer.set(new)  # best-effort (no CAS at this layer)
                    return new
                if cas(expected, new):
                    return new
                continue  # lost the race; re-read and retry
            # pointer advanced under us: fold their version into ours
            new = self.merge(self._blobs, expected, new, current, resolver)
            expected = current
        raise RuntimeError(
            f"commit could not reconcile the pointer after {retries} tries")

    # -- merge ----------------------------------------------------------------

    @classmethod
    def merge(cls, bytes_store: BytesStore, base: Optional[Ref],
              ours: Optional[Ref], theirs: Optional[Ref], resolver=None) -> Optional[Ref]:
        """Three-way merge of two roots that diverged from a common `base`.

        Returns the merged root. Because roots are canonical, this leans on
        reference equality: if a subtree is unchanged on a side its root ref
        still equals `base`'s, so whole branches merge for free.

        Per key: a change made on only one side is taken; a change made on both
        sides to the *same* value is taken once; a change made on both sides to
        *different* values is a conflict. Conflicts are settled by
        ``resolver(key, base, ours, theirs)`` — each argument is the decoded
        value or the ``ABSENT`` sentinel; return the value to keep, or the
        ``DELETE`` sentinel to drop the key. Without a resolver, conflicts raise
        `MergeConflict`. The merge is commutative iff the resolver is symmetric
        in its ours/theirs arguments (the built-in conflict = raise is).

        Only the changed keys are touched: both the read (a structural diff that
        prunes subtrees equal on both sides) and the write (the merged diff
        applied to `base`, bulk-flushed) are proportional to the divergence, not
        the dataset. Unchanged subtrees are shared with `base`.
        """
        if ours == theirs:
            return ours                       # identical (incl. both None)
        if ours == base:
            return theirs                     # only they changed
        if theirs == base:
            return ours                       # only we changed

        trie = _Trie(bytes_store)
        # (base_ref, side_ref) per key that changed from base on each side.
        our_diff = {k: (bv, sv) for k, bv, sv in trie._diff(base, ours)}
        their_diff = {k: (bv, sv) for k, bv, sv in trie._diff(base, theirs)}

        changes: Dict[bytes, object] = {}     # key -> value_ref | _TOMBSTONE
        conflicts: List[str] = []
        for k in set(our_diff) | set(their_diff):
            if k in our_diff and k in their_diff:
                bv, ov = our_diff[k]
                tv = their_diff[k][1]
                if ov == tv:
                    merged = ov               # both changed it the same way
                elif resolver is None:
                    conflicts.append(k.decode("utf-8"))
                    continue
                else:
                    decode = (lambda r: _decode_value(bytes_store.get(r))
                              if r is not None else ABSENT)
                    res = resolver(k.decode("utf-8"), decode(bv), decode(ov), decode(tv))
                    merged = None if res is DELETE else bytes_store.put(_encode_value(res))
            elif k in our_diff:
                bv, merged = our_diff[k]       # changed by us only
            else:
                bv, merged = their_diff[k]     # changed by them only
            if merged != bv:                   # (a resolver could land back on base)
                changes[k] = merged if merged is not None else _TOMBSTONE

        if conflicts:
            raise MergeConflict(conflicts)

        # Apply only the diff to base (O(diff) node writes, bulk-flushed).
        root = base
        trie._buffering = True
        try:
            for k in sorted(changes):
                c = changes[k]
                if c is _TOMBSTONE:
                    try:
                        root = trie.delete(root, k)
                    except KeyError:
                        pass
                else:
                    root = trie.insert(root, k, c)
            root = trie._flush(root)
        finally:
            trie._reset_buffer()
        return root
