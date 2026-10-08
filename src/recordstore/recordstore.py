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
from typing import (Dict, Iterable, Iterator, List, NamedTuple, Optional,
                    Protocol, Tuple)

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


class RecordUnavailable(Exception):
    """The key exists under the committed root, but its value bytes are
    unreachable right now — evicted from a local-first store with no
    network to heal from, or missing from the backend. Deliberately NOT a
    ``KeyError``: ``except KeyError`` (and ``contains``) must never
    misread "temporarily unreachable" as "absent". The record comes back
    when the store can reach Swarm again."""


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
    """``(client, StampManager)`` from swarmfs, imported lazily."""
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
    StampManager (an optional dependency, imported lazily).

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

    The HTTP side is swarmfs's client (since 0.22; it was `requests`), so
    recordstore reaches Bee one way: `get_many`/`put_many` keep
    `max_concurrent_reads` requests in flight on swarmfs's event loop over
    one pooled keep-alive session.
    """

    def __init__(self, api_url: str, postage_batch_id: str = "auto",
                 deferred_upload: bool = True, max_concurrent_reads: int = 32,
                 min_batch_ttl: int = AUTO_MIN_BATCH_TTL):
        try:  # lazy: only needed for the real backend
            from fsspec.asyn import sync
            from swarmfs import SwarmClient, SyncSwarmClient
        except ImportError as e:  # pragma: no cover - only without the extra
            raise ImportError(
                "BeeBytesStore talks to Bee through swarmfs; install it with: "
                'pip install "recordstore[bee]"') from e
        self.api_url = api_url.rstrip("/")
        if postage_batch_id in (None, "auto"):
            postage_batch_id = _auto_batch(self.api_url, min_batch_ttl)
        self.batch = postage_batch_id
        self.deferred = deferred_upload
        # 32 in flight (16 was a guess): measured against a Bee 2.8.2 light
        # node, reads of chunks it had to fetch scaled almost linearly to 32
        # (about 4/s per request in flight, ~270 ms apiece) and were noisy
        # beyond. The best number depends on the node, so it stays a knob
        # (User Guide, "Concurrency tuning across a real link").
        self.max_concurrent_reads = max(1, max_concurrent_reads)
        self._async = SwarmClient(self.api_url)
        self._client = SyncSwarmClient(client=self._async)
        self._run = lambda fn, *a: sync(self._client.loop, fn, *a)

    def batch_status(self, *, buckets: bool = False):
        """This store's postage batch health — ``(StampInfo, BucketStats |
        None)``. Cron this to learn that the batch needs renewing while
        renewal is still possible; see :func:`batch_status`."""
        return batch_status(self.api_url, self.batch, buckets=buckets)

    def _refused(self, e: Exception) -> RuntimeError:
        """A 402 means one of two very different things, and only one is
        recoverable, so say which. Nothing already stored is lost either
        way."""
        if "overissued" in str(e) or "full in at least one bucket" in str(e):
            return RuntimeError(
                f"postage batch {self.batch[:8]}… refused this chunk: a "
                "bucket is full. Nothing already stored is lost. Dilute the "
                "batch one depth to double every bucket's capacity and retry "
                "(swarmfs: StampManager.dilute, or 'swarmlite stamps dilute "
                "<id> --depth N'), then top up — dilution halves the "
                "remaining validity.")
        return RuntimeError(
            f"the node did not accept postage batch {self.batch[:8]}… "
            f"Check it with GET /stamps/{self.batch}; if it expired, a new "
            "batch is the only option — expired batches cannot be revived.")

    def put(self, data: bytes) -> Ref:
        from swarmfs.exceptions import StampError
        try:
            return self._client.bytes_post(data, self.batch,
                                           deferred=self.deferred)
        except StampError as e:
            raise self._refused(e) from e

    def get(self, ref: Ref) -> bytes:
        try:
            return self._client.bytes_get(ref)
        except FileNotFoundError:
            raise KeyError(f"reference not found: {ref}") from None

    async def _many(self, fn, items: list) -> list:
        import asyncio
        gate = asyncio.Semaphore(self.max_concurrent_reads)

        async def one(item):
            async with gate:
                return await fn(item)
        results = await asyncio.gather(*(one(i) for i in items),
                                       return_exceptions=True)
        for r in results:
            if isinstance(r, BaseException):
                raise r
        return results

    def get_many(self, refs: Iterable[Ref]) -> Dict[Ref, bytes]:
        """Fetch many references concurrently — the fast path for hydrating a
        store over a network backend, where each read is otherwise one serial
        HTTP round trip (painful on a high-latency link). Reads are safe to
        parallelise freely: everything here is immutable and content-addressed,
        so there is nothing to lock."""
        refs = list(refs)
        if not refs:
            return {}
        try:
            got = self._run(self._many, self._async.bytes_get, refs)
        except FileNotFoundError as e:
            raise KeyError(f"reference not found: {e}") from None
        return dict(zip(refs, got))

    def put_many(self, datas: Iterable[bytes]) -> List[Ref]:
        """Upload independent blobs concurrently, preserving order. Used for a
        commit's value blobs, which have no dependencies on one another."""
        from swarmfs.exceptions import StampError
        datas = list(datas)
        if not datas:
            return []

        async def post(data):
            return await self._async.bytes_post(data, self.batch,
                                                deferred=self.deferred)
        try:
            return self._run(self._many, post, datas)
        except StampError as e:
            raise self._refused(e) from e


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


class _RecordingStore:
    """Forwards to `inner`, recording the ref of every blob written through
    it — how `commit()` learns exactly which blobs a commit created, which
    a local-first backend's journal wants (`commit_root`). Trie copy-on-
    write means only changed paths and new values are written, so the
    recorded set is naturally the commit's new-blob list."""

    def __init__(self, inner: BytesStore):
        self.inner = inner
        self.refs: set = set()

    def put(self, data: bytes) -> Ref:
        ref = self.inner.put(data)
        self.refs.add(ref)
        return ref

    def put_many(self, datas: Iterable[bytes]) -> List[Ref]:
        datas = list(datas)
        put_many = getattr(self.inner, "put_many", None)
        refs = (put_many(datas) if put_many
                else [self.inner.put(d) for d in datas])
        self.refs.update(refs)
        return refs

    def get(self, ref: Ref) -> bytes:
        return self.inner.get(ref)

    def get_many(self, refs: Iterable[Ref]) -> Dict[Ref, bytes]:
        get_many = getattr(self.inner, "get_many", None)
        if get_many is not None:
            return get_many(refs)
        return {r: self.inner.get(r) for r in refs}

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
    """Bounded LRU over decoded trie nodes. Any entry may be dropped.
    Commit-scoped `pending:` placeholders are kept out of it, in
    `_Trie._pending` (until 0.22.2 they were cached too, and exempt from
    eviction: once a commit's placeholders outnumbered the cache, every
    insertion scanned all of them for a victim, so a commit of ~12,000
    records took over ten minutes instead of seconds). The exemption below
    stays as a guard."""

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
    #: Most nodes a full walk (`items`, `refs_under`) loads ahead of where it
    #: is. Walking one node at a time cost one round trip per node with
    #: children (2,679 for a 5,380-record store: ~10 minutes on a Bee light
    #: node); looking ahead level by level costs about one per trie level per
    #: lookahead window. Capped at half the node cache, so prefetched nodes
    #: are still there when the walk reaches them.
    WALK_LOOKAHEAD = 4096

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

    def _has(self, ref: Ref) -> bool:
        """Is `ref` at hand without a fetch: a commit's placeholder, or cached?"""
        return ref in self._pending or ref in self._cache

    def _load(self, ref: Ref) -> _Node:
        node = self._pending.get(ref)
        if node is not None:
            return node            # a placeholder of the commit being built
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
            node = self._pending.get(r) or self._cache.get(r)
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
            self._pending[pid] = node  # `_load` serves it during the build
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

    def _diff(self, a_root: Optional[Ref], b_root: Optional[Ref]):
        """Yield (key, a_value_ref|None, b_value_ref|None) for every key where
        `a_root` and `b_root` differ. Subtrees with equal refs are pruned, so
        the cost is proportional to the difference, not the dataset.

        Depth-first over pairs of subtrees, so memory stays bounded; loading
        is separate, as in `items`: whenever the next pair holds an unloaded
        node, `_prefetch_pairs` loads ahead level by level, so a network
        store pays about one round per trie level per lookahead window
        instead of one per differing node."""
        if a_root == b_root:
            return
        stack = [(a_root, b_root, b"")]
        while stack:
            if self._unloaded(stack[-1]):
                self._prefetch_pairs(stack)
            a, b, acc = stack.pop()
            values, pairs = self._diff_step(self._as_node(a),
                                            self._as_node(b), acc)
            yield from values
            stack.extend(pairs)

    def _as_node(self, side) -> Optional[_Node]:
        """A pair's side is a ref, a split node (not itself stored), or None."""
        return self._load(side) if isinstance(side, str) else side

    def _unloaded(self, pair) -> bool:
        return any(isinstance(x, str) and not self._has(x)
                   for x in pair[:2])

    @staticmethod
    def _diff_step(a: Optional[_Node], b: Optional[_Node], acc: bytes):
        """One pair of subtrees: the differing values at their tops, and the
        pairs still to compare. Equal prefixes compare child by child,
        skipping equal refs; diverging prefixes hold disjoint keys; a prefix
        that extends the other's lives under one of the other's branches."""
        values: list = []
        pairs: list = []
        if a is None or b is None:
            node = a if b is None else b
            if node is None:
                return values, pairs
            full = acc + node.prefix
            if node.value_ref is not None:
                values.append((full, node.value_ref, None) if b is None
                              else (full, None, node.value_ref))
            for byte, cref in node.children.items():
                key = full + bytes([byte])
                pairs.append((cref, None, key) if b is None
                             else (None, cref, key))
            return values, pairs

        pa, pb = a.prefix, b.prefix
        if pa == pb:
            ka = acc + pa
            if a.value_ref != b.value_ref:
                values.append((ka, a.value_ref, b.value_ref))
            for byte in set(a.children) | set(b.children):
                ca, cb = a.children.get(byte), b.children.get(byte)
                if ca != cb:  # equal refs: a shared subtree
                    pairs.append((ca, cb, ka + bytes([byte])))
            return values, pairs

        common = _common_prefix(pa, pb)
        if len(common) < len(pa) and len(common) < len(pb):
            # prefixes diverge => the two subtrees cover disjoint keys
            pairs.append((a, None, acc))
            pairs.append((None, b, acc))
        elif len(common) == len(pa):
            # a's key is a proper prefix of b's: b lives under one of a's branches
            ka = acc + pa
            bb = pb[len(pa)]
            b_split = _Node(pb[len(pa) + 1:], b.value_ref, b.children)
            if a.value_ref is not None:
                values.append((ka, a.value_ref, None))  # no key at ka on b's side
            for byte, cref in a.children.items():
                pairs.append((cref, b_split if byte == bb else None,
                              ka + bytes([byte])))
            if bb not in a.children:
                pairs.append((None, b_split, ka + bytes([bb])))
        else:
            # symmetric: b's key is a proper prefix of a's
            kb = acc + pb
            ab = pa[len(pb)]
            a_split = _Node(pa[len(pb) + 1:], a.value_ref, a.children)
            if b.value_ref is not None:
                values.append((kb, None, b.value_ref))
            for byte, cref in b.children.items():
                pairs.append((a_split if byte == ab else None, cref,
                              kb + bytes([byte])))
            if ab not in b.children:
                pairs.append((a_split, None, kb + bytes([ab])))
        return values, pairs

    def _prefetch_pairs(self, stack) -> None:
        """`_prefetch` for a diff: load the unloaded sides of the pairs on
        `stack` (nearest first) and, level by level, of the pairs they
        expand into by `_diff_step` itself, so what is loaded is what the
        diff will visit. Same bounds as `_prefetch`."""
        budget = max(1, min(self.WALK_LOOKAHEAD, self._cache.maxsize // 2))
        frontier = list(reversed(stack))  # nearest to be visited first
        fetched = visited = 0
        while frontier and fetched < budget and visited < 2 * budget:
            missing, seen = [], set()
            for a, b, _ in frontier:
                for side in (a, b):
                    if (isinstance(side, str) and not self._has(side)
                            and side not in seen
                            and len(missing) < budget - fetched):
                        seen.add(side)
                        missing.append(side)
            if missing:
                self._load_many(missing)
                fetched += len(missing)
            nxt = []
            for a, b, acc in frontier:
                if self._unloaded((a, b)):
                    continue  # beyond this round's budget
                visited += 1
                nxt.extend(self._diff_step(self._as_node(a),
                                           self._as_node(b), acc)[1])
            frontier = nxt

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

    def _prefetch(self, stack: List[Tuple[Ref, bytes]], prefix: bytes) -> None:
        """Load what a depth-first walk will visit next, in as few batches
        as the trie has levels: the unloaded refs on its stack (nearest
        first) and, level by level, the children of those already loaded,
        skipping subtrees that cannot hold `prefix`. Bounded by
        `WALK_LOOKAHEAD` and by half the node cache. Only loads; the walk's
        order and results are its own."""
        budget = max(1, min(self.WALK_LOOKAHEAD, self._cache.maxsize // 2))
        frontier = list(reversed(stack))  # nearest to be visited first
        fetched = visited = 0
        while frontier and fetched < budget and visited < 2 * budget:
            missing = []
            for ref, _ in frontier:
                if not self._has(ref) and len(missing) < budget - fetched:
                    missing.append(ref)
            if missing:
                self._load_many(missing)
                fetched += len(missing)
            nxt = []
            for ref, acc in frontier:
                node = self._pending.get(ref) or self._cache.get(ref)
                if node is None:
                    continue  # beyond this round's budget
                visited += 1
                full = acc + node.prefix
                probe = min(len(full), len(prefix))
                if full[:probe] != prefix[:probe]:
                    continue
                for byte in sorted(node.children):
                    nxt.append((node.children[byte], full + bytes([byte])))
            frontier = nxt

    def items(self, root: Optional[Ref],
              prefix: bytes = b"") -> Iterator[Tuple[bytes, Ref]]:
        """All (key, value_ref) with key under `prefix`, in sorted key order."""
        if root is None:
            return
        # Sorted pre-order DFS: a node's own key precedes its descendants',
        # children visited in byte order, so keys come out sorted with no final
        # sort and no result-set-sized buffer. Loading is separate: whenever
        # the walk reaches an unloaded node, `_prefetch` loads ahead level by
        # level, so a network store pays one round per trie level per
        # lookahead window instead of one per node.
        stack = [(root, b"")]
        while stack:
            if not self._has(stack[-1][0]):
                self._prefetch(stack, prefix)
            ref, acc = stack.pop()
            node = self._load(ref)
            full = acc + node.prefix
            # prune subtrees that cannot contain the prefix
            probe = min(len(full), len(prefix))
            if full[:probe] != prefix[:probe]:
                continue
            if node.value_ref is not None and full.startswith(prefix):
                yield (full, node.value_ref)
            for byte in sorted(node.children, reverse=True):  # smallest pops first
                stack.append((node.children[byte], full + bytes([byte])))

    def refs_under(self, root: Optional[Ref],
                   prefix: bytes = b"") -> Iterator[Tuple[str, Ref]]:
        """Yield ``("node", ref)`` / ``("value", ref)`` for every blob
        needed to read every key under `prefix`: the trie nodes on the
        path from the root and in the prefix's subtree, plus the value
        refs of the contained records. This is the working-set walk behind
        pin-by-prefix, fetch warm-ups, and history squashing — the
        application-side reachability the blob-blind journal layer cannot
        compute itself."""
        if root is None:
            return
        stack = [(root, b"")]
        while stack:
            if not self._has(stack[-1][0]):
                self._prefetch(stack, prefix)
            ref, acc = stack.pop()
            node = self._load(ref)
            full = acc + node.prefix
            probe = min(len(full), len(prefix))
            if full[:probe] != prefix[:probe]:
                continue  # cannot contain the prefix: skip subtree
            yield ("node", ref)
            if node.value_ref is not None and full.startswith(prefix):
                yield ("value", node.value_ref)
            for byte in sorted(node.children):
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
# Extension proofs: a later root keeps an earlier root's records
#
# "Under these prefixes, root B holds every record root A held, unchanged" —
# what an append-only keyspace promises (a register's revocations, a book's
# tombstones and fills, a catalogue's categories), made checkable by anyone
# who holds the two roots. Records under the prefixes may be *added* in B;
# none may be removed or altered. Unchanged means an equal value reference,
# and a reference is the content address of the value's bytes.
#
# The proof carries the raw trie nodes a lockstep walk of the two roots
# touches, restricted to the prefixes: a subtree whose reference is the same
# under both roots is skipped unopened (content addressing: equal reference,
# equal subtree), keys only B holds are skipped, and the walk follows the
# three shapes in which two canonical tries can differ (equal edges, one
# edge a proper prefix of the other — a split — or diverging edges). Its
# size is proportional to what changed under the prefixes, not to the
# store. The verifier replays the same walk over the carried nodes alone,
# indexed by their recomputed addresses, and fails on any record of A the
# walk finds missing or changed in B, or on any node it needs and the proof
# lacks. Format "recordstore-extension-proof", version 1.
# ---------------------------------------------------------------------------

EXTENSION_FORMAT = "recordstore-extension-proof"


def _meets(key_prefix: bytes, prefix: bytes) -> bool:
    """Can a subtree whose keys all start with `key_prefix` hold a key
    starting with `prefix`?"""
    return key_prefix.startswith(prefix) or prefix.startswith(key_prefix)


def _items_under(load, node: _Node, acc: bytes, prefix: bytes):
    """(key, value_ref, None) for every value under `prefix` in the subtree
    rooted at `node` — each one missing from the other side. Children that
    cannot hold the prefix are never loaded."""
    stack = [(node, acc)]
    while stack:
        n, a = stack.pop()
        full = a + n.prefix
        if not _meets(full, prefix):
            continue
        if n.value_ref is not None and full.startswith(prefix):
            yield (full, n.value_ref, None)
        for byte in sorted(n.children, reverse=True):
            key_prefix = full + bytes([byte])
            if _meets(key_prefix, prefix):
                stack.append((load(n.children[byte]), key_prefix))


def _extension_faults(load, a: Optional[_Node], b: Optional[_Node], acc: bytes, prefix: bytes):
    """(key, a_value_ref, b_value_ref|None) for every record of `a`'s
    subtree under `prefix` that `b`'s subtree does not hold with the same
    value reference — the same three shapes as `_Trie._diff_step`, but one
    way: what only `b` holds is never visited."""
    if a is None:
        return
    if not _meets(acc + a.prefix, prefix):
        return
    if b is None:
        yield from _items_under(load, a, acc, prefix)
        return
    pa, pb = a.prefix, b.prefix
    if pa == pb:
        ka = acc + pa
        if a.value_ref is not None and ka.startswith(prefix) and a.value_ref != b.value_ref:
            yield (ka, a.value_ref, b.value_ref)
        for byte in sorted(a.children):
            ca, cb = a.children[byte], b.children.get(byte)
            key_prefix = ka + bytes([byte])
            if ca == cb or not _meets(key_prefix, prefix):
                continue                      # shared subtree, or outside the prefix
            yield from _extension_faults(load, load(ca), load(cb) if cb is not None else None,
                                         key_prefix, prefix)
        return
    common = _common_prefix(pa, pb)
    if len(common) < len(pa) and len(common) < len(pb):
        yield from _items_under(load, a, acc, prefix)       # disjoint key ranges
    elif len(common) == len(pa):
        # a's edge is a proper prefix of b's: b lives under one of a's branches
        ka = acc + pa
        bb = pb[len(pa)]
        b_split = _Node(pb[len(pa) + 1:], b.value_ref, b.children)
        if a.value_ref is not None and ka.startswith(prefix):
            yield (ka, a.value_ref, None)
        for byte in sorted(a.children):
            key_prefix = ka + bytes([byte])
            if not _meets(key_prefix, prefix):
                continue
            child = load(a.children[byte])
            if byte == bb:
                yield from _extension_faults(load, child, b_split, key_prefix, prefix)
            else:
                yield from _items_under(load, child, key_prefix, prefix)
    else:
        # b's edge is a proper prefix of a's: a lives under one of b's branches
        kb = acc + pb
        ab = pa[len(pb)]
        a_split = _Node(pa[len(pb) + 1:], a.value_ref, a.children)
        cb = b.children.get(ab)
        yield from _extension_faults(load, a_split, load(cb) if cb is not None else None,
                                     kb + bytes([ab]), prefix)


def _extension_walk(load, base: Optional[Ref], root: Optional[Ref], prefix: bytes):
    if base is None or base == root:
        return                                # nothing held before, or the same state
    yield from _extension_faults(load, load(base), load(root) if root is not None else None,
                                 b"", prefix)


def _check_prefixes(prefixes) -> List[str]:
    if isinstance(prefixes, str):
        raise TypeError("prefixes is a list of strings, not one string")
    out = sorted(set(prefixes))
    if not out or not all(isinstance(p, str) for p in out):
        raise ValueError("an extension proof names at least one string prefix ('' for every key)")
    return out


def verify_extension(proof, base: Optional[Ref], root: Optional[Ref]) -> Tuple[str, ...]:
    """Check that `root` extends `base` under the proof's prefixes — every
    record `base` held under them is held by `root` with the same value —
    and return the prefixes proven. `base` and `root` are the references the
    *verifier* trusts (None: the empty store). Raises ``ProofError`` on any
    mismatch. Pure: reads no store, replays the walk over the node blobs the
    envelope carries."""
    if not isinstance(proof, dict) or proof.get("format") != EXTENSION_FORMAT:
        raise ProofError(f"not a {EXTENSION_FORMAT} envelope")
    if proof.get("version") != 1:
        raise ProofError(f"unsupported proof version {proof.get('version')!r}")
    if proof.get("base") != base or proof.get("root") != root:
        raise ProofError(f"proof is about {proof.get('base')!r} -> {proof.get('root')!r}, "
                         f"not {base!r} -> {root!r}")
    ref_of = _resolve_addressing(proof.get("addressing"))
    try:
        prefixes = _check_prefixes(proof.get("prefixes") or [])
    except (TypeError, ValueError) as exc:
        raise ProofError(str(exc)) from None
    if prefixes != list(proof.get("prefixes")):
        raise ProofError("the proof's prefixes are not in canonical order")
    nodes: Dict[Ref, bytes] = {}
    try:
        for blob_hex in proof.get("nodes", []):
            blob = bytes.fromhex(blob_hex)
            nodes[ref_of(blob)] = blob
    except (TypeError, ValueError):
        raise ProofError("malformed node bytes in proof") from None

    def load(ref: Ref) -> _Node:
        blob = nodes.get(ref)
        if blob is None:
            raise ProofError(f"the proof lacks node {ref}")
        try:
            return _Trie._decode(blob)
        except (ValueError, KeyError):
            raise ProofError(f"node {ref} is not a valid trie node") from None

    for prefix in prefixes:
        for key, _a, b in _extension_walk(load, base, root, prefix.encode("utf-8")):
            what = "is absent from" if b is None else "changed in"
            raise ProofError(f"{key.decode('utf-8', 'replace')!r} {what} the later root")
    return tuple(prefixes)


# ---------------------------------------------------------------------------
# Pointers ("latest root")
# ---------------------------------------------------------------------------

class Pointer(Protocol):
    def get(self) -> Optional[Ref]: ...
    def set(self, root: Ref) -> None: ...


class Version(NamedTuple):
    """One state this store has been in — a row of `RecordStore.history()`.

    `root` is the content address, so it is the whole state: `RecordStore.at`
    reopens it exactly. `at` and `message` are **local annotations**, recorded
    beside the pointer and deliberately outside the content — see
    `RecordStore.commit`."""

    root: Ref
    at: Optional[str] = None            # ISO-8601 UTC, when this replica got here
    message: Optional[str] = None
    current: bool = False               # is the pointer here now?


class _Timeline:
    """Where a pointer has been, and where it is now.

    `history_enabled` is the capability a store asks about: a pointer that keeps
    no timeline must make `undo` explain itself rather than quietly do nothing.

    The undo/redo model, and it is an editor's rather than a journal's: a list
    of states in the order they were reached, plus a position in it. Moving the
    position is undo/redo; committing a new state truncates anything ahead of
    the position and appends, so a new commit after an undo abandons the redo
    tail — exactly as typing after undo does, and as git does to a branch.

    Deliberately separate from a local-first store's journal, which keeps
    *every* root ever committed and their parent links. That is the deeper
    audit (git's reflog to this timeline's branch); an abandoned root stays
    readable by ref, since nothing content-addressed is ever destroyed.
    """

    history_enabled = True

    def __init__(self):
        self._line: List[dict] = []
        self._at = -1

    # -- storage seam: subclasses persist, this one keeps it in memory --------
    def _load(self) -> Tuple[List[dict], int]:
        return self._line, self._at

    def _store(self, line: List[dict], at: int) -> None:
        self._line, self._at = line, at

    def record(self, root: Ref, message: Optional[str] = None) -> None:
        """A newly reached state becomes the tip.

        A commit that changed nothing lands on the root it started from, and is
        not a state: recording it would put two identical entries in the line
        and make an `undo` look broken (it would step from a root to the same
        root). Content addressing is what makes that check trivial."""
        line, at = self._load()
        line = line[:at + 1]                       # abandon the redo tail
        if line and line[-1]["root"] == root:
            self._store(line, len(line) - 1)
            return
        line.append({"root": root, "at": _utc_now(), "message": message})
        self._store(line, len(line) - 1)

    def entries(self) -> List[dict]:
        return list(self._load()[0])

    def position(self) -> int:
        return self._load()[1]

    def step(self, delta: int) -> Optional[Ref]:
        """The root `delta` steps away, or None at the end of the line."""
        line, at = self._load()
        target = at + delta
        if not line or not 0 <= target < len(line):
            return None
        self._store(line, target)
        return line[target]["root"]

    def seek(self, root: Ref) -> bool:
        """Point at an already-known state. False if this line has never held it."""
        line, _ = self._load()
        for index in range(len(line) - 1, -1, -1):  # most recent occurrence
            if line[index]["root"] == root:
                self._store(line, index)
                return True
        return False


def _utc_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class MemoryPointer(_Timeline):
    def __init__(self, root: Optional[Ref] = None):
        super().__init__()
        self._root = root
        if root is not None:
            self.record(root)

    def get(self) -> Optional[Ref]:
        return self._root

    def set(self, root: Ref, message: Optional[str] = None) -> None:
        self._root = root
        self.record(root, message)

    def move_to(self, root: Optional[Ref]) -> None:
        """Point here without calling it a new state (undo/redo/checkout)."""
        self._root = root

    def compare_and_set(self, expected: Optional[Ref], new: Optional[Ref]) -> bool:
        """Atomic in-process compare-and-set — lets reconciling commits over a
        shared in-process pointer converge without a lost-update race."""
        if self._root == expected:
            self._root = new
            self.record(new)
            return True
        return False


class FilePointer(_Timeline):
    """Local-file pointer, useful during development.

    Keeps a sibling `<path>.timeline` (JSON) so the store it points at can
    answer `history()`, `undo()` and `redo()`. Best-effort under concurrent
    writers, exactly like the ref file itself: a local-first store serializes
    writers with its own lock, and two racing bare processes can lose a
    timeline entry without losing data (every root stays readable by ref).
    """

    def __init__(self, path: str, keep_history: bool = True):
        super().__init__()
        self.path = path
        self.keep_history = keep_history

    @property
    def history_enabled(self) -> bool:              # type: ignore[override]
        return self.keep_history

    def get(self) -> Optional[Ref]:
        try:
            with open(self.path) as f:
                content = f.read().strip()
                return content or None
        except FileNotFoundError:
            return None

    def _write(self, root: Ref) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            f.write(root)
        os.replace(tmp, self.path)  # atomic on POSIX

    def set(self, root: Ref, message: Optional[str] = None) -> None:
        self._write(root)
        if self.keep_history:
            self.record(root, message)

    def move_to(self, root: Optional[Ref]) -> None:
        if root is not None:
            self._write(root)

    # -- _Timeline storage: a JSON sibling, rewritten atomically -------------
    @property
    def timeline_path(self) -> str:
        return self.path + ".timeline"

    def _load(self) -> Tuple[List[dict], int]:
        try:
            with open(self.timeline_path, encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, ValueError):
            return [], -1
        line = data.get("line") or []
        at = data.get("at", len(line) - 1)
        return line, at if -1 <= at < len(line) else len(line) - 1

    def _store(self, line: List[dict], at: int) -> None:
        tmp = self.timeline_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"line": line, "at": at}, f)
        os.replace(tmp, self.timeline_path)


class SwarmFeedPointer:
    """Mutable "latest root" backed by a Swarm feed.

    A Swarm feed is an owner-signed, mutable pointer: each update is a
    single-owner chunk (SOC), BMT-hashed and secp256k1-signed with the feed
    owner's key, posted to Bee's ``/soc/{owner}/{id}`` endpoint; readers
    resolve "the latest" by sequence-index lookup at
    ``GET /feeds/{owner}/{topic}``. This maps a feed onto the `Pointer`
    protocol: ``set(root)`` publishes a new signed update, ``get()`` resolves
    the latest root.

    Requires swarmfs >= 0.13 with coincurve (``pip install
    "recordstore[feeds]"``): swarmfs talks to Bee and signs through
    ``swarmfs.signer`` (libsecp256k1); a read-only pointer needs no coincurve.
    Imported lazily, so the recordstore core stays stdlib-only. Until 0.22
    this ran on the ``swarm-bee`` package; feeds written either way read the
    same (same topic hashing, identifiers, signatures and payload).

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
    flaky as the feed grows; with no confirmed index yet to resume from it
    probes. This is a Swarm/light-node characteristic, not a client defect —
    any client hits it identically. Every chunk read is verified: the owner's
    signature must recover to ``owner`` at the chunk's address.

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
            from fsspec.asyn import sync
            from swarmfs import SwarmClient, SyncSwarmClient
            from swarmfs.bmt import keccak256
            from swarmfs.exceptions import BeeAPIError
            from swarmfs.feeds import FeedOps, FeedSigner, owner_bytes
        except ImportError as e:  # pragma: no cover - only without the extra
            raise ImportError(
                "SwarmFeedPointer requires swarmfs >= 0.13; install it with: "
                'pip install "recordstore[feeds]"'
            ) from e

        self._BeeAPIError = BeeAPIError
        client = SwarmClient(api_url)
        self._client = SyncSwarmClient(client=client)
        self._ops = FeedOps(client)
        self._run = lambda fn, *a, **kw: sync(self._client.loop, fn, *a, **kw)
        # A topic name is keccak256 of its UTF-8, as bee-js' Topic.fromString
        # and swarm-bee hash it — always, even for a 64-hex name.
        self._topic = keccak256(topic.encode("utf-8"))

        self._signer = FeedSigner(signer) if signer else None
        if self._signer is not None:
            self._owner = self._signer.owner
        elif owner is not None:
            self._owner = owner_bytes(owner)
        else:
            raise ValueError(
                "SwarmFeedPointer needs a signer (to read and write) or an "
                "owner address (read-only)"
            )
        if postage_batch_id is not None:
            batch = postage_batch_id.lower().removeprefix("0x")
            if len(batch) != 64 or any(c not in "0123456789abcdef" for c in batch):
                raise ValueError(f"{postage_batch_id!r} is not a postage batch id")
            postage_batch_id = batch
        self._batch = postage_batch_id

        self._ttl = feed_ttl
        self._max_retries = max(1, max_lookup_retries)
        self._backoff = retry_backoff
        self._backoff_cap = retry_backoff_cap

        # read-your-writes cache + monotonic index floor
        self._cached_ref: Optional[Ref] = None
        self._next_index = 0
        self._cache_expiry = 0.0

    @property
    def owner(self) -> str:
        """The feed owner's 20-byte address, hex (no ``0x``)."""
        return self._owner.hex()

    @property
    def topic(self) -> str:
        """The 32-byte feed topic, hex."""
        return self._topic.hex()

    def _transient(self, e: Exception) -> bool:
        """404s and 500s from a lookup or chunk read are worth asking again:
        a light node answers both for the same chunk moments apart."""
        return isinstance(e, FileNotFoundError) or (
            isinstance(e, self._BeeAPIError) and e.status in (404, 500))

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
        self._run(self._ops.update, self._signer, self._topic, index, root,
                  self._batch)
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
                    upd = self._run(self._ops.at_index, self._owner,
                                    self._topic, latest_index, verify=True)
                    self._cached_ref = upd.reference
                    self._next_index = index_next
                    self._cache_expiry = time.monotonic() + self._ttl
                    return self._cached_ref
                if index_next == self._next_index:
                    # confirmed unchanged; serve cache and refresh the TTL.
                    self._cache_expiry = time.monotonic() + self._ttl
                    return self._cached_ref
                # index_next < floor: stale-early lookup; retry for a fresher one.
            except (FileNotFoundError, self._BeeAPIError) as e:
                if not self._transient(e):
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
        Raises only on transient chunk-fetch errors, which the retry loop in
        ``get`` absorbs."""
        hint = self._next_index - 2  # one below our last-confirmed index
        if hint >= 1:
            try:
                head = self._client.feed_head(self._owner.hex(),
                                              self._topic.hex(), after=hint)
                if head is not None:
                    return int(head[0], 16)
            except (FileNotFoundError, self._BeeAPIError) as e:
                if not self._transient(e):
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
        """Does the feed's chunk at `index` exist? 404 is the one definitive
        "no". A 500 is a retrieval that did not complete — Bee logs "read
        chunk failed" and a light node answers 500 and 404 for the same
        absent chunk moments apart — so it is retried here with the pointer's
        backoff, like `get`'s lookups, and raised only once the retries are
        spent. Before 0.20.3 the probe raised at the first 500 and `set`,
        which probes cold, had no retry of its own: a first commit to a fresh
        feed failed on one flaky read (loopmarket's live gate, 2026-09-18)."""
        delay = self._backoff
        for attempt in range(self._max_retries):
            try:
                self._run(self._ops.at_index, self._owner, self._topic, index,
                          verify=True)
                return True
            except FileNotFoundError:
                return False
            except self._BeeAPIError as e:
                if e.status == 404:
                    return False
                if e.status != 500 or attempt == self._max_retries - 1:
                    raise  # not transient, or transient for too long
            time.sleep(delay)
            delay = min(delay * 2, self._backoff_cap)
        return False  # pragma: no cover - the loop returns or raises

    def update(self, index: int) -> dict:
        """The feed's update at `index` as a self-contained, verifiable
        envelope: the raw single-owner chunk — the owner's signature over
        (identifier, content), its payload the writer's timestamp and the
        root — beside the owner, topic and index it claims to be. Anyone
        checks it with `verify_feed_update`, with no node. Raises the node's
        response error for an index the feed does not hold (404)."""
        from swarmfs.feeds import feed_identifier, soc_address
        address = soc_address(feed_identifier(self._topic, index), self._owner)
        wire = self._client.chunk_get(address.hex())
        return {"format": FEED_UPDATE_FORMAT, "version": 1, "owner": self._owner.hex(),
                "topic": self._topic.hex(), "index": int(index), "soc": bytes(wire).hex()}

    def updates(self, start: int = 0, stop: Optional[int] = None) -> List[dict]:
        """The feed's updates from `start` up to `stop` (default: past the tip,
        found by probing), as `update` envelopes — the owner-signed sequence
        of roots, in index order: what orders a store's published states
        for a third party, where `history` is this replica's own timeline."""
        if stop is None:
            tip = self._probe_latest_index()
            stop = tip + 1 if tip is not None else 0
        return [self.update(i) for i in range(start, stop)]


# ---------------------------------------------------------------------------
# Feed updates: a published sequence of roots, verifiable offline
#
# A store behind a Swarm feed publishes each committed root as a feed update:
# a single-owner chunk at address keccak(identifier || owner), identifier =
# keccak(topic || index as 8 big-endian bytes), signed by the owner, its
# payload the writer's 8-byte timestamp and the root. The chunk alone proves
# "the owner published this root as update `index`", so the sequence of a
# store's states is checkable by a third party — the ordering an extension
# proof lacks (which of two roots came later). Two different roots signed at
# one index are a self-contained proof of equivocation. What a feed cannot
# prove is time: its timestamp is the writer's own claim, binding on the
# writer, not a clock; "no newer root existed at t" needs a trusted anchor.
# Format "recordstore-feed-update", version 1. Verifying needs swarmfs (no
# coincurve: signature recovery falls back to pure Python), loaded lazily.
# ---------------------------------------------------------------------------

FEED_UPDATE_FORMAT = "recordstore-feed-update"


class FeedUpdate(NamedTuple):
    """One verified feed update: the owner published `root` as update
    `index`, stamping it `timestamp` (the writer's claim, unix seconds)."""
    index: int
    root: Ref
    timestamp: int


def _hex_owner(owner: str) -> str:
    h = owner[2:] if owner.startswith("0x") else owner
    if len(h) != 40:
        raise ProofError(f"{owner!r} is not a 20-byte owner address")
    return h.lower()


def _hex_topic(topic: str) -> str:
    """A 32-byte topic in hex, or a topic name as `SwarmFeedPointer` hashes it."""
    h = topic[2:] if topic.startswith("0x") else topic
    if len(h) == 64 and all(c in "0123456789abcdefABCDEF" for c in h):
        return h.lower()
    from swarmfs.bmt import keccak256
    return keccak256(topic.encode("utf-8")).hex()


def verify_feed_update(update, owner: str, topic: str) -> FeedUpdate:
    """Check that `update` is `owner`'s signed update of the feed `topic`
    (a name, as `SwarmFeedPointer` takes it, or the 32-byte topic in hex) at
    the index it claims, and return what it publishes. Raises
    ``ProofError`` on any mismatch. Pure: no node, only the chunk's bytes."""
    try:
        from swarmfs.feeds import SOC_PAYLOAD_OFFSET, feed_identifier, soc_address, verify_soc
        from swarmfs.join import VerificationError
    except ImportError as e:  # pragma: no cover - only without the extra
        raise ImportError('verifying feed updates requires swarmfs >= 0.13; '
                          'install it with: pip install "recordstore[feeds]"') from e
    if not isinstance(update, dict) or update.get("format") != FEED_UPDATE_FORMAT:
        raise ProofError(f"not a {FEED_UPDATE_FORMAT} envelope")
    if update.get("version") != 1:
        raise ProofError(f"unsupported feed update version {update.get('version')!r}")
    owner_hex, topic_hex = _hex_owner(owner), _hex_topic(topic)
    if update.get("owner") != owner_hex or update.get("topic") != topic_hex:
        raise ProofError("the update is another owner's or another feed's")
    index = update.get("index")
    if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < (1 << 64):
        raise ProofError("the update names no index")
    owner_b = bytes.fromhex(owner_hex)
    address = soc_address(feed_identifier(bytes.fromhex(topic_hex), index), owner_b)
    try:
        wire = bytes.fromhex(update.get("soc", ""))
        verify_soc(wire, owner_b, address)
    except (ValueError, VerificationError):
        raise ProofError("the chunk is not the owner's signed update at this index") from None
    payload = wire[SOC_PAYLOAD_OFFSET:]
    if len(payload) not in (8 + 32, 8 + 64):
        raise ProofError("the update's payload is not a timestamp and a root")
    return FeedUpdate(index, payload[8:].hex(), int.from_bytes(payload[:8], "big"))


def verify_equivocation(a, b, owner: str, topic: str) -> int:
    """Two updates by `owner` of one feed at one index publishing different
    roots: the owner equivocated, and this returns the index. Each envelope
    verifies alone (`verify_feed_update`), so the pair is a self-contained,
    third-party-checkable accusation. Raises ``ProofError`` when either does
    not verify, the indices differ (a sequence, not a conflict), or the roots
    agree."""
    ua, ub = verify_feed_update(a, owner, topic), verify_feed_update(b, owner, topic)
    if ua.index != ub.index:
        raise ProofError("updates at different indices are a sequence, not an equivocation")
    if ua.root == ub.root:
        raise ProofError("the two updates publish the same root")
    return ua.index


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
    max_concurrent_reads: int = 32,
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
        try:
            return _decode_value(self._blobs.get(vref))
        except (KeyError, OSError) as e:
            # The key EXISTS — the trie names its value — so a blob-level
            # failure here is unreachability (evicted + offline, backend
            # loss), never absence. KeyError would lie to `contains` and
            # to every `except KeyError` caller.
            raise RecordUnavailable(
                f"record {key!r} exists (value {vref[:16]}…) but its bytes "
                f"are unreachable: {e}") from e

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

    def extends(self, base: Optional[Ref], prefixes: Iterable[str] = ("",)) -> bool:
        """Does the committed root hold, under each of `prefixes`, every
        record `base` held, unchanged (additions allowed)? The local check
        behind `prove_extension`; `base`'s nodes must be in this store's
        bytes store (a content-addressed store keeps every root it wrote)."""
        load = self._trie._load
        return not any(True for p in _check_prefixes(prefixes)
                       for _ in _extension_walk(load, base, self._root, p.encode("utf-8")))

    def prove_extension(self, base: Optional[Ref], prefixes: Iterable[str] = ("",),
                        addressing: Optional[str] = None) -> dict:
        """A verifiable proof that the committed root extends `base` under
        `prefixes`: every record `base` held under them is still held, with
        the same value; records may have been added, none removed or
        altered. The result is a JSON-ready dict that
        ``verify_extension(proof, base, root)`` checks with no store access;
        its nodes are those of the two roots along what changed under the
        prefixes, so its size follows the change, not the store. Raises
        ``ValueError`` when the root does not extend `base` (the offending
        key named) and when a key under the prefixes has staged changes —
        proofs are statements about committed roots. Self-verified before
        being returned, as `prove` is."""
        prefixes = _check_prefixes(prefixes)
        staged = [k for k in self._staged if any(k.startswith(p) for p in prefixes)]
        if staged:
            raise ValueError(f"{staged[0]!r} has staged, uncommitted changes under the "
                             "prefixes — commit() first")
        name = addressing or _addressing_name(self._blobs)
        blobs: Dict[Ref, str] = {}

        def load(ref: Ref) -> _Node:
            blob = self._blobs.get(ref)       # the exact bytes behind the ref
            blobs[ref] = blob.hex()
            return _Trie._decode(blob)

        for prefix in prefixes:
            for key, _a, b in _extension_walk(load, base, self._root, prefix.encode("utf-8")):
                what = "absent from" if b is None else "changed in"
                raise ValueError(f"{self._root!r} does not extend {base!r} under {prefix!r}: "
                                 f"{key.decode('utf-8', 'replace')!r} is {what} it")
        proof = {
            "format": EXTENSION_FORMAT,
            "version": 1,
            "addressing": name,
            "base": base,
            "root": self._root,
            "prefixes": prefixes,
            "nodes": sorted(blobs.values()),
        }
        verify_extension(proof, base, self._root)   # never hand out a broken proof
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
        # A window is a barrier (each waits for its slowest fetch), so it is
        # several times the store's concurrency rather than equal to it:
        # 256 records keep 32 requests in flight with eight waves per wait.
        window = max(256, getattr(self._blobs, "max_concurrent_reads", 256))
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
        # Values are fetched in windows, like `items`.
        window = max(256, getattr(self._blobs, "max_concurrent_reads", 256))
        buf: list = []
        for entry in self._trie._diff(self._root, other_root):
            buf.append(entry)
            if len(buf) >= window:
                yield from self._flush_diff(buf)
                buf = []
        if buf:
            yield from self._flush_diff(buf)

    def _flush_diff(self, buf):
        refs = list({r for _, mine, theirs in buf for r in (mine, theirs)
                     if r is not None})
        blobs = self._fetch_blobs(refs) if refs else {}
        for kb, mine_ref, theirs_ref in buf:
            yield (
                kb.decode("utf-8"),
                ABSENT if mine_ref is None else _decode_value(blobs[mine_ref]),
                ABSENT if theirs_ref is None
                else _decode_value(blobs[theirs_ref]),
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

    def _journal_commit(self, base: Optional[Ref], new: Optional[Ref],
                        vrec: "_RecordingStore",
                        nrec: "_RecordingStore") -> None:
        """Record the commit in a local-first backend's journal (duck-typed:
        the backend has `commit_root`, e.g. swarmfs's LocalStore). The
        recorders captured exactly the blobs this commit wrote — values and
        trie nodes separately, so nodes carry the `structure` eviction
        hint. Skipped when the root didn't change or canonical addressing
        brought the store back to an already-journaled state (an emptied
        store — root None — cannot be journaled either: the format has no
        null-root event; keep a pointer for the head in that case)."""
        inner = self._blobs
        if new is None or new == base or inner.has_root(new):
            return
        parent = base if (base is None or inner.has_root(base)) else None
        inner.commit_root(new, parent, sorted(vrec.refs | nrec.refs),
                          structure=sorted(nrec.refs - vrec.refs))

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

    def commit(self, *, message: Optional[str] = None,
               reconcile: bool = False, resolver=None,
               retries: int = 5) -> Optional[Ref]:
        """Flush staged changes; return the new root and update the pointer.

        The root/pointer changes only after every blob write has succeeded, so
        a reader following the pointer sees all of a commit or none of it.

        `message` labels the state in this replica's timeline (`history()`), and
        is deliberately **not** part of the content. This is the one place the
        git analogy breaks and it matters: a git commit hashes its message, so
        two people who make the same change with different words get different
        commits. A root here is a hash of *state alone*, which is what makes
        equal content converge to one root, dedup structurally, and merge
        without conflict. So a message is a local annotation on a transition,
        never a fact about the data — if attribution has to travel, sign a
        record about the claim instead.

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
        inner = self._blobs
        journaled = hasattr(inner, "commit_root")  # local-first backend?
        vrec = nrec = None
        if journaled:
            # Record which blobs this commit writes — values through
            # self._blobs, trie nodes through the trie's handle — so the
            # journal event can list them (and classify the nodes as
            # `structure`). The recorders forward everything else.
            vrec, nrec = _RecordingStore(inner), _RecordingStore(inner)
            self._blobs, self._trie._blobs = vrec, nrec
        try:
            new = self._build_root(base)
            if self._pointer is not None:
                if reconcile:
                    new = self._reconcile(base, new, resolver, retries)
                else:
                    self._set_pointer(new, message)
        finally:
            if journaled:
                self._blobs, self._trie._blobs = inner, inner
        if journaled:
            self._journal_commit(base, new, vrec, nrec)
        self._staged.clear()
        self._root = new
        return new

    def _set_pointer(self, root: Ref, message: Optional[str] = None) -> None:
        """Pointers that keep a timeline take the message; others never see it.

        Asked by capability rather than by catching TypeError, which would also
        swallow a real signature error inside somebody's pointer."""
        if message is not None and hasattr(self._pointer, "record"):
            self._pointer.set(root, message)          # type: ignore[call-arg]
        else:
            self._pointer.set(root)

    # -- history: where this store has been, and going back ------------------

    def _timeline(self):
        """The pointer's timeline, or a teaching error naming what is needed."""
        pointer = self._pointer
        if pointer is None or not getattr(pointer, "history_enabled", False):
            raise TypeError(
                "this store keeps no history: its pointer does not record one. "
                "Use FilePointer (or a local-first store, whose HEAD is one) "
                "to get history(), undo() and redo()."
            )
        return pointer

    def history(self, limit: Optional[int] = None) -> List[Version]:
        """The states this store has been in, newest first.

        The `git log` of a record store — except that every entry *is* a whole
        state, not a delta, so `RecordStore.at(entry.root, blobs)` reopens any
        of them exactly. Returns `[]` for a store whose pointer keeps no
        timeline; `undo`/`redo` raise there instead, since silently doing
        nothing would be worse.

        An `undo` moves the position without erasing anything, so the entry
        after `current` is what `redo` would go to.
        """
        pointer = self._pointer
        if pointer is None or not getattr(pointer, "history_enabled", False):
            return []
        entries, at = pointer.entries(), pointer.position()
        versions = [Version(entry["root"], entry.get("at"),
                            entry.get("message"), index == at)
                    for index, entry in enumerate(entries)]
        versions.reverse()
        return versions if limit is None else versions[:limit]

    def status(self) -> dict:
        """Where the store is right now — the `git status` of it.

        `staged` counts changes made since the last commit (0 means the root is
        the whole truth). `behind` is how many states are ahead of the current
        position, i.e. how much `redo` could replay."""
        entries: List[dict] = []
        at = -1
        pointer = self._pointer
        if pointer is not None and getattr(pointer, "history_enabled", False):
            entries, at = pointer.entries(), pointer.position()
        return {
            "root": self._root,
            "staged": len(self._staged),
            "readonly": self._readonly,
            "history": len(entries),
            "position": at,
            "undoable": max(0, at),
            "redoable": max(0, len(entries) - 1 - at) if entries else 0,
        }

    def _move_to(self, root: Optional[Ref]) -> Optional[Ref]:
        pointer = self._pointer
        move = getattr(pointer, "move_to", None)
        if move is None:
            self._set_pointer(root)      # pointer without a position: just set
        else:
            move(root)
        self._root = root
        self._staged.clear()             # the new root IS the state
        return root

    def _step(self, delta: int) -> Optional[Ref]:
        """Move, or don't. The end of the line must leave the store where it
        was — an `undo` with nothing behind it once set the root to None and
        emptied the store's view of itself."""
        target = self._timeline().step(delta)
        return None if target is None else self._move_to(target)

    def undo(self) -> Optional[Ref]:
        """Step back to the previous state; None if there is nothing before it.

        Nothing is destroyed and nothing is rewritten: a root is content, so
        going back is *pointing* back, and `redo()` comes forward again. What an
        undo does NOT do is travel — a peer that merges this replica afterwards
        re-adds what was undone, because merge only ever adds (the same wall a
        removal has). Undo is local time travel, not a retraction others see.

        Staged-but-uncommitted changes are dropped: the state you asked for is
        the state you get.
        """
        return self._step(-1)

    def redo(self) -> Optional[Ref]:
        """Step forward again after an undo; None if already at the tip.

        A commit made after an undo abandons the redo tail (see `_Timeline`),
        so this replays only what nothing has overwritten."""
        return self._step(+1)

    def checkout(self, root: Ref) -> Ref:
        """Jump to a named state this store has been in before.

        Refused for a root the timeline has never held — pointing a store at
        content it does not have is how a "restored" store fails later, on the
        first read, rather than here."""
        if not self._timeline().seek(root):
            raise KeyError(
                f"{root} is not a state this store has been in — history() "
                f"lists the ones it has (RecordStore.at reads any root)")
        return self._move_to(root)

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
        conflicts: List[bytes] = []
        for k in set(our_diff) | set(their_diff):
            if k in our_diff and k in their_diff:
                bv, ov = our_diff[k]
                if ov != their_diff[k][1]:
                    conflicts.append(k)        # changed differently on both
                    continue
                merged = ov                    # both changed it the same way
            elif k in our_diff:
                bv, merged = our_diff[k]       # changed by us only
            else:
                bv, merged = their_diff[k]     # changed by them only
            if merged != bv:
                changes[k] = merged if merged is not None else _TOMBSTONE

        if conflicts and resolver is None:
            raise MergeConflict([k.decode("utf-8") for k in conflicts])
        if conflicts:
            changes.update(cls._resolve(bytes_store, sorted(conflicts),
                                        our_diff, their_diff, resolver))

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


    @staticmethod
    def _resolve(bytes_store, conflicts, our_diff, their_diff, resolver):
        """Settle conflicting keys with `resolver`, reading their values in
        one batch and writing the resolutions in another (one round each
        on a network store, not three reads and a write per key)."""
        trio = {k: (our_diff[k][0], our_diff[k][1], their_diff[k][1])
                for k in conflicts}
        refs = list({r for t in trio.values() for r in t if r is not None})
        get_many = getattr(bytes_store, "get_many", None)
        blobs = (get_many(refs) if get_many
                 else {r: bytes_store.get(r) for r in refs})
        decode = (lambda r: _decode_value(blobs[r]) if r is not None
                  else ABSENT)
        kept: Dict[bytes, object] = {}
        for k in conflicts:
            bv, ov, tv = trio[k]
            kept[k] = resolver(k.decode("utf-8"), decode(bv), decode(ov),
                               decode(tv))
        writes = [k for k in conflicts if kept[k] is not DELETE]
        datas = [_encode_value(kept[k]) for k in writes]
        put_many = getattr(bytes_store, "put_many", None)
        new = (put_many(datas) if put_many
               else [bytes_store.put(d) for d in datas])
        refs_of = dict(zip(writes, new))
        changes: Dict[bytes, object] = {}
        for k in conflicts:
            merged = refs_of.get(k)            # None: the resolver deleted it
            if merged != trio[k][0]:           # it could land back on base
                changes[k] = merged if merged is not None else _TOMBSTONE
        return changes


# ---------------------------------------------------------------------------
# Local-first: RecordStore over a swarmfs localstore directory
# ---------------------------------------------------------------------------

class LocalFirstRecordStore(RecordStore):
    """A RecordStore whose backend is a local-first store directory —
    create it with :func:`local_first_store`.

    Commits land on local disk instantly (offline is the normal mode) and
    are recorded in the store directory's journal — the reflog: lineage,
    durability rungs, everything `sync_status()` reports. When opened with
    an ``api_url``, a background syncer pushes commits to Swarm and
    confirms them peer-to-peer; ``sync()`` is the blocking certainty
    barrier ("my data is really out there"). Reads of locally evicted
    blobs heal transparently by verified re-fetch.
    """

    def __init__(self, bytes_store, local, syncer=None, **kw):
        super().__init__(bytes_store, **kw)
        #: The underlying swarmfs LocalStore (journal, budget, pins).
        self.local = local
        #: The background pusher, or None when opened without api_url.
        self.syncer = syncer

    def sync(self, timeout: Optional[float] = None) -> None:
        """Block until every commit is network-confirmed — the fsync of
        the durability ladder. TimeoutError (naming the last sync error)
        if `timeout` passes first."""
        if self.syncer is None:
            raise RuntimeError(
                "this store was opened without api_url — local-only; "
                "reopen with api_url=... to sync to Swarm")
        self.syncer.sync(timeout)

    def sync_status(self):
        """The local-first store's status: bytes pinned (unpushed) vs
        evictable, each root's durability rung, blobs living only on
        Swarm. See swarmfs's ``StoreStatus``."""
        return self.local.status()

    # -- working-set controls (R2) --------------------------------------------

    def _refs_under(self, prefix: str):
        return self._trie.refs_under(self._root,
                                     prefix.encode("utf-8") if prefix
                                     else b"")

    def pin(self, name: str, prefix: str = "") -> int:
        """Hold every blob needed to read the keys under `prefix` — trie
        nodes and values — against eviction, as named pin `name` ("always
        keep users/ on this device"). Pins the *current committed root's*
        subtree; re-pin after commits to track new data (a repeated name
        replaces the earlier pin). Returns the number of pinned blobs;
        `unpin(name)` releases them."""
        refs = [ref for _, ref in self._refs_under(prefix)]
        self.local.pin(name, refs)
        return len(refs)

    def unpin(self, name: str) -> None:
        self.local.unpin(name)

    def fetch(self, prefix: str = "") -> int:
        """Warm-up: materialize locally everything needed to read the keys
        under `prefix` ("make me offline-capable before the flight").
        Walks heal on demand, so this works even when parts of the
        structure itself were evicted. Returns the number of blobs fetched
        back from Swarm; raises `RecordUnavailable`-adjacent errors from
        the backend if the network is down."""
        healed = 0
        for _, ref in self._refs_under(prefix):
            if not self.local.has_local(ref):
                self.local.get(ref)  # verified re-fetch via the fetcher
                healed += 1
        return healed

    # -- publication (R2) -------------------------------------------------------

    def publish(self, pointer: Pointer, remote_name: str = "feed"
                ) -> Optional[Ref]:
        """Point `pointer` (e.g. a `SwarmFeedPointer`) at the newest
        **network-confirmed** root on the current lineage, and record it
        as the remote-tracking root. Publication deliberately follows
        confirmation: a feed must never send readers to content the
        network cannot serve yet. If the head is not confirmed, its
        nearest confirmed ancestor is published (an older-but-servable
        state). Returns the published root, or None when nothing on the
        lineage is confirmed yet."""
        root = self._root
        while root is not None and not self.local.network_confirmed(root):
            try:
                root = self.local.parent_of(root)
            except KeyError:
                return None
        if root is None:
            return None
        if self.local.remote_root(remote_name) != root:
            pointer.set(root)
            self.local.set_remote_root(remote_name, root)
        return root

    # -- history retention (R3) ---------------------------------------------------

    def squash_history(self, gc: bool = True) -> dict:
        """Collapse this replica's history to the current root — the
        explicit retention decision for history that would otherwise stay
        pinned (unpushed old roots are a permanent disk commitment under
        the invariant).

        App-assisted by design: the journal layer is blob-blind, so only
        recordstore — which can walk its trie — knows the tip's full
        reachable set; that set is re-listed and the lineage rebased onto
        it. Afterwards the journal holds ONE root; dropped history's
        exclusive blobs are orphans, deleted when `gc=True`. Swarm keeps
        whatever was already pushed (nothing deletes from Swarm); dropped
        history that was never pushed is gone for good. Returns
        ``{"roots_dropped", "orphans_deleted", "bytes_freed"}``."""
        if self._staged:
            raise ValueError("commit or discard staged changes first")
        root = self._root
        if root is None:
            raise ValueError("empty store: nothing to squash onto")
        if not self.local.has_root(root):
            raise ValueError(f"current root {root[:8]}… is not journaled")
        if not hasattr(self.local, "rebase_root"):
            raise RuntimeError(
                "history squashing needs swarmfs >= 0.6 "
                '(pip install -U "recordstore[local-first-swarm]")')
        nodes, blobs = set(), set()
        for kind, ref in self._refs_under(""):
            blobs.add(ref)
            if kind == "node":
                nodes.add(ref)
        before = len(self.local.status().roots)
        self.local.rebase_root(root, sorted(blobs), structure=sorted(nodes))
        stats = {"roots_dropped": before - 1,
                 "orphans_deleted": 0, "bytes_freed": 0}
        if gc:
            count, freed = self.local.gc_orphans()
            stats["orphans_deleted"], stats["bytes_freed"] = count, freed
        return stats

    # -- lifecycle -----------------------------------------------------------------

    def close(self) -> None:
        if self.syncer is not None:
            self.syncer.stop()
        self.local.close()

    def __enter__(self) -> "LocalFirstRecordStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def local_first_store(path: str, api_url: Optional[str] = None, *,
                      stamp: str = "auto",
                      max_bytes: Optional[int] = None,
                      cache_bytes: int = 64 * 1024 * 1024,
                      addressing: str = "swarm",
                      sync_policy=None, witness=None,
                      publish_pointer: Optional[Pointer] = None,
                      node_cache_size: int = DEFAULT_NODE_CACHE_SIZE
                      ) -> LocalFirstRecordStore:
    """Open (or create) a local-first record store: commits go to local
    disk instantly, a background worker pushes them to Swarm and confirms
    arrival, and local storage behaves as a budgeted working set.

    The one call for the whole arrangement::

        store = local_first_store("~/.myapp/store", "http://localhost:1633")
        store.put("k", {"v": 1})
        store.commit()          # local, instant, offline-safe
        store.sync()            # optional barrier: confirmed on Swarm

    Without ``api_url`` the store is local-only (commits journal as usual
    and push later, when reopened with an ``api_url``). ``max_bytes``
    budgets the directory — unpushed data is pinned and the limit is soft
    for it; only Swarm-confirmed blobs are evicted under pressure, and
    reads of evicted blobs heal by verified re-fetch. ``cache_bytes``
    sizes the in-memory blob cache (0 disables). ``stamp``/``sync_policy``
    /``witness`` pass through to swarmfs's ``BeeRemote``/``Syncer``.

    Head resolution: the journal records lineage; a ``HEAD`` pointer file
    in the directory tracks the current root (needed because canonical
    addressing means returning to a previous state re-uses its old root,
    which the append-only journal deliberately refuses to duplicate).

    Requires swarmfs >= 0.9 (``pip install "recordstore[local-first-swarm]"``).
    Feed publication is not wired here yet — publishing a head to a Swarm
    feed belongs *after* confirmation, and lands with the R2 phase.
    """
    try:
        from swarmfs.localstore import LocalStore
    except ImportError as e:
        raise ImportError(
            "local_first_store needs swarmfs with its localstore module "
            '(>= 0.9): pip install "recordstore[local-first-swarm]"') from e
    if api_url is not None and addressing != "swarm":
        raise ValueError(
            'pushing to Swarm requires addressing="swarm" (the push '
            "asserts the node returns the locally computed reference)")
    local = LocalStore(path, addressing=addressing, max_bytes=max_bytes)
    syncer = None
    if api_url is not None:
        from swarmfs.localsync import BeeRemote, Syncer
        remote = BeeRemote(api_url, stamp=stamp)
        syncer = Syncer(local, remote, sync_policy, witness=witness).start()
    blobs = CachedBytesStore(local, cache_bytes) if cache_bytes else local
    pointer = FilePointer(os.path.join(local.path, "HEAD"))
    root = pointer.get() if pointer.get() is not None else local.latest_root()
    store = LocalFirstRecordStore(blobs, local, syncer, root=root,
                                  pointer=pointer,
                                  node_cache_size=node_cache_size)
    if publish_pointer is not None:
        if syncer is None:
            raise ValueError("publish_pointer needs api_url: publication "
                             "follows network confirmation")
        # Publication rides confirmation: each confirmed rung re-tries the
        # publish (listener exceptions are isolated by the journal layer,
        # so a failed feed update is retried on the next confirmation).
        local.add_listener(
            lambda ev: store.publish(publish_pointer)
            if ev.get("ev") == "confirmed" else None)
    return store
