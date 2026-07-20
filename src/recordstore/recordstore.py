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
  Pointer     : mutable "latest root" (Memory / File; Swarm feed = follow-up)

Nothing above this layer should ever see a stored blob or a trie node.
"""

from __future__ import annotations

import json
import hashlib
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterable, Iterator, List, Optional, Protocol, Tuple

Ref = str  # hex-encoded reference to a stored blob

_SCHEMA_VERSION = 1
_TOMBSTONE = object()


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


class BeeBytesStore:
    """BytesStore over a Bee node's `/bytes` endpoint.

    Named for the endpoint it actually uses: `/bytes` is Bee's blob-level
    API, not the raw `/chunks/{address}` single-chunk primitive. Values of
    any length are handled transparently — Bee's splitter turns the payload
    into a chunk tree server-side and returns one reference. Requires a
    usable postage batch id for writes.
    """

    def __init__(self, api_url: str, postage_batch_id: str,
                 deferred_upload: bool = True, max_concurrent_reads: int = 16):
        import requests  # lazy: only needed for the real backend
        self.api_url = api_url.rstrip("/")
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


class _Trie:
    def __init__(self, bytes_store: BytesStore):
        self._blobs = bytes_store
        self._cache: Dict[Ref, _Node] = {}  # nodes are immutable => safe
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
        missing = list({r for r in refs if r not in self._cache})
        if missing:
            get_many = getattr(self._blobs, "get_many", None)
            blobs = (get_many(missing) if get_many
                     else {r: self._blobs.get(r) for r in missing})
            for r in missing:
                self._cache[r] = self._decode(blobs[r])
        return {r: self._cache[r] for r in refs}

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

class RecordStore:
    """Staged, versioned key->record store over a BytesStore.

    Reads are read-your-writes (staged changes shadow the committed trie).
    `commit()` flushes staged changes and returns the new root reference;
    `RecordStore.at(root, bytes_store)` opens a read-only snapshot of any root.
    Returned records are deep copies: mutating them never mutates the store.
    """

    def __init__(self, bytes_store: BytesStore, root: Optional[Ref] = None,
                 pointer: Optional[Pointer] = None, _readonly: bool = False):
        self._blobs = bytes_store
        self._trie = _Trie(bytes_store)
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

    def commit(self) -> Optional[Ref]:
        """Flush staged changes; return the new root and update the pointer.

        The root/pointer changes only after every blob write has succeeded,
        so a reader following the pointer sees all of a commit or none of it.
        """
        if self._readonly:
            raise TypeError("read-only snapshot")
        # 1. Value blobs are independent — write them all up front, concurrently
        #    if the store supports it, instead of one serial round trip each.
        writes = [(k, self._staged[k]) for k in sorted(self._staged)
                  if self._staged[k] is not _TOMBSTONE]
        put_many = getattr(self._blobs, "put_many", None)
        datas = [_encode_value(v) for _, v in writes]
        refs = put_many(datas) if put_many is not None else [self._blobs.put(d) for d in datas]
        vref = {k: r for (k, _), r in zip(writes, refs)}

        # 2. Build the new trie with node writes buffered, then flush only the
        #    surviving nodes bottom-up, one concurrent batch per level. (Node
        #    writes must stay bottom-up: a parent's ref is the backend-assigned
        #    hash of its children, so children come first.)
        self._trie._buffering = True
        try:
            root = self._root
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
            root = self._trie._flush(root)
        finally:
            self._trie._reset_buffer()

        self._staged.clear()
        self._root = root
        if self._pointer is not None and root is not None:
            self._pointer.set(root)
        return root
