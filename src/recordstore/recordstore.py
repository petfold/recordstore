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
from typing import Dict, Iterator, Optional, Protocol, Tuple

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
                 deferred_upload: bool = True):
        import requests  # lazy: only needed for the real backend
        self._requests = requests
        self.api_url = api_url.rstrip("/")
        self.batch = postage_batch_id
        self.deferred = deferred_upload

    def put(self, data: bytes) -> Ref:
        r = self._requests.post(
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

    def get(self, ref: Ref) -> bytes:
        r = self._requests.get(f"{self.api_url}/bytes/{ref}", timeout=120)
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

    # -- node io -----------------------------------------------------------

    def _load(self, ref: Ref) -> _Node:
        node = self._cache.get(ref)
        if node is None:
            obj = json.loads(self._blobs.get(ref).decode("utf-8"))
            if obj.get("tn") != 1:
                raise ValueError("not a trie node or unsupported version")
            node = _Node(
                bytes.fromhex(obj["p"]),
                obj["v"],
                {int(k, 16): v for k, v in obj["c"].items()},
            )
            self._cache[ref] = node
        return node

    def _store(self, node: _Node) -> Ref:
        data = canonical_bytes({
            "tn": 1,
            "p": node.prefix.hex(),
            "v": node.value_ref,
            "c": {format(b, "02x"): r for b, r in sorted(node.children.items())},
        })
        ref = self._blobs.put(data)
        self._cache[ref] = node
        return ref

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
        common = os.path.commonprefix([node.prefix, key])

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
        stack = [(root, b"")]
        out = []
        while stack:
            ref, acc = stack.pop()
            node = self._load(ref)
            full = acc + node.prefix
            # prune subtrees that cannot contain the prefix
            probe = min(len(full), len(prefix))
            if full[:probe] != prefix[:probe]:
                continue
            if node.value_ref is not None and full.startswith(prefix):
                out.append((full, node.value_ref))
            for byte, child in node.children.items():
                stack.append((child, full + bytes([byte])))
        yield from sorted(out)


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
    """Placeholder for the Swarm feed backend.

    A feed update is a signed single-owner chunk: the update must be
    BMT-hashed and secp256k1-signed with the feed owner's key client-side,
    then posted to Bee's /soc/{owner}/{id} endpoint; lookups go through
    GET /feeds/{owner}/{topic}. Doing this properly needs an Ethereum
    signing dependency, so it is deliberately out of scope for this
    stdlib-only first cut. The interface is the contract; swapping this in
    changes nothing above it.

    Implementation plan (when built):
    - Depend on the ``swarm-bee`` package behind a ``recordstore[feeds]``
      extra, not this core. It does the SOC/secp256k1 signing correctly
      (independently verified against a live Bee 2.8.1 node, 2026-07),
      via ``feeds.update_feed_with_reference`` (set) and ``fetch_latest``
      (get). This keeps the core stdlib-only.
    - ``get()`` MUST NOT trust a single feed lookup. On a light node —
      especially over a high-latency link — Bee's feed lookup is unreliable
      per call, in two modes: it returns 404 ("lookup at failed"; ~10/12
      calls in one hotspot measurement) or a *stale early* index instead of
      the latest (see ethersphere/bee#5251). The underlying SOC chunks
      push-sync and are individually retrievable fine (``/chunks`` and
      ``/stewardship`` → 200); it is the *lookup* (which must fetch
      candidate index chunks from the network) that flakes. So ``get()``
      needs retry-until-stable (~15-20 tries with backoff) plus
      read-your-writes caching: after ``set(ref)``, serve ``ref`` from a
      local cache and never round-trip the network for our own write.
    - Pass the cached last-known index to Bee as the ``after`` query hint
      (``GET /feeds/{owner}/{topic}?after=N``) so the lookup starts from
      there instead of probing from scratch — this is what makes retries
      cheap and reliable. swarm-bee's ``fetch_latest`` does NOT currently
      send this hint (probes from scratch every call), so either extend it
      or call the endpoint with the param directly; a candidate upstream
      contribution too. This turns the read-your-writes cache into a lookup
      accelerator, not just a correctness shim.
    - swarmfs already solves this exact Swarm property (``feed_ttl`` +
      immediate self-refresh on own commits, polling for others'); use its
      ``bzzf://`` feed layer as the reference implementation rather than
      reinventing the policy.
    This is a Swarm/light-node characteristic, not a swarm-bee defect — any
    client hits it identically. See the bee-client repo's evaluation for
    the full measurement.
    """

    def __init__(self, *_, **__):
        raise NotImplementedError(
            "Swarm feed pointer requires client-side SOC signing; "
            "see class docstring. Use FilePointer/MemoryPointer meanwhile."
        )


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
        canonical_bytes(value)  # fail fast on non-encodable values
        self._staged[key] = json.loads(canonical_bytes(value))  # detach

    def delete(self, key: str) -> None:
        if self._readonly:
            raise TypeError("read-only snapshot")
        kb = self._check_key(key)
        if key not in self._staged and self._trie.get(self._root, kb) is None:
            raise KeyError(key)
        self._staged[key] = _TOMBSTONE

    def keys(self, prefix: str = "") -> Iterator[str]:
        """Sorted keys under `prefix`, staged overlay included."""
        pb = prefix.encode("utf-8")
        committed = {
            k.decode("utf-8")
            for k, _ in self._trie.items(self._root, pb)
        }
        for key, staged in self._staged.items():
            if not key.startswith(prefix):
                continue
            if staged is _TOMBSTONE:
                committed.discard(key)
            else:
                committed.add(key)
        yield from sorted(committed)

    # -- commit ---------------------------------------------------------------

    def commit(self) -> Optional[Ref]:
        """Flush staged changes; return the new root and update the pointer.

        The root/pointer changes only after every blob write has succeeded,
        so a reader following the pointer sees all of a commit or none of it.
        """
        if self._readonly:
            raise TypeError("read-only snapshot")
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
                vref = self._blobs.put(_encode_value(staged))
                root = self._trie.insert(root, kb, vref)
        self._staged.clear()
        self._root = root
        if self._pointer is not None and root is not None:
            self._pointer.set(root)
        return root
