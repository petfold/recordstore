#!/usr/bin/env python3
"""Generate the cross-language fixtures the JavaScript port must reproduce.

The Python package is the reference implementation of recordstore's wire
format. This script runs it (the source in ``src/``, not an installed copy)
and writes what it produces to ``js/test/fixtures/``: canonical encodings,
Swarm content addresses, the root after every commit of scripted and random
stores, and three-way merges. The JavaScript tests replay the same operations
and must reach byte-identical bytes and roots.

    python3 js/tools/make_fixtures.py           # rewrite the fixtures
    python3 js/tools/make_fixtures.py --check   # exit 1 if they are stale

Needs swarmfs (with keccak) for the Swarm-addressed cases, as
``addressing="swarm"`` does.
"""

from __future__ import annotations

import json
import os
import random
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
OUT = os.path.join(REPO, "js", "test", "fixtures")
sys.path.insert(0, os.path.join(REPO, "src"))

from recordstore import (  # noqa: E402  (the path insert must come first)
    ABSENT, DELETE, MemoryBytesStore, MergeConflict, RecordStore,
    canonical_bytes,
)
from recordstore.recordstore import _encode_value, _swarm_ref  # noqa: E402


class SwarmMemoryStore:
    """MemoryBytesStore under Swarm (BMT) addressing: the references a Bee
    node gives these blobs, computed offline."""

    def __init__(self):
        self.blobs = {}

    def put(self, data: bytes) -> str:
        ref = _swarm_ref(data)
        self.blobs[ref] = data
        return ref

    def get(self, ref: str) -> bytes:
        try:
            return self.blobs[ref]
        except KeyError:
            raise KeyError(f"reference not found: {ref}") from None


def new_store(addressing: str):
    return SwarmMemoryStore() if addressing == "swarm" else MemoryBytesStore()


# ---------------------------------------------------------------------------
# Canonical encoding
# ---------------------------------------------------------------------------

def canonical_cases():
    """Values whose canonical bytes the JavaScript encoder must match.

    Integral floats below 2**53 (``1.0``) are left out on purpose: JavaScript
    has one number type and writes them as integers (see the JS README).
    """
    strings = [
        "", "plain", 'quote " and backslash \\', "tab\tnewline\ncr\r",
        "controls \x00\x01\x1f\x7f", "\b\f", "é 中文 😀", "  ",
        "￿", "/", "</script>",
    ]
    ints = [0, 1, -1, 42, 2**31, -(2**31) - 1, 2**53 - 1, -(2**53 - 1),
            2**53, 2**63, -(2**70), 10**30]
    floats = [0.1, -2.5, 0.30000000000000004, 1.0000000000000002, 123.456,
              1e-4, 0.0001234, 1e-5, 1e-7, 2.5e-5, 1.5e-300, 5e-324,
              1e15 + 0.5, 1e16, 1e21, 1e22, 1.7976931348623157e308,
              9007199254740992.0, -1e100, 3.14159265358979, 100.5]
    objects = [
        {},
        [],
        {"b": 1, "a": 2},
        {"z": 1, "é": 2, "😀": 3, "￿": 4, "A": 5, "a": 6, "": 7,
         "aa": 8},
        {"nested": {"list": [1, "two", None, True, False, {"x": []}],
                    "empty": {}}},
        [None, True, False, 0, "", [], {}],
        {"users": [{"name": "Alice", "role": "admin"},
                   {"name": "Bob", "tags": ["x", "y"]}]},
    ]
    values = ([None, True, False] + strings + ints + floats + objects)
    return [{"value": v, "hex": canonical_bytes(v).hex()} for v in values]


def value_blob_cases():
    """The record envelope ``{"rsv":1,"val":...}`` around a few values."""
    return [{"value": v, "hex": _encode_value(v).hex()}
            for v in [None, "x", 1, 0.5, {"a": [1, 2]}]]


# ---------------------------------------------------------------------------
# Swarm content addresses
# ---------------------------------------------------------------------------

def pattern(size: int) -> bytes:
    """Deterministic test data; the JS tests rebuild it the same way."""
    return bytes(((i * 31) + 7) & 0xFF for i in range(size))


def swarm_cases():
    sizes = [0, 1, 31, 32, 33, 64, 4095, 4096, 4097, 8192, 8193,
             4096 * 128, 4096 * 128 + 1, 4096 * 129, 4096 * 256 + 5]
    return [{"size": n, "ref": _swarm_ref(pattern(n))} for n in sizes]


# ---------------------------------------------------------------------------
# Stores: scripted and random operation sequences
# ---------------------------------------------------------------------------

def scripted_ops():
    """Shared prefixes, splits and the merges a delete forces."""
    return [
        ("put", "users/alice", {"name": "Alice", "role": "admin"}),
        ("put", "users/bob", {"name": "Bob"}),
        ("put", "users/al", "a proper prefix of alice"),
        ("put", "u", 1),
        ("put", "userz", [1, 2, 3]),
        ("commit",),
        ("delete", "users/al"),
        ("commit",),
        ("put", "users/alice", {"name": "Alice", "role": "admin"}),  # same value
        ("commit",),
        ("put", "é", "two bytes"),
        ("put", "😀", "four bytes"),
        ("put", "￿", "three bytes, sorts before the emoji"),
        ("put", "a/b/c", None),
        ("delete", "u"),
        ("delete", "never-was"),
        ("commit",),
        ("put", "tmp", "staged then deleted"),
        ("delete", "tmp"),
        ("put", "users/bob", {"name": "Bob", "age": 0.5}),
        ("commit",),
        ("delete", "users/alice"),
        ("delete", "users/bob"),
        ("delete", "userz"),
        ("commit",),
    ]


SEGMENTS = ["users", "u", "user", "a", "ab", "abc", "é", "😀", "￿", "x"]
ALPHABET = list("abcxyz019 /\"\\") + ["\n", "\t", "\x01", "é", "中", "😀",
                                       "￿", " "]


def random_key(rng):
    if rng.random() < 0.2:
        return rng.choice(SEGMENTS)
    return "/".join(rng.choice(SEGMENTS) for _ in range(rng.randint(1, 3)))


def random_string(rng):
    return "".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 8)))


def random_float(rng):
    while True:
        f = rng.choice([rng.uniform(-1e6, 1e6), rng.uniform(-1, 1),
                        rng.uniform(1, 10) * 10 ** rng.randint(-12, 25)])
        if not f.is_integer():
            return f


def random_value(rng, depth=0):
    kind = rng.randint(0, 8 if depth < 2 else 5)
    if kind == 0:
        return None
    if kind == 1:
        return rng.random() < 0.5
    if kind == 2:
        return rng.randint(-(2**53 - 1), 2**53 - 1)
    if kind == 3:
        return random_float(rng)
    if kind == 4:
        return random_string(rng)
    if kind == 5:
        return rng.choice([2**60 + rng.randint(0, 999), -(2**64)])
    if kind in (6, 7):
        return {random_string(rng): random_value(rng, depth + 1)
                for _ in range(rng.randint(0, 4))}
    return [random_value(rng, depth + 1) for _ in range(rng.randint(0, 4))]


def random_ops(seed, count):
    rng = random.Random(seed)
    ops, live = [], set()
    for _ in range(count):
        roll = rng.random()
        if roll < 0.6:
            key = random_key(rng)
            ops.append(("put", key, random_value(rng)))
            live.add(key)
        elif roll < 0.85:
            if live and rng.random() < 0.85:
                key = rng.choice(sorted(live))
                live.discard(key)
            else:
                key = random_key(rng)
                live.discard(key)
            ops.append(("delete", key))
        else:
            ops.append(("commit",))
    ops.append(("commit",))
    return ops


def run(ops, addressing):
    """Replay `ops` on the Python store; return the steps as fixture data
    (each commit carrying the root it must produce, each delete whether the
    key existed) and the final store."""
    blobs = new_store(addressing)
    store = RecordStore(blobs)
    steps = []
    for op in ops:
        if op[0] == "put":
            store.put(op[1], op[2])
            steps.append({"op": "put", "key": op[1], "value": op[2]})
        elif op[0] == "delete":
            try:
                store.delete(op[1])
                existed = True
            except KeyError:
                existed = False
            steps.append({"op": "delete", "key": op[1], "existed": existed})
        else:
            steps.append({"op": "commit", "root": store.commit()})
    return steps, store, blobs


def store_cases():
    cases = []
    for name, ops in [("scripted", scripted_ops()),
                      ("random-1", random_ops(1, 400)),
                      ("random-2", random_ops(2, 400))]:
        for addressing in ("sha256", "swarm"):
            steps, store, blobs = run(ops, addressing)
            case = {
                "name": f"{name}/{addressing}",
                "addressing": addressing,
                "steps": steps,
                "items": [[k, v] for k, v in store.items()],
            }
            if name == "scripted":
                # Every blob, so the JS reader is tested on bytes Python wrote.
                case["blobs"] = {ref: data.hex()
                                 for ref, data in sorted(blobs.blobs.items())}
            cases.append(case)
    return cases


# ---------------------------------------------------------------------------
# Three-way merges
# ---------------------------------------------------------------------------

def ours_wins(_key, _base, ours, _theirs):
    """The resolver the JS tests implement identically."""
    return DELETE if ours is ABSENT else ours


def merge_cases():
    base_ops = [("put", f"k{i}", {"n": i}) for i in range(20)] + [
        ("put", "shared/a", 1), ("put", "shared/b", 2), ("commit",)]
    scenarios = [
        ("disjoint",
         [("put", "k1", "ours"), ("delete", "k2"), ("put", "new/ours", 1)],
         [("put", "k3", "theirs"), ("delete", "k4"), ("put", "new/theirs", 2)]),
        ("same-change",
         [("put", "k5", "both"), ("delete", "k6")],
         [("put", "k5", "both"), ("delete", "k6"), ("put", "k7", "theirs")]),
        ("conflict",
         [("put", "k8", "ours"), ("delete", "shared/a"), ("put", "x", 1)],
         [("put", "k8", "theirs"), ("put", "shared/a", 99), ("put", "x", 2)]),
    ]
    cases = []
    for addressing in ("sha256", "swarm"):
        for name, ours_ops, theirs_ops in scenarios:
            blobs = new_store(addressing)
            base_store = RecordStore(blobs)
            for op in base_ops:
                _apply(base_store, op)
            base = base_store.root

            def branch(ops):
                s = RecordStore(blobs, root=base)
                for op in ops:
                    _apply(s, op)
                return s.commit()

            ours, theirs = branch(ours_ops), branch(theirs_ops)
            case = {"name": f"{name}/{addressing}", "addressing": addressing,
                    "base": [_op_json(op) for op in base_ops],
                    "ours": [_op_json(op) for op in ours_ops],
                    "theirs": [_op_json(op) for op in theirs_ops],
                    "roots": {"base": base, "ours": ours, "theirs": theirs}}
            try:
                case["merged"] = RecordStore.merge(blobs, base, ours, theirs)
            except MergeConflict as e:
                case["conflicts"] = sorted(e.conflicts)
                case["merged_ours_wins"] = RecordStore.merge(
                    blobs, base, ours, theirs, resolver=ours_wins)
            cases.append(case)
    return cases


def _apply(store, op):
    if op[0] == "put":
        store.put(op[1], op[2])
    elif op[0] == "delete":
        store.delete(op[1])
    else:
        store.commit()


def _op_json(op):
    if op[0] == "put":
        return {"op": "put", "key": op[1], "value": op[2]}
    if op[0] == "delete":
        return {"op": "delete", "key": op[1]}
    return {"op": "commit"}


# ---------------------------------------------------------------------------
# Feeds: topics, owners, addresses, signed updates (needs swarmfs[feeds])
# ---------------------------------------------------------------------------

KEYS = ["11" * 32, "a7" * 31 + "01"]
TOPIC_NAMES = ["recordstore/test", "loop-twonode-1791730596", "ab" * 32, "é😀"]


def pointer_for(name, key):
    """The Python pointer, for how it hashes topics and derives owners (no
    network is touched until it is used)."""
    from recordstore import SwarmFeedPointer
    return SwarmFeedPointer("http://127.0.0.1:9", name, signer=key)


def signed_update(key, topic_hex, index, root, timestamp):
    """A feed update exactly as swarmfs's FeedOps.update publishes it (its
    own code, with the clock fixed and the upload captured)."""
    import asyncio
    from unittest import mock
    from swarmfs.feeds import FeedOps, FeedSigner, feed_identifier, soc_address

    captured = {}

    class Capture:
        async def soc_post(self, owner, identifier, signature, data, stamp):
            captured.update(owner=owner, identifier=identifier,
                            signature=signature, data=bytes(data))
            return "00" * 32

    signer = FeedSigner(key)
    with mock.patch("swarmfs.feeds.time.time", return_value=float(timestamp)):
        asyncio.run(FeedOps(Capture()).update(
            signer, bytes.fromhex(topic_hex), index, root, "00" * 32))
    identifier = bytes.fromhex(captured["identifier"])
    soc = identifier + bytes.fromhex(captured["signature"]) + captured["data"]
    return {
        "key": key, "topic": topic_hex, "index": index, "root": root,
        "timestamp": timestamp,
        "identifier": captured["identifier"],
        "address": soc_address(feed_identifier(bytes.fromhex(topic_hex), index),
                               signer.owner).hex(),
        "soc": soc.hex(),
    }


def feed_cases():
    from recordstore import FEED_UPDATE_FORMAT, ProofError, verify_equivocation, verify_feed_update
    from swarmfs.feeds import feed_identifier, soc_address

    owners = [{"key": k, "owner": pointer_for("x", k).owner} for k in KEYS]
    topics = [{"name": n, "topic": pointer_for(n, KEYS[0]).topic} for n in TOPIC_NAMES]
    t0 = topics[0]["topic"]
    owner0 = bytes.fromhex(owners[0]["owner"])
    addresses = []
    for index in (0, 1, 255, 2**32 + 5):
        ident = feed_identifier(bytes.fromhex(t0), index)
        addresses.append({"topic": t0, "owner": owners[0]["owner"], "index": index,
                          "identifier": ident.hex(),
                          "address": soc_address(ident, owner0).hex()})

    root_a = "5a" * 32
    root_b = "c3" * 32
    root_64 = "77" * 64  # an encrypted reference: 64 bytes
    updates = [signed_update(KEYS[0], t0, 0, root_a, 1_790_000_000),
               signed_update(KEYS[0], t0, 7, root_b, 1_790_000_123),
               signed_update(KEYS[1], topics[1]["topic"], 3, root_a, 1_800_000_000),
               signed_update(KEYS[0], t0, 7, root_a, 1_790_000_124),   # equivocates with [1]
               signed_update(KEYS[0], t0, 9, root_64, 1_790_000_200)]
    for u in updates:
        owner = pointer_for("x", u["key"]).owner
        envelope = {"format": FEED_UPDATE_FORMAT, "version": 1, "owner": owner,
                    "topic": u["topic"], "index": u["index"], "soc": u["soc"]}
        verified = verify_feed_update(envelope, owner, u["topic"])
        u.update(owner=owner, envelope=envelope,
                 verified={"index": verified.index, "root": verified.root,
                           "timestamp": verified.timestamp})
    equivocation_index = verify_equivocation(updates[1]["envelope"], updates[3]["envelope"],
                                             updates[1]["owner"], t0)
    # Each must be refused by Python, and so by JavaScript.
    bad = []
    for name, tamper in [
        ("signature byte flipped", lambda e: {**e, "soc": e["soc"][:66] + ("00" if e["soc"][66:68] != "00" else "01") + e["soc"][68:]}),
        ("payload byte flipped", lambda e: {**e, "soc": e["soc"][:-2] + ("00" if e["soc"][-2:] != "00" else "01")}),
        ("claims another index", lambda e: {**e, "index": e["index"] + 1}),
        ("claims another owner", lambda e: {**e, "owner": "00" * 20}),
    ]:
        envelope = tamper(updates[0]["envelope"])
        try:
            verify_feed_update(envelope, envelope["owner"], t0)
            raise AssertionError(f"Python accepted a tampered update: {name}")
        except ProofError:
            bad.append({"name": name, "envelope": envelope})
    return {"owners": owners, "topics": topics, "addresses": addresses,
            "updates": updates, "equivocation": {"a": 1, "b": 3, "index": equivocation_index},
            "refused": bad}


# ---------------------------------------------------------------------------

def fixtures():
    note = ("Generated by js/tools/make_fixtures.py from the Python "
            "reference implementation; do not edit by hand.")
    return {
        "canonical.json": {"note": note, "values": canonical_cases(),
                           "records": value_blob_cases()},
        "swarm-addresses.json": {"note": note,
                                 "data": "byte i = (i * 31 + 7) & 0xff",
                                 "cases": swarm_cases()},
        "stores.json": {"note": note, "cases": store_cases()},
        "merges.json": {"note": note, "resolver": "ours wins: DELETE if ours "
                        "is ABSENT, else ours", "cases": merge_cases()},
        "feeds.json": {"note": note, **feed_cases()},
    }


def render(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=1, allow_nan=False) + "\n"


def main(argv):
    check = "--check" in argv
    stale = []
    os.makedirs(OUT, exist_ok=True)
    for name, obj in fixtures().items():
        path = os.path.join(OUT, name)
        text = render(obj)
        if check:
            try:
                with open(path, encoding="utf-8") as f:
                    if f.read() != text:
                        stale.append(name)
            except FileNotFoundError:
                stale.append(name)
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"wrote {os.path.relpath(path, REPO)}")
    if stale:
        print("stale fixtures (rerun js/tools/make_fixtures.py): "
              + ", ".join(stale), file=sys.stderr)
        return 1
    if check:
        print("fixtures are current")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
