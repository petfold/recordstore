"""R1 of the local-first track: RecordStore over a swarmfs localstore.

Commits land locally and are journaled with their exact new-blob lists;
the background syncer pushes and confirms; pulling the network cable
mid-workload never blocks a commit (the acceptance criterion). Runs only
where swarmfs's localstore modules are importable.
"""

import time

import pytest

pytest.importorskip("swarmfs.localstore")

from swarmfs.localstore import CONFIRMED, LocalStore  # noqa: E402
from swarmfs.localsync import Syncer, SyncPolicy  # noqa: E402

from recordstore import (  # noqa: E402
    CachedBytesStore,
    DirBytesStore,
    LocalFirstRecordStore,
    MemoryPointer,
    RecordStore,
    local_first_store,
)

WAIT = 10


class FakeRemote:
    """Protocol twin of swarmfs.localsync.BeeRemote."""

    def __init__(self):
        self.blobs = {}
        self.fail = False

    def push_blob(self, ref, data, deferred=True):
        if self.fail:
            raise ConnectionError("cable pulled")
        self.blobs[ref] = data

    def fetch(self, ref):
        return self.blobs[ref]

    def is_retrievable(self, ref):
        return not self.fail and ref in self.blobs

    def batch_info(self):
        return "fakebatch", 1e9


def synced_store(tmp_path, remote, **policy_kw):
    policy_kw.setdefault("debounce", 0.0)
    policy_kw.setdefault("max_staleness", 0.1)
    policy_kw.setdefault("backoff_base", 0.02)
    policy_kw.setdefault("backoff_max", 0.05)
    policy_kw.setdefault("confirm_sample", 1.0)
    local = LocalStore(str(tmp_path / "store"), addressing="sha256")
    syncer = Syncer(local, remote, SyncPolicy(**policy_kw)).start()
    return LocalFirstRecordStore(
        CachedBytesStore(local, 1 << 20), local, syncer,
        root=local.latest_root())


# -- offline local-first ---------------------------------------------------------


def test_commit_is_journaled_and_survives_reopen(tmp_path):
    path = str(tmp_path / "s")
    with local_first_store(path, addressing="sha256") as store:
        store.put("greeting", {"hello": "world"})
        store.put("answer", 42)
        root = store.commit()
        st = store.sync_status()
        assert st.roots[root] == "committed"
        assert st.pinned_bytes > 0 and st.evictable_bytes == 0

    with local_first_store(path, addressing="sha256") as store2:
        assert store2.root == root          # HEAD survived the reopen
        assert store2.get("answer") == 42
        assert store2.local.parent_of(root) is None  # reflog: first commit


def test_journal_records_lineage_and_structure(tmp_path):
    with local_first_store(str(tmp_path / "s"),
                           addressing="sha256") as store:
        store.put("a", 1)
        r1 = store.commit()
        store.put("b", 2)
        r2 = store.commit()
        assert store.local.parent_of(r2) == r1
        state = store.local.roots_below(CONFIRMED)
        by_root = dict(state)
        # every commit listed blobs, and classified its trie nodes
        assert by_root[r2].blobs and by_root[r2].structure
        # value blobs are payload, not structure
        assert set(by_root[r2].structure) < set(by_root[r2].blobs)


def test_returning_to_a_previous_state_keeps_head_right(tmp_path):
    """Canonical addressing: re-creating an earlier state re-uses its old
    root, which the journal refuses to duplicate — the HEAD pointer keeps
    the store opening at the right place anyway."""
    path = str(tmp_path / "s")
    with local_first_store(path, addressing="sha256") as store:
        store.put("k", "one")
        r_one = store.commit()
        store.put("k", "two")
        r_two = store.commit()
        store.put("k", "one")
        assert store.commit() == r_one      # canonicity brought it back
        assert store.local.latest_root() == r_two  # journal: no duplicate

    with local_first_store(path, addressing="sha256") as store2:
        assert store2.root == r_one          # HEAD wins over latest_root
        assert store2.get("k") == "one"


def test_offline_store_refuses_sync(tmp_path):
    with local_first_store(str(tmp_path / "s"),
                           addressing="sha256") as store:
        with pytest.raises(RuntimeError, match="without api_url"):
            store.sync()


# -- the acceptance criterion: pull the cable ---------------------------------------


def test_cable_pulled_mid_workload(tmp_path):
    remote = FakeRemote()
    store = synced_store(tmp_path, remote)
    try:
        store.put("before", 1)
        r1 = store.commit()
        store.sync(timeout=WAIT)             # healthy: confirmed

        remote.fail = True                   # ── cable pulled ──
        for i in range(5):
            store.put(f"offline/{i}", {"n": i})
            assert store.commit()            # commits keep succeeding
        assert store.get("offline/3") == {"n": 3}
        st = store.sync_status()
        assert sum(1 for r in st.roots.values() if r != CONFIRMED) == 5

        remote.fail = False                  # ── reconnected ──
        store.sync(timeout=WAIT)
        st = store.sync_status()
        assert all(r == CONFIRMED for r in st.roots.values())
        assert st.pinned_bytes == 0          # everything now evictable
    finally:
        store.close()


def test_evicted_records_heal_through_the_syncer(tmp_path):
    remote = FakeRemote()
    store = synced_store(tmp_path, remote)
    try:
        store.put("k", {"payload": "x" * 500})
        store.commit()
        store.sync(timeout=WAIT)
        assert store.local.evict(10**9) > 0  # drop everything evictable
        # cold read: cache may hold it — clear the wrapper to force disk
        store._blobs._cache.clear()
        store._blobs._bytes = 0
        assert store.get("k") == {"payload": "x" * 500}  # verified re-fetch
    finally:
        store.close()


def test_reconcile_through_journal_backend(tmp_path):
    """Two writers over one local store: reconcile merges and the merge's
    blobs are journaled too (recorded through the commit's recorders)."""
    remote = FakeRemote()
    local = LocalStore(str(tmp_path / "store"), addressing="sha256")
    pointer = MemoryPointer()
    a = LocalFirstRecordStore(CachedBytesStore(local, 1 << 20), local,
                              None, pointer=pointer)
    b = LocalFirstRecordStore(CachedBytesStore(local, 1 << 20), local,
                              None, pointer=pointer)
    try:
        a.put("base", 0)
        a.commit()
        b._root = pointer.get()
        a.put("from/a", 1)
        b.put("from/b", 2)
        a.commit(reconcile=True)
        merged_root = b.commit(reconcile=True)
        m = RecordStore.at(merged_root, CachedBytesStore(local, 1 << 20))
        assert m.get("from/a") == 1 and m.get("from/b") == 2
        assert local.has_root(merged_root)   # the merge result is journaled
    finally:
        local.close()


# -- no local regression -------------------------------------------------------------


def test_commit_latency_close_to_dirbytesstore(tmp_path):
    """R1 acceptance: a localstore-backed commit at default durability
    (commit-boundary fsync batching) is not meaningfully slower than
    DirBytesStore for a many-small-node commit. Generous bound — this
    guards against per-put fsync regressions, not micro-variance."""
    def timed_commit(store):
        for i in range(300):
            store.put(f"key/{i:04d}", {"v": i})
        t0 = time.perf_counter()
        store.commit()
        return time.perf_counter() - t0

    baseline = timed_commit(RecordStore(DirBytesStore(
        str(tmp_path / "plain"), addressing="sha256")))
    with local_first_store(str(tmp_path / "lf"),
                           addressing="sha256") as store:
        local_first = timed_commit(store)
    assert local_first < max(baseline * 5, baseline + 1.0), \
        f"local-first commit {local_first:.3f}s vs DirBytesStore {baseline:.3f}s"
