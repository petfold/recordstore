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


# -- R2: working-set controls ---------------------------------------------------------


def test_pin_by_prefix_survives_eviction_pressure(tmp_path):
    remote = FakeRemote()
    store = synced_store(tmp_path, remote)
    try:
        for i in range(10):
            store.put(f"hot/{i}", {"v": "x" * 100})
            store.put(f"cold/{i}", {"v": "y" * 100})
        store.commit()
        store.sync(timeout=WAIT)
        assert store.pin("keep-hot", "hot/") > 0
        store.local.evict(10**9)             # maximum pressure
        for i in range(10):                  # hot subtree fully local
            assert store.local.has_local(
                store._trie.get(store.root, f"hot/{i}".encode()))
        cold_refs = [store._trie.get(store.root, f"cold/{i}".encode())
                     for i in range(10)]
        assert any(not store.local.has_local(r) for r in cold_refs)
        store.unpin("keep-hot")
        assert store.local.evict(10**9) > 0  # released
    finally:
        store.close()


def test_fetch_warms_a_subtree(tmp_path):
    remote = FakeRemote()
    store = synced_store(tmp_path, remote)
    try:
        for i in range(5):
            store.put(f"trip/{i}", {"doc": "z" * 200})
        store.put("other", {"doc": "w" * 200})
        store.commit()
        store.sync(timeout=WAIT)
        store.local.evict(10**9)
        healed = store.fetch("trip/")        # offline-capable for trip/
        assert healed > 0
        for _, ref in store._refs_under("trip/"):
            assert store.local.has_local(ref)
        assert store.fetch("trip/") == 0     # idempotent: already warm
    finally:
        store.close()


def test_unreachable_record_is_not_a_keyerror(tmp_path):
    from recordstore import RecordUnavailable

    local = LocalStore(str(tmp_path / "store"), addressing="sha256")
    store = LocalFirstRecordStore(CachedBytesStore(local, 0 or 1), local,
                                  None)
    try:
        store.put("k", {"v": "data" * 50})
        store.commit()
        # confirm without a fetcher, then evict: exists, on Swarm, offline
        for root, _ in local.roots_below(CONFIRMED):
            local.mark_confirmed(root, ttl=None)
        local.evict(10**9)
        with pytest.raises(RecordUnavailable):
            store.get("k")
        with pytest.raises(RecordUnavailable):
            store.contains("k")              # never a silent False
        with pytest.raises(KeyError):
            store.get("truly-missing")       # real absence stays KeyError
    finally:
        store.close()


# -- R2: publication follows confirmation ----------------------------------------------


def test_publish_only_confirmed_roots(tmp_path):
    from recordstore import MemoryPointer as MP

    remote = FakeRemote()
    remote.fail = True
    store = synced_store(tmp_path, remote)
    feed = MP()
    try:
        store.put("k", 1)
        store.commit()
        assert store.publish(feed) is None   # nothing confirmed: no publish
        assert feed.get() is None
        remote.fail = False
        store.sync(timeout=WAIT)
        published = store.publish(feed)
        assert published == store.root == feed.get()
        assert store.local.remote_root("feed") == published
    finally:
        store.close()


def test_publish_falls_back_to_confirmed_ancestor(tmp_path):
    from recordstore import MemoryPointer as MP

    remote = FakeRemote()
    store = synced_store(tmp_path, remote)
    feed = MP()
    try:
        store.put("k", 1)
        r1 = store.commit()
        store.sync(timeout=WAIT)
        remote.fail = True                   # offline again
        store.put("k", 2)
        r2 = store.commit()
        assert store.publish(feed) == r1     # head r2 unconfirmed: ancestor
        assert feed.get() == r1 and store.root == r2
    finally:
        store.close()


def test_auto_publish_rides_confirmation(tmp_path):
    from recordstore import MemoryPointer as MP

    pytest.importorskip("swarmfs.localsync")
    feed = MP()
    # wire manually (local_first_store would need a real api_url)
    remote = FakeRemote()
    store = synced_store(tmp_path, remote)
    store.local.add_listener(
        lambda ev: store.publish(feed) if ev.get("ev") == "confirmed"
        else None)
    try:
        store.put("k", 1)
        store.commit()
        store.sync(timeout=WAIT)
        assert feed.get() == store.root     # published without being asked
    finally:
        store.close()


# -- R3: history retention ---------------------------------------------------------------


def test_squash_history_drops_lineage_and_frees_disk(tmp_path):
    remote = FakeRemote()
    store = synced_store(tmp_path, remote)
    try:
        for i in range(5):
            store.put("churn", {"generation": i, "pad": "p" * 200})
            store.commit()                   # each overwrites the last
        store.sync(timeout=WAIT)
        assert len(store.sync_status().roots) == 5
        stats = store.squash_history()
        assert stats["roots_dropped"] == 4
        assert stats["orphans_deleted"] > 0 and stats["bytes_freed"] > 0
        st = store.sync_status()
        assert list(st.roots) == [store.root]
        assert store.get("churn")["generation"] == 4  # data intact
    finally:
        store.close()


def test_squash_of_unpushed_history_then_push(tmp_path):
    """Dropped intermediates are never pushed: squash offline history to
    the tip, reconnect, and the remote receives exactly the tip's
    reachable set."""
    remote = FakeRemote()
    remote.fail = True
    store = synced_store(tmp_path, remote)
    try:
        for i in range(4):
            store.put("doc", {"rev": i, "pad": "q" * 300})
            store.commit()
        stats = store.squash_history()
        assert stats["roots_dropped"] == 3
        remote.fail = False
        store.sync(timeout=WAIT)
        assert store.local.network_confirmed(store.root)
        reachable = {ref for _, ref in store._refs_under("")}
        assert set(remote.blobs) == reachable  # tip only, no dropped revs
        assert store.get("doc")["rev"] == 3
    finally:
        store.close()


def test_squash_refuses_staged_changes(tmp_path):
    with local_first_store(str(tmp_path / "s"),
                           addressing="sha256") as store:
        store.put("k", 1)
        store.commit()
        store.put("k", 2)                    # staged, uncommitted
        with pytest.raises(ValueError, match="staged"):
            store.squash_history()


def test_squash_reopen_resumes_correctly(tmp_path):
    path = str(tmp_path / "s")
    with local_first_store(path, addressing="sha256") as store:
        store.put("a", 1)
        store.commit()
        store.put("b", 2)
        root = store.commit()
        store.squash_history()
    with local_first_store(path, addressing="sha256") as store2:
        assert store2.root == root
        assert store2.get("a") == 1 and store2.get("b") == 2
        assert list(store2.sync_status().roots) == [root]
