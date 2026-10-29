"""``python3 -m recordstore refs``: the blobs a keeper re-stamps.

Every blob the store needs at a root, each once, so that a store on Swarm
outlives its postage batch when a keeper (swarm-keep) re-stamps the lines.
"""

import contextlib
import io
import os
import subprocess
import sys

import pytest

from recordstore import MemoryBytesStore, RecordStore
from recordstore.__main__ import main


def run(argv, blobs):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(argv, blobs=blobs)
    return code, out.getvalue().splitlines(), err.getvalue()


def store_with(blobs, records):
    s = RecordStore(blobs)
    for k, v in records.items():
        s.put(k, v)
    return s.commit()


def test_every_blob_once_root_first():
    blobs = MemoryBytesStore()
    # two keys holding one value share its blob
    root = store_with(blobs, {f"k/{i:03}": {"n": i % 7} for i in range(200)})
    code, lines, _ = run(["refs", root], blobs)
    assert code == 0
    refs = [line.split() for line in lines]
    assert all(kind == "ref" for kind, _ in refs)
    got = [ref for _, ref in refs]
    assert got[0] == root
    assert len(got) == len(set(got))
    want = {ref for _, ref in RecordStore.at(root, blobs).refs_under()}
    assert set(got) == want
    # a store is the blobs it needs: nothing else was written
    assert want == set(blobs.blobs)


def test_two_roots_share_their_blobs():
    blobs = MemoryBytesStore()
    a = store_with(blobs, {f"k/{i:03}": i for i in range(100)})
    s = RecordStore(blobs, root=a)
    s.put("k/new", "one more")
    b = s.commit()
    _, only_b, _ = run(["refs", b], blobs)
    _, both, _ = run(["refs", a, b], blobs)
    assert set(both) == {f"ref {r}" for r in blobs.blobs}
    assert len(both) < len(only_b) * 2
    assert both[0] == f"ref {a}"


def test_a_missing_node_fails():
    # The walk reads trie nodes, not records: a lost record is the node's
    # to find when the keeper checks, a lost node hides what is under it.
    blobs = MemoryBytesStore()
    root = store_with(blobs, {f"k/{i:03}": i for i in range(50)})
    lost = next(r for kind, r in RecordStore.at(root, blobs).refs_under()
                if kind == "node" and r != root)
    del blobs.blobs[lost]
    code, _, err = run(["refs", root], blobs)
    assert code == 1
    assert err.startswith("recordstore refs: KeyError") and lost in err


def test_refuses_what_is_not_a_root(capsys):
    with pytest.raises(SystemExit) as e:
        main(["refs", "abc"], blobs=MemoryBytesStore())
    assert e.value.code == 2
    assert "is not a root" in capsys.readouterr().err


def test_refs_under_a_prefix():
    blobs = MemoryBytesStore()
    root = store_with(blobs, {"a/1": 1, "a/2": 2, "b/1": 3})
    snap = RecordStore.at(root, blobs)
    values = {ref for kind, ref in snap.refs_under("a/") if kind == "value"}
    assert len(values) == 2
    with pytest.raises(TypeError):
        snap.refs_under(b"a/")


def test_runs_as_a_module():
    import recordstore
    src = os.path.dirname(os.path.dirname(recordstore.__file__))
    env = dict(os.environ, PYTHONPATH=src)
    r = subprocess.run([sys.executable, "-m", "recordstore", "refs", "--help"],
                       capture_output=True, text=True, check=True, env=env)
    assert "SWARM_KEEP_BEE_URL" in r.stdout
