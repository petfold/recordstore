"""Undo, redo, and looking at where a store has been.

The mechanism was already there and unnamed: a root is content, so every past
state stays readable and going back is *pointing* back. What was missing was the
ability to say "the previous one" — a store knew where it was and not where it
had been. These tests pin the vocabulary that fills that gap:

  H1  history()      - the states this replica has been in, newest first
  H2  undo/redo      - step back and forward; editor semantics, not a rewrite
  H3  abandonment    - committing after an undo drops the redo tail
  H4  checkout       - jump to a named past state, refuse an unknown one
  H5  persistence    - a FilePointer's timeline survives reopening the store
  H6  messages       - a label on a transition, NEVER part of the content
  H7  absence        - a store with no timeline says so instead of pretending
"""

import json

import pytest

from recordstore import (DirBytesStore, FilePointer, MemoryBytesStore,
                         MemoryPointer, RecordStore)


def store(tmp_path=None, keep_history=True):
    if tmp_path is None:
        return RecordStore(MemoryBytesStore(), pointer=MemoryPointer())
    return RecordStore(
        DirBytesStore(str(tmp_path / "blobs")),
        pointer=FilePointer(str(tmp_path / "root"), keep_history=keep_history))


def three(s):
    """Three commits, each adding a key. Returns their roots in order."""
    roots = []
    for name in ("a", "b", "c"):
        s.put(name, {"v": name})
        roots.append(s.commit(message=f"add {name}"))
    return roots


# -- H1 ------------------------------------------------------------------------

def test_history_is_newest_first_and_marks_the_current_state():
    s = store()
    roots = three(s)
    history = s.history()
    assert [v.root for v in history] == list(reversed(roots))
    assert [v.message for v in history] == ["add c", "add b", "add a"]
    assert [v.current for v in history] == [True, False, False]
    assert all(v.at for v in history)          # each records when it happened


def test_history_can_be_limited():
    s = store()
    three(s)
    assert len(s.history(limit=2)) == 2
    assert s.history(limit=1)[0].message == "add c"


def test_every_past_state_is_still_readable():
    # The reason undo needs no data recovery: a root IS the state.
    blobs = MemoryBytesStore()
    s = RecordStore(blobs, pointer=MemoryPointer())
    roots = three(s)
    assert sorted(RecordStore.at(roots[0], blobs).keys()) == ["a"]
    assert sorted(RecordStore.at(roots[1], blobs).keys()) == ["a", "b"]


# -- H2 ------------------------------------------------------------------------

def test_undo_and_redo_walk_the_line():
    s = store()
    roots = three(s)
    assert s.undo() == roots[1]
    assert sorted(s.keys()) == ["a", "b"]
    assert s.undo() == roots[0]
    assert sorted(s.keys()) == ["a"]
    assert s.redo() == roots[1]
    assert s.redo() == roots[2]
    assert sorted(s.keys()) == ["a", "b", "c"]


def test_the_ends_of_the_line_answer_none():
    s = store()
    three(s)
    assert s.redo() is None                    # already at the tip
    s.undo(), s.undo()
    assert s.undo() is None                    # nothing before the first state
    assert sorted(s.keys()) == ["a"]           # ...and it did not move


def test_undo_moves_the_pointer_others_read():
    s = store()
    roots = three(s)
    s.undo()
    assert s._pointer.get() == roots[1]        # a follower sees the older state
    assert s.root == roots[1]


def test_undo_drops_staged_changes():
    # The state you asked for is the state you get.
    s = store()
    roots = three(s)
    s.put("scratch", {"v": 1})
    assert s.status()["staged"] == 1
    s.undo()
    assert s.status()["staged"] == 0
    assert "scratch" not in s.keys()


def test_status_reports_what_is_possible():
    s = store()
    three(s)
    assert s.status() == {"root": s.root, "staged": 0, "readonly": False,
                          "history": 3, "position": 2, "undoable": 2,
                          "redoable": 0}
    s.undo()
    after = s.status()
    assert (after["undoable"], after["redoable"]) == (1, 1)


# -- H3 ------------------------------------------------------------------------

def test_committing_after_an_undo_abandons_the_redo_tail():
    blobs = MemoryBytesStore()
    s = RecordStore(blobs, pointer=MemoryPointer())
    roots = three(s)
    s.undo()                                   # back to a, b
    s.put("d", {"v": "d"})
    forked = s.commit(message="add d")
    assert s.redo() is None, "the abandoned branch is still offered"
    assert [v.root for v in s.history()] == [forked, roots[1], roots[0]]
    # ...but nothing was destroyed: the abandoned root still reads.
    assert sorted(RecordStore.at(roots[2], blobs).keys()) == ["a", "b", "c"]


def test_undo_after_a_fork_walks_the_new_line():
    s = store()
    roots = three(s)
    s.undo()
    s.put("d", {"v": "d"})
    s.commit()
    assert s.undo() == roots[1]
    assert sorted(s.keys()) == ["a", "b"]


# -- H4 ------------------------------------------------------------------------

def test_checkout_jumps_to_a_known_state():
    s = store()
    roots = three(s)
    assert s.checkout(roots[0]) == roots[0]
    assert sorted(s.keys()) == ["a"]
    assert s.redo() == roots[1]                # position moved, line intact


def test_checkout_refuses_a_root_this_store_never_held():
    s = store()
    three(s)
    other = RecordStore(MemoryBytesStore(), pointer=MemoryPointer())
    other.put("elsewhere", {"v": 1})
    foreign = other.commit()
    with pytest.raises(KeyError):
        s.checkout(foreign)
    assert "a" in s.keys()                     # unchanged


# -- H5 ------------------------------------------------------------------------

def test_the_timeline_survives_reopening_the_store(tmp_path):
    s = store(tmp_path)
    roots = three(s)
    del s

    reopened = store(tmp_path)
    assert [v.root for v in reopened.history()] == list(reversed(roots))
    assert reopened.undo() == roots[1]
    assert sorted(reopened.keys()) == ["a", "b"]

    # and the next process sees the undo, not the tip
    again = store(tmp_path)
    assert again.root == roots[1]
    assert again.redo() == roots[2]


def test_the_timeline_is_plain_json_beside_the_ref(tmp_path):
    s = store(tmp_path)
    three(s)
    data = json.loads((tmp_path / "root.timeline").read_text())
    assert data["at"] == 2
    assert [entry["message"] for entry in data["line"]] == \
        ["add a", "add b", "add c"]


def test_history_can_be_turned_off(tmp_path):
    s = store(tmp_path, keep_history=False)
    three(s)
    assert s.history() == []
    assert not (tmp_path / "root.timeline").exists()
    with pytest.raises(TypeError):
        s.undo()


# -- H6 ------------------------------------------------------------------------

def test_a_message_is_not_part_of_the_content():
    """The one place the git analogy breaks, and it is load-bearing.

    A git commit hashes its message, so the same change described differently is
    a different commit. A root here hashes state alone — which is what makes
    equal content converge to one root and merge without conflict."""
    first = RecordStore(MemoryBytesStore(), pointer=MemoryPointer())
    first.put("k", {"v": 1})
    second = RecordStore(MemoryBytesStore(), pointer=MemoryPointer())
    second.put("k", {"v": 1})
    assert first.commit(message="because Alice asked") == \
        second.commit(message="totally different words")


def test_a_commit_needs_no_message():
    s = store()
    s.put("k", {"v": 1})
    s.commit()
    assert s.history()[0].message is None


# -- H7 ------------------------------------------------------------------------

def test_a_store_without_a_timeline_says_so():
    s = RecordStore(MemoryBytesStore())        # no pointer at all
    s.put("k", {"v": 1})
    s.commit()
    assert s.history() == []                   # honest: nothing known
    for call in (s.undo, s.redo):
        with pytest.raises(TypeError) as caught:
            call()
        assert "keeps no history" in str(caught.value)


def test_a_snapshot_has_nothing_to_undo():
    blobs = MemoryBytesStore()
    s = RecordStore(blobs, pointer=MemoryPointer())
    roots = three(s)
    snapshot = RecordStore.at(roots[0], blobs)
    with pytest.raises(TypeError):
        snapshot.undo()


def test_a_pointer_that_keeps_no_timeline_still_commits():
    class Bare:
        def __init__(self):
            self.root = None

        def get(self):
            return self.root

        def set(self, root):
            self.root = root

    bare = Bare()
    s = RecordStore(MemoryBytesStore(), pointer=bare)
    s.put("k", {"v": 1})
    root = s.commit(message="ignored by a bare pointer")
    assert bare.root == root
    assert s.history() == []


def test_a_no_op_commit_is_not_a_state():
    # `odag` commits after every command, including ones that change nothing.
    # Recording those would make undo step from a root to the same root.
    s = store()
    s.put("a", {"v": 1})
    first = s.commit(message="add a")
    s.put("a", {"v": 1})                       # same content
    assert s.commit(message="again") == first
    assert len(s.history()) == 1
    assert s.undo() is None


def test_local_first_stores_get_it_from_their_HEAD(tmp_path):
    # No extra wiring: a local-first store's HEAD *is* a FilePointer.
    swarmfs = pytest.importorskip("swarmfs.localstore")   # noqa: F841
    from recordstore import local_first_store

    with local_first_store(str(tmp_path / "lf"), addressing="sha256") as s:
        s.put("a", {"v": 1})
        first = s.commit(message="add a")
        s.put("b", {"v": 2})
        s.commit(message="add b")
        assert [v.message for v in s.history()] == ["add b", "add a"]
        assert s.undo() == first
        assert sorted(s.keys()) == ["a"]
        assert s.redo() is not None
        assert sorted(s.keys()) == ["a", "b"]

    # ...and it survives reopening, since HEAD and its timeline are on disk
    with local_first_store(str(tmp_path / "lf"), addressing="sha256") as again:
        assert len(again.history()) == 2
        assert again.undo() == first
