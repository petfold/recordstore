"""The command line: ``python3 -m recordstore refs ROOT...``.

It prints every blob a store on Swarm needs at the given roots, one
``ref <hex>`` line each, each blob once, for a keeper that re-stamps them
onto a new postage batch. A node that walks a reference walks one blob's
chunk tree and cannot see the trie inside it, so it is the store that lists
its own blobs. The lines are swarm-keep's entries, and a keep-list keeps a
store with an item such as::

    {"name": "my store", "ref": "<root>", "redundancy": 1,
     "walk": ["python3", "-m", "recordstore", "refs", "{ref}"]}

or, for a store behind a feed, ``"feed": {...}`` in place of ``"ref"``: the
walk then runs for every kept version's root. ``redundancy`` is 1 because
`BeeBytesStore` uploads every blob that fits one chunk at level 1.
"""

import argparse
import os
import re
import sys


def main(argv=None, blobs=None) -> int:
    """Run the command line; `blobs` replaces the Bee node (for tests)."""
    p = argparse.ArgumentParser(
        prog="python3 -m recordstore",
        description="recordstore's command line")
    sub = p.add_subparsers(dest="command", required=True)
    refs = sub.add_parser(
        "refs", help="list every blob a store needs at the given roots",
        description="Print `ref <hex>` for every blob (trie node and record) "
                    "the store needs at each ROOT, each blob once, the roots "
                    "first: the entries a keeper such as swarm-keep re-stamps.")
    refs.add_argument("roots", nargs="+", metavar="ROOT",
                      help="a committed root, 64 hex")
    refs.add_argument("--api", default=os.environ.get("SWARM_KEEP_BEE_URL")
                      or "http://localhost:1633",
                      help="the Bee node to read from (default: "
                           "$SWARM_KEEP_BEE_URL, else http://localhost:1633)")
    args = p.parse_args(argv)

    roots = [r.lower() for r in args.roots]
    for r in roots:
        if not re.fullmatch(r"[0-9a-f]{64}", r):
            p.error(f"{r!r} is not a root (64 hex)")

    from .recordstore import BeeBytesStore, RecordStore
    if blobs is None:
        # reads only: a store that never writes never asks for a batch
        blobs = BeeBytesStore(args.api)
    seen = set()
    try:
        for root in roots:
            for _kind, ref in RecordStore.at(root, blobs).refs_under():
                if ref not in seen:
                    seen.add(ref)
                    print("ref", ref)
    except Exception as e:  # one line for the keeper's report, not a traceback
        print(f"recordstore refs: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
