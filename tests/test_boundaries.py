"""Dependency-boundary test.

recordstore is a generic record store: its module-level imports must stay
stdlib-only. Third-party imports are allowed only lazily inside functions
(BeeChunkStore imports `requests` this way). This keeps `import recordstore`
dependency-free for consumers that only use the in-memory backends.

(Ported from the OntoDAG repo's B2 boundary check when the package was
extracted; the ontodag-specific direction check stayed there.)
"""

import ast
import os
import sys
import unittest

PKG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "src", "recordstore",
)


class TestStdlibOnly(unittest.TestCase):
    def test_module_level_imports_are_stdlib_only(self):
        def imported_tops(node):
            if isinstance(node, ast.Import):
                return [a.name.split(".")[0] for a in node.names]
            if isinstance(node, ast.ImportFrom) and not node.level:
                return [node.module.split(".")[0]]
            return []                        # relative import or not an import

        for fname in sorted(os.listdir(PKG_DIR)):
            if not fname.endswith(".py"):
                continue
            with open(os.path.join(PKG_DIR, fname)) as f:
                tree = ast.parse(f.read(), filename=fname)
            for node in tree.body:           # module level: stdlib only
                for top in imported_tops(node):
                    self.assertIn(
                        top, sys.stdlib_module_names,
                        f"{fname} imports non-stdlib module {top!r} at module "
                        "level; third-party imports must stay lazy",
                    )


if __name__ == "__main__":
    unittest.main()
