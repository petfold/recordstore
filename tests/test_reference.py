"""docs/REFERENCE.md is pinned to the code: if a name, a parameter, or the
export list in that file and the package disagree, this suite fails.

Conventions the reference follows (and this test enforces):
- The §3 Exports table lists exactly ``recordstore.__all__`` — no more,
  no less.
- In every API table, the first column is a resolvable dotted name on the
  package (`RecordStore.commit`, `verify_proof`, …).
- Where the second column is a backticked signature, every parameter name
  in it exists on the real callable (so renames fail loudly).
- The "version this file describes" line matches pyproject.toml.
"""

import inspect
import re
from pathlib import Path

import recordstore

DOC = Path(__file__).parent.parent / "docs" / "REFERENCE.md"
TEXT = DOC.read_text(encoding="utf-8")


def _table_rows(section: str) -> list[list[str]]:
    """The body rows of every markdown table inside a `## section`."""
    m = re.search(rf"^## {re.escape(section)}.*?(?=^## |\Z)", TEXT,
                  re.M | re.S)
    assert m, f"section {section!r} missing from REFERENCE.md"
    rows = []
    for line in m.group(0).splitlines():
        if line.startswith("|") and not re.match(r"^\|[\s\-|]+\|$", line):
            cells = [c.strip() for c in line.strip("|").split("|")]
            rows.append(cells)
    return rows[1:] if rows else []  # drop the header row


def _first_code(cell: str) -> str | None:
    m = re.match(r"`([^`]+)`", cell)
    return m.group(1) if m else None


def test_exports_table_is_exactly_dunder_all():
    documented = {
        _first_code(row[0])
        for row in _table_rows("3. Exports")
        if _first_code(row[0])
    }
    assert documented == set(recordstore.__all__), (
        f"only in docs: {documented - set(recordstore.__all__)}; "
        f"only in __all__: {set(recordstore.__all__) - documented}")


API_SECTIONS = [
    "4. `RecordStore`",
    "5. Bytes stores",
    "6. Pointers",
    "7. Assemblers",
    "8. `LocalFirstRecordStore` (adds to `RecordStore`)",
    "9. Proofs",
]


def _resolve(dotted: str):
    obj = recordstore
    for part in dotted.split("."):
        obj = getattr(obj, part)
    return obj


def _api_rows():
    for section in API_SECTIONS:
        for row in _table_rows(section):
            name = _first_code(row[0])
            if name and re.fullmatch(r"[A-Za-z_][\w.]*", name):
                yield section, name, row


def test_every_documented_name_resolves():
    checked = 0
    for section, name, _ in _api_rows():
        try:
            _resolve(name)
        except AttributeError as e:
            raise AssertionError(
                f"{section}: `{name}` does not resolve on recordstore: {e}"
            ) from None
        checked += 1
    assert checked > 30  # the tables did parse


def test_documented_parameters_exist():
    checked = 0
    for section, name, row in _api_rows():
        sig_cell = _first_code(row[1]) if len(row) > 1 else None
        if not sig_cell or not sig_cell.startswith("("):
            continue
        obj = _resolve(name)
        target = obj.__init__ if inspect.isclass(obj) else obj
        try:
            real = set(inspect.signature(target).parameters)
        except (ValueError, TypeError):
            continue
        real |= {"self"}
        has_kwargs = any(
            p.kind is inspect.Parameter.VAR_KEYWORD
            for p in inspect.signature(target).parameters.values())
        documented = {
            re.split(r"[=:]", p.strip())[0].strip("* ")
            for p in sig_cell.strip("()").split(",") if p.strip()
        }
        for param in documented:
            if not re.fullmatch(r"[A-Za-z_]\w*", param):
                continue  # a literal like "" or 15.0 leaked from a default
            assert has_kwargs or param in real, (
                f"{section}: `{name}` documents parameter {param!r} "
                f"which the code does not have (real: {sorted(real)})")
            checked += 1
    assert checked > 40


def test_described_version_matches_pyproject():
    doc_version = re.search(
        r"version this file describes: `([\d.]+)`", TEXT).group(1)
    pyproject = (Path(__file__).parent.parent / "pyproject.toml").read_text()
    real = re.search(r'^version = "([\d.]+)"', pyproject, re.M).group(1)
    assert doc_version == real, (
        f"REFERENCE.md describes {doc_version}, pyproject says {real} — "
        "update the reference as part of the release docs sweep")


def test_proof_format_constant_matches():
    assert f'`"{recordstore.PROOF_FORMAT}"`' in TEXT
