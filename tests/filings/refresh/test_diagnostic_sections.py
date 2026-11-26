"""Every diagnostics section parses into one query.

``make diagnose COMPANY_IDS=...`` and the snapshot scorecard split each file
on its ``\\echo '--- title ---'`` lines. A title the pattern does not match (an
apostrophe in it, say) silently merges two queries into one invalid body.
"""

import re

from filings.refresh.parity.snapshot import DIAGNOSTICS_DIR, diagnostic_queries


def test_every_echo_line_starts_a_section():
    for path in sorted(DIAGNOSTICS_DIR.glob("*.sql")):
        echoes = re.findall(r"^\\echo\b", path.read_text(), re.MULTILINE)
        sections = [q for q in diagnostic_queries() if q[0] == path.name]
        assert len(sections) == len(echoes), path.name


def test_each_section_is_a_single_statement_without_limit():
    for file, title, sql in diagnostic_queries():
        body = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        )
        code = re.sub(r"'(?:[^']|'')*'", "''", body)
        assert ";" not in code, (file, title)
        assert not re.search(r"\bLIMIT\s+\d+\s*$", body, re.IGNORECASE), (file, title)
