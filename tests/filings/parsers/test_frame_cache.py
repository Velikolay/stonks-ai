"""The disk cache of statement tables built while parsing a filing."""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from filings.parsers.sec_xbrl import frame_cache
from filings.parsers.sec_xbrl.frame_cache import cached_frame, filing_frames


def statement(role: str) -> SimpleNamespace:
    return SimpleNamespace(role_or_type=role, _include_dimensions=True)


def counting_build(calls: list, value: int):
    def build() -> pd.DataFrame:
        calls.append(value)
        return pd.DataFrame({"value": [value]})

    return build


def test_a_second_parse_reads_the_tables_back(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("FILINGS_FRAME_CACHE", str(tmp_path))
    calls: list = []
    with filing_frames("0000000000-26-000001"):
        first = cached_frame(
            statement("income"), "dimensions", counting_build(calls, 1)
        )
    with filing_frames("0000000000-26-000001"):
        again = cached_frame(
            statement("income"), "dimensions", counting_build(calls, 2)
        )
        other = cached_frame(
            statement("balance"), "dimensions", counting_build(calls, 3)
        )

    assert calls == [1, 3]
    assert again.equals(first)
    assert other["value"].tolist() == [3]


def test_a_reread_table_is_a_copy_the_caller_cannot_change(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("FILINGS_FRAME_CACHE", str(tmp_path))
    with filing_frames("0000000000-26-000003"):
        first = cached_frame(
            statement("income"),
            "dimensions",
            lambda: pd.DataFrame({"meta": [{"axis": "a"}]}),
        )
        first.loc[0, "meta"]["axis"] = "changed"
        again = cached_frame(statement("income"), "dimensions", lambda: None)
    assert again.loc[0, "meta"] == {"axis": "a"}


def test_only_real_accessions_and_roles_are_cached(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("FILINGS_FRAME_CACHE", str(tmp_path))
    calls: list = []
    with filing_frames("not-an-accession"):
        cached_frame(statement("income"), "plain", counting_build(calls, 1))
    with filing_frames("0000000000-26-000004"):
        cached_frame(SimpleNamespace(), "plain", counting_build(calls, 2))
    assert calls == [1, 2]
    assert list(tmp_path.iterdir()) == []


def test_a_new_frame_version_or_a_disabled_cache_rebuilds(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("FILINGS_FRAME_CACHE", str(tmp_path))
    calls: list = []
    with filing_frames("0000000000-26-000002"):
        cached_frame(statement("income"), "plain", counting_build(calls, 1))
    monkeypatch.setattr(frame_cache, "FRAME_VERSION", frame_cache.FRAME_VERSION + 1)
    with filing_frames("0000000000-26-000002"):
        cached_frame(statement("income"), "plain", counting_build(calls, 2))
    monkeypatch.setenv("FILINGS_FRAME_CACHE", "0")
    with filing_frames("0000000000-26-000002"):
        cached_frame(statement("income"), "plain", counting_build(calls, 3))

    assert calls == [1, 2, 3]


def test_no_cache_outside_a_filing() -> None:
    calls: list = []
    cached_frame(statement("income"), "plain", counting_build(calls, 1))
    cached_frame(statement("income"), "plain", counting_build(calls, 2))
    assert calls == [1, 2]
