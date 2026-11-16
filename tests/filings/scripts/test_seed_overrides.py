"""Tests for financial rewrite CSV parsing."""

from decimal import Decimal

import pytest

from filings.scripts.seed_overrides import (
    SeedError,
    parse_facts_rows,
    parse_rewrite_rows,
)


def test_parse_rewrite_delete_set_and_add(tmp_path):
    """Delete omits a value; set and add require one."""
    path = tmp_path / "financial-rewrites.csv"
    path.write_text(
        "company_id,statement,normalized_label,axis,member,period_end,"
        "granularity,action,value\n"
        "8,Income Statement,Revenue,Geographical,Other,2023-07-30,"
        "quarterly,delete,\n"
        "8,Income Statement,Revenue,Geographical,Singapore,2023-07-30,"
        "yearly,set,1042000000\n"
        "8,Income Statement,Revenue,, ,2023-07-30,quarterly,add,1\n",
        encoding="utf-8",
    )

    rows = parse_rewrite_rows(path)

    assert rows[0]["action"] == "delete"
    assert rows[0]["granularity"] == "quarterly"
    assert rows[0]["value"] is None
    assert rows[0]["member"] == "Other"
    assert rows[1]["granularity"] == "yearly"
    assert rows[1]["value"] == Decimal("1042000000")
    assert rows[2]["axis"] == ""
    assert rows[2]["member"] == ""


def test_parse_rewrite_rejects_delete_with_value(tmp_path):
    """A delete that also sets a number is not an edit."""
    path = tmp_path / "financial-rewrites.csv"
    path.write_text(
        "company_id,statement,normalized_label,axis,member,period_end,"
        "granularity,action,value\n"
        "8,Income Statement,Revenue,Geographical,Other,2023-07-30,"
        "quarterly,delete,1\n",
        encoding="utf-8",
    )

    with pytest.raises(SeedError, match="must not set a value"):
        parse_rewrite_rows(path)


def test_parse_rewrite_rejects_unknown_granularity(tmp_path):
    """Granularity selects the quarterly or yearly layer."""
    path = tmp_path / "financial-rewrites.csv"
    path.write_text(
        "company_id,statement,normalized_label,axis,member,period_end,"
        "granularity,action,value\n"
        "8,Income Statement,Revenue,Geographical,Other,2023-07-30,annual,delete,\n",
        encoding="utf-8",
    )

    with pytest.raises(SeedError, match="granularity"):
        parse_rewrite_rows(path)


def test_parse_facts_clone_keeps_rewrite_as_default(tmp_path):
    """A blank action rewrites the fact. clone copies it onto a dimension."""
    path = tmp_path / "financial-facts-overrides.csv"
    path.write_text(
        "id,company_id,concept,statement,axis,member,label,form_type,"
        "from_period,to_period,to_concept,to_axis,to_member,to_member_label,"
        "to_weight,is_global,action\n"
        "1,5,us-gaap:Revenues,Income Statement,__EMPTY__,__EMPTY__,,,,"
        ",us-gaap:Revenues,,,,,False,\n"
        "2,2,us-gaap:RevenueNotFromContractWithCustomer,Income Statement,"
        "__EMPTY__,__EMPTY__,,,,,us-gaap:Revenues,"
        "us-gaap:StatementBusinessSegmentsAxis,"
        "goog:OtherRevenueHedgingGainLossMember,Hedging gains (losses),,"
        "False,clone\n",
        encoding="utf-8",
    )

    rows = parse_facts_rows(path)

    assert rows[0]["action"] == "rewrite"
    assert rows[0]["axis"] == ""
    assert rows[1]["action"] == "clone"
    assert rows[1]["to_member_label"] == "Hedging gains (losses)"
    assert rows[1]["to_concept"] == "us-gaap:Revenues"


def test_parse_facts_clone_requires_a_dimension(tmp_path):
    """A clone with no target axis would not show up on either cut."""
    path = tmp_path / "financial-facts-overrides.csv"
    path.write_text(
        "id,company_id,concept,statement,to_concept,action\n"
        "1,2,us-gaap:RevenueNotFromContractWithCustomer,Income Statement,"
        "us-gaap:Revenues,clone\n",
        encoding="utf-8",
    )

    with pytest.raises(SeedError, match="clone requires"):
        parse_facts_rows(path)
