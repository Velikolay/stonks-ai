"""The concepts and axes the parser treats specially."""

from __future__ import annotations

#: US GAAP filers use either namespace for the standard segment axes.
_GAAP_NAMESPACES = ("srt", "us-gaap")


def _in_gaap_namespaces(*local_names: str) -> frozenset[str]:
    return frozenset(f"{ns}:{name}" for ns in _GAAP_NAMESPACES for name in local_names)


PRODUCT_AXES = _in_gaap_namespaces("ProductOrServiceAxis")

#: Product, geography and reportable-segment cuts. Disclosure tables also
#: carry customer and residual axes; those are not series we keep.
SEGMENT_AXES = (
    PRODUCT_AXES
    | _in_gaap_namespaces(
        "StatementGeographicalAxis",
        "ConsolidationItemsAxis",
        "StatementBusinessSegmentsAxis",
    )
    | {
        "ifrs-full:SegmentsAxis",
        "ifrs-full:GeographicalAreasAxis",
        "ifrs-full:ProductsAndServicesAxis",
    }
)

#: The total revenue line, whatever a filing calls it. A note often tags the
#: same line with a different concept than the face.
TOTAL_REVENUE_CONCEPTS = frozenset(
    {
        "us-gaap:Revenues",
        "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
        "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
        "us-gaap:SalesRevenueNet",
        "us-gaap:SalesRevenueGoodsNet",
        "us-gaap:SalesRevenueServicesNet",
        "ifrs-full:Revenue",
        "ifrs-full:RevenueFromContractsWithCustomers",
    }
)

#: Pieces of that total, kept when they name a member the total does not.
#: An undimensioned pair that adds up to the face total is stored as filed,
#: and a fact override places the lines on the product axis.
COMPONENT_REVENUE_CONCEPTS = frozenset(
    {"us-gaap:AdvertisingRevenue", "us-gaap:OtherSalesRevenueNet"}
)

#: Other sales tagged on a segment member that already carries the total is
#: that segment's residual, stored as its own member.
RESIDUAL_REVENUE_CONCEPT = "us-gaap:OtherSalesRevenueNet"
RESIDUAL_MEMBER = "Other sales"

_TOTAL_REVENUE_LOCAL_NAMES = frozenset(
    c.split(":")[-1].casefold() for c in TOTAL_REVENUE_CONCEPTS
)


def local_name(concept: str) -> str:
    return concept.split(":")[-1]


def is_total_revenue_concept(concept: str) -> bool:
    """A total-revenue line, including an extension that only recapitalizes one.

    S&P Global tags the ASC 606 total as
    ``spgi:RevenuefromContractwithCustomerExcludingAssessedTax``.
    """
    return (
        concept in TOTAL_REVENUE_CONCEPTS
        or local_name(concept).casefold() in _TOTAL_REVENUE_LOCAL_NAMES
    )


def is_revenue_concept(concept: str) -> bool:
    """A total-revenue line or one of its components."""
    return is_total_revenue_concept(concept) or concept in COMPONENT_REVENUE_CONCEPTS


def is_per_share(concept: str, unit: object) -> bool:
    """A per-share amount, by unit or by concept name."""
    text = str(unit or "").casefold()
    if text.endswith("pershare") or text == "usdeps":
        return True
    name = local_name(concept)
    return name.endswith("PerShare") or name.startswith("EarningsPerShare")
