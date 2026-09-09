"""Batch ingest SEC XBRL filings for many tickers.

Wraps :class:`SECXBRLFilingsLoader` with ticker fan-out, retries, rate limiting
and a per-ticker summary so a failure on one ticker cannot abort the run.

By default every available XBRL filing is loaded, which is what a company needs
the first time it is ingested. Filings already stored are skipped before their
XBRL is downloaded, so re-running is cheap and only picks up what is new.

Usage:
    python -m filings.scripts.ingest --tickers AAPL,MSFT --forms 10-K
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Set

from dotenv import load_dotenv
from edgar import set_identity

from filings.db import AsyncFilingsDatabase
from filings.sec_xbrl_filings_loader import SECXBRLFilingsLoader

logger = logging.getLogger(__name__)

DEFAULT_FORMS = "10-K,10-Q"
# Sentinel for --limit: load every XBRL filing EDGAR offers for the company.
LIMIT_ALL = 0
DEFAULT_LIMIT = LIMIT_ALL
DEFAULT_DELAY = 1.0
DEFAULT_RETRIES = 3


class IngestError(Exception):
    """Raised when the ingest run cannot be configured or started."""


def _split_csv_arg(value: str) -> List[str]:
    """Split a comma-separated CLI argument into non-empty stripped items."""
    return [item.strip() for item in value.split(",") if item.strip()]


def _read_tickers_file(path: Path) -> List[str]:
    """Read tickers from a file, one per line, ignoring blanks and # comments."""
    if not path.is_file():
        raise IngestError(f"Tickers file not found: {path}")

    tickers = []
    for line in path.read_text(encoding="utf-8").splitlines():
        entry = line.split("#", 1)[0].strip()
        if entry:
            tickers.append(entry)
    return tickers


def _resolve_tickers(tickers: Optional[str], tickers_file: Optional[str]) -> List[str]:
    """Merge --tickers and --tickers-file into a de-duplicated, ordered list."""
    collected: List[str] = []
    if tickers:
        collected.extend(_split_csv_arg(tickers))
    if tickers_file:
        collected.extend(_read_tickers_file(Path(tickers_file)))

    seen: Set[str] = set()
    resolved = []
    for ticker in collected:
        upper = ticker.upper()
        if upper not in seen:
            seen.add(upper)
            resolved.append(upper)

    if not resolved:
        raise IngestError("No tickers provided; pass --tickers or --tickers-file")
    return resolved


def _configure_edgar_identity() -> None:
    """Set the EDGAR identity required by SEC fair-access rules."""
    identity = os.getenv("EDGAR_IDENTITY")
    if not identity:
        raise IngestError(
            "EDGAR_IDENTITY is not set. SEC requires a contact string on every "
            'request, e.g. EDGAR_IDENTITY="Jane Doe jane@example.com"'
        )
    set_identity(identity)


def _resolve_database_url() -> str:
    """Return the database URL from the environment."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise IngestError("DATABASE_URL is not set (see env.example)")
    return database_url


async def _load_with_retry(
    loader: SECXBRLFilingsLoader,
    ticker: str,
    form: str,
    limit: int,
    override: bool,
    retries: int,
    delay: float,
) -> dict:
    """Load one ticker/form, retrying transient failures with exponential backoff.

    ``load_company_filings`` returns errors rather than raising, so a retry is
    driven by the presence of an ``error`` key in the result.
    """
    result: dict = {}
    for attempt in range(1, retries + 1):
        result = await loader.load_company_filings(
            ticker=ticker,
            form=form,
            limit=limit,
            override=override,
        )
        if "error" not in result:
            return result

        if attempt < retries:
            backoff = delay * (2 ** (attempt - 1))
            logger.warning(
                "Attempt %s/%s failed for %s %s (%s); retrying in %.1fs",
                attempt,
                retries,
                ticker,
                form,
                result["error"],
                backoff,
            )
            await asyncio.sleep(backoff)

    return result


def _summarize(results: Sequence[dict]) -> None:
    """Log a per-ticker/form summary table of the run."""
    header = f"{'TICKER':<14} {'FORM':<8} {'FILINGS':>7} {'FACTS':>9}  STATUS"
    logger.info("Ingest summary:\n%s\n%s", header, "-" * len(header))
    for result in results:
        logger.info(
            "%-14s %-8s %7s %9s  %s",
            result["ticker"],
            result["form"],
            result.get("filings_loaded", 0),
            result.get("total_facts", 0),
            result.get("status", ""),
        )


async def ingest(
    tickers: Sequence[str],
    forms: Sequence[str],
    limit: int,
    override: bool,
    delay: float,
    retries: int,
    refresh: bool,
) -> int:
    """Ingest filings for every ticker/form pair and optionally refresh financials.

    A ``limit`` of :data:`LIMIT_ALL` loads the company's whole XBRL history.

    Returns the number of ticker/form pairs that failed.
    """
    if limit == LIMIT_ALL:
        logger.info("Loading every available XBRL filing per ticker and form")
        limit = sys.maxsize

    database = AsyncFilingsDatabase(_resolve_database_url())
    await database.initialize()
    loader = SECXBRLFilingsLoader(database)

    results: List[dict] = []
    company_ids: Set[int] = set()
    failures = 0

    try:
        for ticker in tickers:
            for form in forms:
                outcome = await _load_with_retry(
                    loader, ticker, form, limit, override, retries, delay
                )
                summary = {"ticker": ticker, "form": form}

                if "error" in outcome:
                    failures += 1
                    summary["status"] = f"FAILED: {outcome['error']}"
                    logger.error("%s %s failed: %s", ticker, form, outcome["error"])
                elif "message" in outcome:
                    # The loader cannot tell "company files no such form" from
                    # "every filing is already stored", so report the outcome
                    # that is true either way rather than its "not found" text.
                    summary["status"] = "no new filings"
                    logger.info("%s %s: %s", ticker, form, outcome["message"])
                else:
                    summary.update(
                        {
                            "filings_loaded": outcome["filings_loaded"],
                            "total_facts": outcome["total_facts"],
                            "status": "ok",
                        }
                    )
                    company_ids.add(outcome["company_id"])

                results.append(summary)
                await asyncio.sleep(delay)

        if refresh and company_ids:
            logger.info("Refreshing financials for company_ids=%s", sorted(company_ids))
            await database.refresh_financials_for_companies(sorted(company_ids))
        elif refresh:
            logger.info("Nothing ingested; skipping refresh")
    finally:
        await database.aclose()

    _summarize(results)
    return failures


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Batch ingest SEC XBRL filings for many tickers."
    )
    parser.add_argument("--tickers", help="Comma-separated tickers, e.g. AAPL,MSFT")
    parser.add_argument("--tickers-file", help="File with one ticker per line")
    parser.add_argument(
        "--forms",
        default=DEFAULT_FORMS,
        help=f"Comma-separated form types (default: {DEFAULT_FORMS})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=(
            "Max filings per ticker/form; "
            f"{LIMIT_ALL} loads every available XBRL filing (default: {LIMIT_ALL})"
        ),
    )
    parser.add_argument(
        "--override",
        action="store_true",
        help="Replace filings that are already stored",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=(
            "Seconds to pause between ticker/form loads to stay under the SEC "
            f"rate limit (default: {DEFAULT_DELAY})"
        ),
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help=f"Attempts per ticker/form (default: {DEFAULT_RETRIES})",
    )
    parser.add_argument(
        "--no-refresh",
        dest="refresh",
        action="store_false",
        help="Skip refresh_financials after ingesting",
    )
    parser.set_defaults(refresh=True)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point. Returns a process exit code."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    load_dotenv()
    args = _parse_args(argv)

    try:
        tickers = _resolve_tickers(args.tickers, args.tickers_file)
        _configure_edgar_identity()
        failures = asyncio.run(
            ingest(
                tickers=tickers,
                forms=_split_csv_arg(args.forms),
                limit=args.limit,
                override=args.override,
                delay=args.delay,
                retries=args.retries,
                refresh=args.refresh,
            )
        )
    except IngestError as e:
        logger.error("%s", e)
        return 2

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
