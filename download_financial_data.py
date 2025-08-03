#!/usr/bin/env python3
"""
Quarterly Financial Data Downloader using yfinance
Downloads 10 years of Apple (AAPL) quarterly financial data and saves to CSV.
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class FinancialDataDownloader:
    """Download quarterly financial data using yfinance."""

    def __init__(self, symbol: str = "AAPL", years: int = 10):
        """
        Initialize the financial data downloader.

        Args:
            symbol: Stock symbol (default: AAPL for Apple)
            years: Number of years of data to download (default: 10)
        """
        self.symbol = symbol.upper()
        self.years = years
        self.ticker = None
        self.output_dir = Path("data/financial_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def download_financial_data(self) -> dict:
        """
        Download quarterly financial data for the specified symbol.

        Returns:
            Dictionary containing financial data DataFrames
        """
        logger.info(
            f"Downloading {self.years} years of financial data for {self.symbol}"
        )

        try:
            # Initialize ticker
            self.ticker = yf.Ticker(self.symbol)

            # Download financial data
            financial_data = {}

            # Get quarterly financial statements
            logger.info("Downloading quarterly income statements...")
            financial_data["income_statement"] = self.ticker.quarterly_financials

            logger.info("Downloading quarterly balance sheets...")
            financial_data["balance_sheet"] = self.ticker.quarterly_balance_sheet

            logger.info("Downloading quarterly cash flow statements...")
            financial_data["cash_flow"] = self.ticker.quarterly_cashflow

            # Get earnings data
            logger.info("Downloading earnings data...")
            financial_data["earnings"] = self.ticker.quarterly_earnings

            # Get analyst recommendations
            logger.info("Downloading analyst recommendations...")
            financial_data["recommendations"] = self.ticker.recommendations

            # Get institutional holders
            logger.info("Downloading institutional holders...")
            financial_data["institutional_holders"] = self.ticker.institutional_holders

            # Get major holders
            logger.info("Downloading major holders...")
            financial_data["major_holders"] = self.ticker.major_holders

            # Get company info
            logger.info("Downloading company info...")
            financial_data["info"] = self.ticker.info

            logger.info("Financial data download completed successfully")
            return financial_data

        except Exception as e:
            logger.error(f"Error downloading financial data for {self.symbol}: {e}")
            raise

    def save_financial_data(self, financial_data: dict) -> dict:
        """
        Save all financial data to CSV files.

        Args:
            financial_data: Dictionary containing financial data DataFrames

        Returns:
            Dictionary with paths to saved files
        """
        saved_files = {}

        try:
            # Save each financial statement
            for statement_name, data in financial_data.items():
                if data is not None:
                    filename = f"{self.symbol}_{statement_name}.csv"
                    filepath = self.output_dir / filename

                    # Handle different data types
                    if isinstance(data, pd.DataFrame):
                        if not data.empty:
                            data.to_csv(filepath)
                            saved_files[statement_name] = str(filepath)
                            logger.info(f"Saved {statement_name} to {filepath}")
                        else:
                            logger.warning(f"No data available for {statement_name}")
                    elif isinstance(data, dict):
                        # Convert dict to DataFrame for saving
                        df = pd.DataFrame.from_dict(data, orient="index")
                        df.to_csv(filepath)
                        saved_files[statement_name] = str(filepath)
                        logger.info(f"Saved {statement_name} to {filepath}")
                    else:
                        logger.warning(
                            f"Skipping {statement_name} - unsupported data type"
                        )
                else:
                    logger.warning(f"No data available for {statement_name}")

            return saved_files

        except Exception as e:
            logger.error(f"Error saving financial data: {e}")
            raise

    def get_financial_summary(self, financial_data: dict) -> dict:
        """
        Generate a summary of key financial metrics.

        Args:
            financial_data: Dictionary containing financial data

        Returns:
            Dictionary with key financial metrics
        """
        summary = {
            "symbol": self.symbol,
            "company_name": financial_data.get("info", {}).get("longName", "Unknown"),
            "sector": financial_data.get("info", {}).get("sector", "Unknown"),
            "industry": financial_data.get("info", {}).get("industry", "Unknown"),
            "market_cap": financial_data.get("info", {}).get("marketCap", "Unknown"),
            "enterprise_value": financial_data.get("info", {}).get(
                "enterpriseValue", "Unknown"
            ),
            "pe_ratio": financial_data.get("info", {}).get("trailingPE", "Unknown"),
            "forward_pe": financial_data.get("info", {}).get("forwardPE", "Unknown"),
            "price_to_book": financial_data.get("info", {}).get(
                "priceToBook", "Unknown"
            ),
            "dividend_yield": financial_data.get("info", {}).get(
                "dividendYield", "Unknown"
            ),
            "beta": financial_data.get("info", {}).get("beta", "Unknown"),
            "fifty_two_week_change": financial_data.get("info", {}).get(
                "fiftyTwoWeekChange", "Unknown"
            ),
            "fifty_day_average": financial_data.get("info", {}).get(
                "fiftyDayAverage", "Unknown"
            ),
            "two_hundred_day_average": financial_data.get("info", {}).get(
                "twoHundredDayAverage", "Unknown"
            ),
        }

        return summary

    def print_financial_summary(self, summary: dict):
        """Print a formatted summary of financial data."""
        print(f"\n{'='*70}")
        print(f"📊 {summary['symbol']} FINANCIAL DATA SUMMARY")
        print(f"{'='*70}")
        print(f"🏢 Company: {summary['company_name']}")
        print(f"🏭 Sector: {summary['sector']}")
        print(f"🏭 Industry: {summary['industry']}")
        print(f"\n💰 VALUATION METRICS:")
        print(
            f"   Market Cap: {summary['market_cap']:,}"
            if isinstance(summary["market_cap"], (int, float))
            else f"   Market Cap: {summary['market_cap']}"
        )
        print(
            f"   Enterprise Value: {summary['enterprise_value']:,}"
            if isinstance(summary["enterprise_value"], (int, float))
            else f"   Enterprise Value: {summary['enterprise_value']}"
        )
        print(
            f"   P/E Ratio: {summary['pe_ratio']}"
            if summary["pe_ratio"] != "Unknown"
            else f"   P/E Ratio: {summary['pe_ratio']}"
        )
        print(
            f"   Forward P/E: {summary['forward_pe']}"
            if summary["forward_pe"] != "Unknown"
            else f"   Forward P/E: {summary['forward_pe']}"
        )
        print(
            f"   Price to Book: {summary['price_to_book']}"
            if summary["price_to_book"] != "Unknown"
            else f"   Price to Book: {summary['price_to_book']}"
        )
        print(
            f"   Dividend Yield: {summary['dividend_yield']}"
            if summary["dividend_yield"] != "Unknown"
            else f"   Dividend Yield: {summary['dividend_yield']}"
        )
        print(f"\n📈 PERFORMANCE METRICS:")
        print(
            f"   Beta: {summary['beta']}"
            if summary["beta"] != "Unknown"
            else f"   Beta: {summary['beta']}"
        )
        print(
            f"   52-Week Change: {summary['fifty_two_week_change']}"
            if summary["fifty_two_week_change"] != "Unknown"
            else f"   52-Week Change: {summary['fifty_two_week_change']}"
        )
        print(
            f"   50-Day Average: ${summary['fifty_day_average']:.2f}"
            if isinstance(summary["fifty_day_average"], (int, float))
            else f"   50-Day Average: {summary['fifty_day_average']}"
        )
        print(
            f"   200-Day Average: ${summary['two_hundred_day_average']:.2f}"
            if isinstance(summary["two_hundred_day_average"], (int, float))
            else f"   200-Day Average: {summary['two_hundred_day_average']}"
        )
        print(f"{'='*70}\n")


def main():
    """Main function to download Apple's quarterly financial data."""
    print("🍎 Apple Quarterly Financial Data Downloader")
    print("=" * 50)

    try:
        # Create downloader instance
        downloader = FinancialDataDownloader(symbol="AAPL", years=10)

        # Download financial data
        financial_data = downloader.download_financial_data()

        # Generate and print summary
        summary = downloader.get_financial_summary(financial_data)
        downloader.print_financial_summary(summary)

        # Save data to CSV files
        saved_files = downloader.save_financial_data(financial_data)

        print("💾 Saved financial data files:")
        for statement, filepath in saved_files.items():
            print(f"   📄 {statement}: {filepath}")

        print("\n✅ Financial data download and analysis complete!")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
