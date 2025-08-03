#!/usr/bin/env python3
"""
Stock Data Downloader using yfinance
Downloads 10 years of Apple (AAPL) stock data and provides basic analysis.
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import yfinance as yf

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class StockDataDownloader:
    """Download and analyze stock data using yfinance."""

    def __init__(self, symbol: str = "AAPL", years: int = 10):
        """
        Initialize the stock data downloader.

        Args:
            symbol: Stock symbol (default: AAPL for Apple)
            years: Number of years of data to download (default: 10)
        """
        self.symbol = symbol.upper()
        self.years = years
        self.data = None
        self.output_dir = Path("data/stock_data")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def download_data(self) -> pd.DataFrame:
        """
        Download stock data for the specified symbol and time period.

        Returns:
            DataFrame containing the stock data
        """
        logger.info(f"Downloading {self.years} years of data for {self.symbol}")

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.years * 365)

        try:
            # Download data
            ticker = yf.Ticker(self.symbol)
            self.data = ticker.history(start=start_date, end=end_date, interval="1d")

            if self.data.empty:
                raise ValueError(f"No data found for {self.symbol}")

            logger.info(f"Successfully downloaded {len(self.data)} days of data")
            logger.info(
                f"Date range: {self.data.index[0].date()} to {self.data.index[-1].date()}"
            )

            return self.data

        except Exception as e:
            logger.error(f"Error downloading data for {self.symbol}: {e}")
            raise

    def save_data(self, filename: str = None) -> str:
        """
        Save the downloaded data to CSV file.

        Args:
            filename: Optional custom filename

        Returns:
            Path to the saved file
        """
        if self.data is None:
            raise ValueError("No data to save. Run download_data() first.")

        if filename is None:
            filename = f"{self.symbol}_10y_data.csv"

        filepath = self.output_dir / filename

        try:
            self.data.to_csv(filepath)
            logger.info(f"Data saved to {filepath}")
            return str(filepath)
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            raise

    def get_basic_stats(self) -> dict:
        """
        Calculate basic statistics for the stock data.

        Returns:
            Dictionary containing basic statistics
        """
        if self.data is None:
            raise ValueError("No data available. Run download_data() first.")

        stats = {
            "symbol": self.symbol,
            "total_days": len(self.data),
            "date_range": {
                "start": self.data.index[0].date(),
                "end": self.data.index[-1].date(),
            },
            "price_stats": {
                "current_price": self.data["Close"].iloc[-1],
                "highest_price": self.data["High"].max(),
                "lowest_price": self.data["Low"].min(),
                "avg_price": self.data["Close"].mean(),
            },
            "volume_stats": {
                "avg_volume": self.data["Volume"].mean(),
                "max_volume": self.data["Volume"].max(),
                "total_volume": self.data["Volume"].sum(),
            },
            "returns": {
                "total_return": (
                    (self.data["Close"].iloc[-1] / self.data["Close"].iloc[0]) - 1
                )
                * 100,
                "annualized_return": self._calculate_annualized_return(),
            },
        }

        return stats

    def _calculate_annualized_return(self) -> float:
        """Calculate annualized return percentage."""
        if len(self.data) < 2:
            return 0.0

        total_return = (self.data["Close"].iloc[-1] / self.data["Close"].iloc[0]) - 1
        years = len(self.data) / 252  # Approximate trading days per year
        annualized_return = ((1 + total_return) ** (1 / years) - 1) * 100
        return annualized_return

    def plot_price_history(self, save_plot: bool = True) -> str:
        """
        Create a plot of the stock price history.

        Args:
            save_plot: Whether to save the plot to file

        Returns:
            Path to the saved plot file (if saved)
        """
        if self.data is None:
            raise ValueError("No data available. Run download_data() first.")

        # Set up the plot
        plt.figure(figsize=(15, 8))

        # Plot closing prices
        plt.plot(self.data.index, self.data["Close"], linewidth=1, alpha=0.8)

        # Customize the plot
        plt.title(
            f"{self.symbol} Stock Price - Last {self.years} Years",
            fontsize=16,
            fontweight="bold",
        )
        plt.xlabel("Date", fontsize=12)
        plt.ylabel("Price (USD)", fontsize=12)
        plt.grid(True, alpha=0.3)

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)

        # Add current price annotation
        current_price = self.data["Close"].iloc[-1]
        plt.annotate(
            f"Current: ${current_price:.2f}",
            xy=(self.data.index[-1], current_price),
            xytext=(10, 10),
            textcoords="offset points",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7),
            arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=0"),
        )

        plt.tight_layout()

        if save_plot:
            plot_filename = f"{self.symbol}_price_history.png"
            plot_path = self.output_dir / plot_filename
            plt.savefig(plot_path, dpi=300, bbox_inches="tight")
            logger.info(f"Plot saved to {plot_path}")
            plt.close()
            return str(plot_path)
        else:
            plt.show()
            return ""

    def plot_volume_and_price(self, save_plot: bool = True) -> str:
        """
        Create a plot showing both volume and price.

        Args:
            save_plot: Whether to save the plot to file

        Returns:
            Path to the saved plot file (if saved)
        """
        if self.data is None:
            raise ValueError("No data available. Run download_data() first.")

        # Create subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)

        # Plot 1: Price
        ax1.plot(
            self.data.index, self.data["Close"], linewidth=1, color="blue", alpha=0.8
        )
        ax1.set_title(
            f"{self.symbol} Stock Analysis - Last {self.years} Years",
            fontsize=16,
            fontweight="bold",
        )
        ax1.set_ylabel("Price (USD)", fontsize=12)
        ax1.grid(True, alpha=0.3)

        # Plot 2: Volume
        ax2.bar(self.data.index, self.data["Volume"], alpha=0.6, color="green")
        ax2.set_xlabel("Date", fontsize=12)
        ax2.set_ylabel("Volume", fontsize=12)
        ax2.grid(True, alpha=0.3)

        # Rotate x-axis labels
        plt.xticks(rotation=45)

        plt.tight_layout()

        if save_plot:
            plot_filename = f"{self.symbol}_volume_price.png"
            plot_path = self.output_dir / plot_filename
            plt.savefig(plot_path, dpi=300, bbox_inches="tight")
            logger.info(f"Plot saved to {plot_path}")
            plt.close()
            return str(plot_path)
        else:
            plt.show()
            return ""

    def print_summary(self):
        """Print a summary of the downloaded data."""
        if self.data is None:
            print("No data available. Run download_data() first.")
            return

        stats = self.get_basic_stats()

        print(f"\n{'='*60}")
        print(f"📊 {self.symbol} STOCK DATA SUMMARY")
        print(f"{'='*60}")
        print(
            f"📅 Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}"
        )
        print(f"📈 Total Days: {stats['total_days']}")
        print(f"\n💰 PRICE STATISTICS:")
        print(f"   Current Price: ${stats['price_stats']['current_price']:.2f}")
        print(f"   Highest Price: ${stats['price_stats']['highest_price']:.2f}")
        print(f"   Lowest Price: ${stats['price_stats']['lowest_price']:.2f}")
        print(f"   Average Price: ${stats['price_stats']['avg_price']:.2f}")
        print(f"\n📊 VOLUME STATISTICS:")
        print(f"   Average Volume: {stats['volume_stats']['avg_volume']:,.0f}")
        print(f"   Max Volume: {stats['volume_stats']['max_volume']:,.0f}")
        print(f"   Total Volume: {stats['volume_stats']['total_volume']:,.0f}")
        print(f"\n📈 RETURN STATISTICS:")
        print(f"   Total Return: {stats['returns']['total_return']:.2f}%")
        print(f"   Annualized Return: {stats['returns']['annualized_return']:.2f}%")
        print(f"{'='*60}\n")


def main():
    """Main function to download and analyze Apple stock data."""
    print("🍎 Apple Stock Data Downloader")
    print("=" * 40)

    try:
        # Create downloader instance
        downloader = StockDataDownloader(symbol="AAPL", years=10)

        # Download data
        data = downloader.download_data()

        # Print summary
        downloader.print_summary()

        # Save data to CSV
        csv_file = downloader.save_data()
        print(f"💾 Data saved to: {csv_file}")

        # Create plots
        price_plot = downloader.plot_price_history()
        volume_plot = downloader.plot_volume_and_price()

        print(f"📊 Price plot saved to: {price_plot}")
        print(f"📊 Volume plot saved to: {volume_plot}")

        print("\n✅ Data download and analysis complete!")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
