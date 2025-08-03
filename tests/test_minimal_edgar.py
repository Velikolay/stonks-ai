"""
Tests for the minimal EDGAR XBRL extractor.
"""

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from edgar_xbrl_extractor import MinimalEdgarExtractor


class TestMinimalEdgarExtractor:
    """Test cases for MinimalEdgarExtractor class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = MinimalEdgarExtractor(download_path="test_downloads")

    def test_init(self):
        """Test extractor initialization."""
        assert self.extractor.download_path == Path("test_downloads")
        assert self.extractor.downloader is not None
        assert self.extractor.xbrl_parser is not None

    @patch.object(MinimalEdgarExtractor, "downloader")
    def test_download_10q_filing_success(self, mock_downloader):
        """Test successful 10-Q filing download."""
        # Mock the downloader
        mock_downloader.get.return_value = None

        # Mock the file system
        with patch.object(Path, "exists", return_value=True):
            with patch.object(Path, "glob", return_value=[Path("test_filing")]):
                with patch.object(Path, "stat") as mock_stat:
                    mock_stat.return_value = Mock(st_mtime=1234567890)

                    result = self.extractor.download_10q_filing("AAPL")

                    assert result == "test_filing"
                    mock_downloader.get.assert_called_once_with(
                        "10-Q", "AAPL", amount=1
                    )

    @patch.object(MinimalEdgarExtractor, "downloader")
    def test_download_10q_filing_error(self, mock_downloader):
        """Test 10-Q filing download with error."""
        mock_downloader.get.side_effect = Exception("Download error")

        result = self.extractor.download_10q_filing("INVALID")

        assert result is None

    @patch("edgar_xbrl_extractor.XBRLParser")
    @patch("edgar_xbrl_extractor.GAAP")
    @patch("edgar_xbrl_extractor.GAAPSerializer")
    def test_parse_xbrl_filing_success(self, mock_serializer, mock_gaap, mock_parser):
        """Test successful XBRL filing parsing."""
        # Mock the parser and related objects
        mock_xbrl_obj = Mock()
        mock_parser.return_value.parse.return_value = mock_xbrl_obj

        mock_gaap_obj = Mock()
        mock_gaap.return_value = mock_gaap_obj

        mock_serializer_obj = Mock()
        mock_serializer.return_value = mock_serializer_obj
        mock_serializer_obj.data = {"Revenues": {"value": "1000000", "unit": "USD"}}

        # Mock file system
        with patch.object(Path, "rglob", return_value=[Path("test.xml")]):
            result = self.extractor.parse_xbrl_filing("test_filing_path")

            assert result is not None
            assert "Revenues" in result

    def test_parse_xbrl_filing_no_files(self):
        """Test XBRL parsing with no files found."""
        with patch.object(Path, "rglob", return_value=[]):
            result = self.extractor.parse_xbrl_filing("test_filing_path")

            assert result is None

    def test_extract_financial_metrics(self):
        """Test financial metrics extraction."""
        xbrl_data = {
            "Revenues": {"value": "1000000", "unit": "USD", "context": "FD2023Q1"},
            "NetIncomeLoss": {"value": "200000", "unit": "USD", "context": "FD2023Q1"},
        }

        df = self.extractor.extract_financial_metrics(xbrl_data)

        assert not df.empty
        assert len(df) == 2
        assert "Revenues" in df["metric"].values
        assert "NetIncomeLoss" in df["metric"].values

    def test_extract_financial_metrics_empty(self):
        """Test financial metrics extraction with empty data."""
        df = self.extractor.extract_financial_metrics({})

        assert df.empty

    @patch("pandas.DataFrame.to_csv")
    def test_save_to_csv_success(self, mock_to_csv):
        """Test successful CSV export."""
        df = pd.DataFrame(
            {"metric": ["Revenues"], "value": ["1000000"], "unit": ["USD"]}
        )

        result = self.extractor.save_to_csv(df, "test_output.csv")

        assert result is True
        mock_to_csv.assert_called_once()

    def test_save_to_csv_empty(self):
        """Test CSV export with empty DataFrame."""
        df = pd.DataFrame()

        result = self.extractor.save_to_csv(df, "test_output.csv")

        assert result is False

    @patch.object(MinimalEdgarExtractor, "download_10q_filing")
    @patch.object(MinimalEdgarExtractor, "parse_xbrl_filing")
    @patch.object(MinimalEdgarExtractor, "extract_financial_metrics")
    @patch.object(MinimalEdgarExtractor, "save_to_csv")
    def test_get_10q_data_for_ticker_success(
        self, mock_save, mock_extract, mock_parse, mock_download
    ):
        """Test successful 10-Q data extraction."""
        # Mock all the steps
        mock_download.return_value = "test_filing_path"
        mock_parse.return_value = {"Revenues": {"value": "1000000"}}

        df = pd.DataFrame(
            {"metric": ["Revenues"], "value": ["1000000"], "unit": ["USD"]}
        )
        mock_extract.return_value = df
        mock_save.return_value = True

        result = self.extractor.get_10q_data_for_ticker(
            ticker="AAPL", output_file="test.csv"
        )

        assert result is not None
        assert not result.empty
        mock_save.assert_called_once()

    @patch.object(MinimalEdgarExtractor, "download_10q_filing")
    def test_get_10q_data_for_ticker_download_failed(self, mock_download):
        """Test 10-Q data extraction with download failure."""
        mock_download.return_value = None

        result = self.extractor.get_10q_data_for_ticker("INVALID")

        assert result is None


if __name__ == "__main__":
    pytest.main([__file__])
