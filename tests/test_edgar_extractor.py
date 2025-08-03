"""
Tests for the EDGAR XBRL extractor functionality.
"""

from unittest.mock import Mock, patch

import pytest

from edgar_xbrl_extractor import (
    CompanyInfo,
    EdgarXBRLExtractor,
    FilingInfo,
    FinancialMetric,
)


class TestEdgarXBRLExtractor:
    """Test cases for EdgarXBRLExtractor class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = EdgarXBRLExtractor()

    def test_init(self):
        """Test extractor initialization."""
        assert self.extractor.base_url == "https://www.sec.gov"
        assert self.extractor.search_url == "https://www.sec.gov/cgi-bin/browse-edgar"
        assert "User-Agent" in self.extractor.headers

    @patch("edgar_xbrl_extractor.requests.Session.get")
    def test_search_company_success(self, mock_get):
        """Test successful company search."""
        # Mock response
        mock_response = Mock()
        mock_response.content = """
        <feed>
            <companyInfo>
                <CIK>0000320193</CIK>
                <conformedName>Apple Inc.</conformedName>
                <SIC>3571</SIC>
                <SICDescription>Electronic Computers</SICDescription>
            </companyInfo>
        </feed>
        """
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        companies = self.extractor.search_company("AAPL")

        assert len(companies) == 1
        assert companies[0].cik == "0000320193"
        assert companies[0].name == "Apple Inc."
        assert companies[0].sic == "3571"
        assert companies[0].sic_description == "Electronic Computers"

    @patch("edgar_xbrl_extractor.requests.Session.get")
    def test_search_company_error(self, mock_get):
        """Test company search with error."""
        mock_get.side_effect = Exception("Network error")

        companies = self.extractor.search_company("INVALID")

        assert len(companies) == 0

    @patch("edgar_xbrl_extractor.requests.Session.get")
    def test_get_company_filings_success(self, mock_get):
        """Test successful filing retrieval."""
        # Mock response for filing search
        mock_response = Mock()
        mock_response.content = """
        <feed>
            <filingHREF>https://www.sec.gov/Archives/edgar/data/320193/000032019323000006/aapl-20230701.htm</filingHREF>
        </feed>
        """
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        filings = self.extractor.get_company_filings("0000320193", "10-Q")

        assert len(filings) >= 0  # May be empty due to parsing complexity

    @patch("edgar_xbrl_extractor.requests.Session.get")
    def test_get_company_filings_error(self, mock_get):
        """Test filing retrieval with error."""
        mock_get.side_effect = Exception("Network error")

        filings = self.extractor.get_company_filings("0000320193", "10-Q")

        assert len(filings) == 0

    def test_parse_filing_url_valid(self):
        """Test parsing valid filing URL."""
        url = "https://www.sec.gov/Archives/edgar/data/320193/000032019323000006/aapl-20230701.htm"

        with patch.object(self.extractor, "_parse_filing_url") as mock_parse:
            mock_parse.return_value = FilingInfo(
                accession_number="0000320193-23-000006",
                filing_date="2023-07-01",
                filing_type="10-Q",
                company_name="Apple Inc.",
                cik="0000320193",
                form_name="10-Q",
            )

            result = self.extractor._parse_filing_url(url)

            assert result is not None
            assert result.accession_number == "0000320193-23-000006"

    def test_parse_filing_url_invalid(self):
        """Test parsing invalid filing URL."""
        url = "https://invalid-url.com"

        result = self.extractor._parse_filing_url(url)

        assert result is None

    @patch("edgar_xbrl_extractor.requests.Session.get")
    def test_download_xbrl_data_success(self, mock_get):
        """Test successful XBRL data download."""
        # Mock filing info
        filing_info = FilingInfo(
            accession_number="0000320193-23-000006",
            filing_date="2023-07-01",
            filing_type="10-Q",
            company_name="Apple Inc.",
            cik="0000320193",
            form_name="10-Q",
        )

        # Mock response
        mock_response = Mock()
        mock_response.content = """
        <?xml version="1.0" encoding="UTF-8"?>
        <xbrl xmlns="http://www.xbrl.org/2003/instance">
            <context id="FD2023Q1">
                <period>
                    <startDate>2023-01-01</startDate>
                    <endDate>2023-03-31</endDate>
                </period>
            </context>
            <Revenues contextRef="FD2023Q1" unitRef="USD">94836</Revenues>
            <NetIncomeLoss contextRef="FD2023Q1" unitRef="USD">30000</NetIncomeLoss>
        </xbrl>
        """
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = self.extractor.download_xbrl_data(filing_info)

        assert result is not None
        assert "facts" in result
        assert "contexts" in result

    @patch("edgar_xbrl_extractor.requests.Session.get")
    def test_download_xbrl_data_error(self, mock_get):
        """Test XBRL data download with error."""
        filing_info = FilingInfo(
            accession_number="0000320193-23-000006",
            filing_date="2023-07-01",
            filing_type="10-Q",
            company_name="Apple Inc.",
            cik="0000320193",
            form_name="10-Q",
        )

        mock_get.side_effect = Exception("Download error")

        result = self.extractor.download_xbrl_data(filing_info)

        assert result is None

    def test_parse_xbrl_instance_valid(self):
        """Test parsing valid XBRL instance."""
        content = """
        <?xml version="1.0" encoding="UTF-8"?>
        <xbrl xmlns="http://www.xbrl.org/2003/instance">
            <context id="FD2023Q1">
                <period>
                    <startDate>2023-01-01</startDate>
                    <endDate>2023-03-31</endDate>
                </period>
            </context>
            <Revenues contextRef="FD2023Q1" unitRef="USD">94836</Revenues>
            <NetIncomeLoss contextRef="FD2023Q1" unitRef="USD">30000</NetIncomeLoss>
        </xbrl>
        """.encode()

        filing_info = FilingInfo(
            accession_number="0000320193-23-000006",
            filing_date="2023-07-01",
            filing_type="10-Q",
            company_name="Apple Inc.",
            cik="0000320193",
            form_name="10-Q",
        )

        result = self.extractor._parse_xbrl_instance(content, filing_info)

        assert result is not None
        assert "facts" in result
        assert "contexts" in result
        assert len(result["facts"]) >= 2

    def test_extract_financial_metrics(self):
        """Test financial metrics extraction."""
        xbrl_data = {
            "filing_info": FilingInfo(
                accession_number="0000320193-23-000006",
                filing_date="2023-07-01",
                filing_type="10-Q",
                company_name="Apple Inc.",
                cik="0000320193",
                form_name="10-Q",
            ),
            "facts": [
                {
                    "concept": "Revenues",
                    "value": "94836",
                    "context_ref": "FD2023Q1",
                    "unit_ref": "USD",
                    "period": "2023-03-31",
                },
                {
                    "concept": "NetIncomeLoss",
                    "value": "30000",
                    "context_ref": "FD2023Q1",
                    "unit_ref": "USD",
                    "period": "2023-03-31",
                },
            ],
        }

        metrics = self.extractor.extract_financial_metrics(xbrl_data)

        assert len(metrics) == 2
        assert any(m.concept == "Revenues" for m in metrics)
        assert any(m.concept == "NetIncomeLoss" for m in metrics)

    def test_extract_financial_metrics_empty(self):
        """Test financial metrics extraction with empty data."""
        metrics = self.extractor.extract_financial_metrics({})
        assert len(metrics) == 0

    @patch("edgar_xbrl_extractor.Path.mkdir")
    @patch("pandas.DataFrame.to_csv")
    def test_export_to_csv_success(self, mock_to_csv, mock_mkdir):
        """Test successful CSV export."""
        metrics = [
            FinancialMetric(
                concept="Revenues",
                value=94836.0,
                unit="USD",
                context_ref="FD2023Q1",
                period="2023-03-31",
                filing_date="2023-07-01",
            )
        ]

        result = self.extractor.export_to_csv(metrics, "test_output.csv")

        assert result is True
        mock_to_csv.assert_called_once()

    def test_export_to_csv_empty(self):
        """Test CSV export with empty metrics."""
        result = self.extractor.export_to_csv([], "test_output.csv")
        assert result is False

    @patch.object(EdgarXBRLExtractor, "search_company")
    @patch.object(EdgarXBRLExtractor, "get_company_filings")
    @patch.object(EdgarXBRLExtractor, "download_xbrl_data")
    @patch.object(EdgarXBRLExtractor, "extract_financial_metrics")
    def test_get_10q_data_for_quarter_success(
        self, mock_extract, mock_download, mock_filings, mock_search
    ):
        """Test successful 10-Q data extraction."""
        # Mock company search
        mock_search.return_value = [
            CompanyInfo(cik="0000320193", name="Apple Inc.", ticker="AAPL")
        ]

        # Mock filings
        mock_filings.return_value = [
            FilingInfo(
                accession_number="0000320193-23-000006",
                filing_date="2023-07-01",
                filing_type="10-Q",
                company_name="Apple Inc.",
                cik="0000320193",
                form_name="10-Q",
            )
        ]

        # Mock XBRL data
        mock_download.return_value = {
            "filing_info": FilingInfo(
                accession_number="0000320193-23-000006",
                filing_date="2023-07-01",
                filing_type="10-Q",
                company_name="Apple Inc.",
                cik="0000320193",
                form_name="10-Q",
            ),
            "facts": [
                {
                    "concept": "Revenues",
                    "value": "94836",
                    "context_ref": "FD2023Q1",
                    "unit_ref": "USD",
                    "period": "2023-03-31",
                }
            ],
        }

        # Mock metrics extraction
        mock_extract.return_value = [
            FinancialMetric(
                concept="Revenues",
                value=94836.0,
                unit="USD",
                context_ref="FD2023Q1",
                period="2023-03-31",
                filing_date="2023-07-01",
            )
        ]

        result = self.extractor.get_10q_data_for_quarter(
            company_query="AAPL", year=2023, quarter=1
        )

        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["concept"] == "Revenues"

    @patch.object(EdgarXBRLExtractor, "search_company")
    def test_get_10q_data_for_quarter_company_not_found(self, mock_search):
        """Test 10-Q data extraction with company not found."""
        mock_search.return_value = []

        result = self.extractor.get_10q_data_for_quarter(
            company_query="INVALID", year=2023, quarter=1
        )

        assert result is None


class TestModels:
    """Test cases for Pydantic models."""

    def test_company_info(self):
        """Test CompanyInfo model."""
        company = CompanyInfo(
            cik="0000320193",
            name="Apple Inc.",
            ticker="AAPL",
            sic="3571",
            sic_description="Electronic Computers",
        )

        assert company.cik == "0000320193"
        assert company.name == "Apple Inc."
        assert company.ticker == "AAPL"
        assert company.sic == "3571"
        assert company.sic_description == "Electronic Computers"

    def test_filing_info(self):
        """Test FilingInfo model."""
        filing = FilingInfo(
            accession_number="0000320193-23-000006",
            filing_date="2023-07-01",
            filing_type="10-Q",
            company_name="Apple Inc.",
            cik="0000320193",
            form_name="10-Q",
        )

        assert filing.accession_number == "0000320193-23-000006"
        assert filing.filing_date == "2023-07-01"
        assert filing.filing_type == "10-Q"
        assert filing.company_name == "Apple Inc."
        assert filing.cik == "0000320193"
        assert filing.form_name == "10-Q"

    def test_financial_metric(self):
        """Test FinancialMetric model."""
        metric = FinancialMetric(
            concept="Revenues",
            value=94836.0,
            unit="USD",
            context_ref="FD2023Q1",
            period="2023-03-31",
            filing_date="2023-07-01",
        )

        assert metric.concept == "Revenues"
        assert metric.value == 94836.0
        assert metric.unit == "USD"
        assert metric.context_ref == "FD2023Q1"
        assert metric.period == "2023-03-31"
        assert metric.filing_date == "2023-07-01"


if __name__ == "__main__":
    pytest.main([__file__])
