"""Tests for the XBRLFilingsLoader."""

from datetime import date
from unittest.mock import Mock, patch

from filings import FilingsDatabase
from filings.models.company import Company
from filings.models.filing import Filing
from filings.models.financial_fact import FinancialFactCreate
from filings.sec_xbrl_filings_loader import SECXBRLFilingsLoader


class TestXBRLFilingsLoader:
    """Test the XBRLFilingsLoader class."""

    def test_loader_initialization(self):
        """Test loader initialization."""
        mock_database = Mock(spec=FilingsDatabase)
        loader = SECXBRLFilingsLoader(mock_database)

        assert loader.parser is not None
        assert loader.database == mock_database

    def test_loader_initialization_with_custom_parser(self):
        """Test loader initialization with custom parser."""
        mock_database = Mock()
        mock_parser = Mock()
        loader = SECXBRLFilingsLoader(mock_database, parser=mock_parser)

        assert loader.parser == mock_parser
        assert loader.database == mock_database

    def test_get_or_create_company_existing(self):
        """Test getting existing company."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # Mock existing company
        existing_company = Mock(spec=Company)
        existing_company.name = "Apple Inc."
        existing_company.ticker = "AAPL"
        mock_database.companies.get_company_by_ticker.return_value = existing_company

        result = loader._get_or_create_company("AAPL")

        assert result == existing_company
        mock_database.companies.get_company_by_ticker.assert_called_once_with("AAPL")

    def test_get_or_create_company_new(self):
        """Test creating new company."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # Mock no existing company
        mock_database.companies.get_company_by_ticker.return_value = None

        # Mock new company creation
        new_company = Mock(spec=Company)
        new_company.name = "AAPL"
        new_company.ticker = "AAPL"
        mock_database.companies.get_or_create_company.return_value = new_company

        result = loader._get_or_create_company("AAPL")

        assert result == new_company
        mock_database.companies.get_company_by_ticker.assert_called_once_with("AAPL")
        mock_database.companies.get_or_create_company.assert_called_once()

    def test_load_single_filing_new(self):
        """Test loading a new filing."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # Mock filing doesn't exist
        mock_database.filings.get_filing_by_number.return_value = None

        # Mock filing object
        mock_filing = Mock()
        mock_filing.accession_number = "0001193125-24-000001"
        mock_filing.form = "10-Q"
        mock_filing.filing_date = "2024-01-15"
        mock_filing.period_of_report = "2024-03-31"  # Q1 end date
        mock_filing.url = "https://example.com/filing"

        # Mock parser returning facts
        mock_fact1 = Mock(spec=FinancialFactCreate)
        mock_fact1.period_end = date(2024, 3, 31)
        mock_fact2 = Mock(spec=FinancialFactCreate)
        mock_fact2.period_end = date(2024, 3, 31)
        mock_facts = [mock_fact1, mock_fact2]
        loader.parser.parse_filing = Mock(return_value=mock_facts)

        # Mock database insertions
        mock_db_filing = Mock(spec=Filing)
        mock_db_filing.id = 1
        mock_database.filings.insert_filing.return_value = mock_db_filing
        mock_database.financial_facts.insert_financial_facts_batch.return_value = [1, 2]

        result = loader._load_single_filing(mock_filing, 1, override=False)

        assert result[0] == 2
        mock_database.filings.insert_filing.assert_called_once()

        # Verify that insert_filing was called with the correct fiscal quarter
        call_args = mock_database.filings.insert_filing.call_args[0][0]
        assert call_args.fiscal_quarter == 1  # March 31st should be Q1
        assert call_args.fiscal_period_end == date(2024, 3, 31)

        mock_database.financial_facts.insert_financial_facts_batch.assert_called_once()

    def test_load_single_filing_different_quarters(self):
        """Test loading filings with different fiscal quarters."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # Test Q2 filing
        mock_database.filings.get_filing_by_number.return_value = None
        mock_filing_q2 = Mock()
        mock_filing_q2.accession_number = "0001193125-24-000002"
        mock_filing_q2.form = "10-Q"
        mock_filing_q2.filing_date = "2024-05-15"
        mock_filing_q2.period_of_report = "2024-06-30"  # Q2 end date
        mock_filing_q2.url = "https://example.com/filing"

        mock_fact = Mock(spec=FinancialFactCreate)
        mock_fact.period_end = date(2024, 6, 30)
        mock_facts = [mock_fact]
        loader.parser.parse_filing = Mock(return_value=mock_facts)
        mock_db_filing = Mock(spec=Filing)
        mock_db_filing.id = 1
        mock_database.filings.insert_filing.return_value = mock_db_filing
        mock_database.financial_facts.insert_financial_facts_batch.return_value = [1]

        result = loader._load_single_filing(mock_filing_q2, 1, override=False)
        assert result[0] == 1

        # Verify Q2 was calculated correctly
        call_args = mock_database.filings.insert_filing.call_args[0][0]
        assert call_args.fiscal_quarter == 2  # June 30th should be Q2

        # Test Q4 filing
        mock_database.filings.get_filing_by_number.return_value = None
        mock_filing_q4 = Mock()
        mock_filing_q4.accession_number = "0001193125-24-000003"
        mock_filing_q4.form = "10-K"
        mock_filing_q4.filing_date = "2024-12-15"
        mock_filing_q4.period_of_report = "2024-12-31"  # Q4 end date
        mock_filing_q4.url = "https://example.com/filing"

        mock_fact_q4 = Mock(spec=FinancialFactCreate)
        mock_fact_q4.period_end = date(2024, 12, 31)
        mock_facts_q4 = [mock_fact_q4]
        loader.parser.parse_filing = Mock(return_value=mock_facts_q4)
        mock_database.filings.insert_filing.return_value = mock_db_filing
        mock_database.financial_facts.insert_financial_facts_batch.return_value = [1]

        result = loader._load_single_filing(mock_filing_q4, 1, override=False)
        assert result[0] == 1

        # Verify Q4 was calculated correctly
        call_args = mock_database.filings.insert_filing.call_args[0][0]
        assert call_args.fiscal_quarter == 4  # December 31st should be Q4

    def test_load_single_filing_exists(self):
        """Test loading an existing filing."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # Mock filing already exists
        existing_filing = Mock(spec=Filing)
        mock_database.filings.get_filing_by_number.return_value = existing_filing

        mock_filing = Mock()
        mock_filing.accession_number = "0001193125-24-000001"

        result = loader._load_single_filing(mock_filing, 1, override=False)

        assert result[0] == 0
        mock_database.filings.get_filing_by_number.assert_called_once_with(
            "SEC", "0001193125-24-000001"
        )

    def test_load_single_filing_with_override(self):
        """Test loading a filing with override enabled."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)  # No constructor override

        # Mock existing filing
        existing_filing = Mock()
        existing_filing.id = 1
        mock_database.filings.get_filing_by_number.return_value = existing_filing

        # Mock filing object
        mock_filing = Mock()
        mock_filing.accession_number = "0001193125-24-000001"
        mock_filing.form = "10-Q"
        mock_filing.filing_date = "2024-01-15"
        mock_filing.period_of_report = "2024-03-31"
        mock_filing.url = "https://example.com/filing"

        # Mock parser returning facts
        mock_fact1 = Mock(spec=FinancialFactCreate)
        mock_fact1.period_end = date(2024, 3, 31)
        mock_fact2 = Mock(spec=FinancialFactCreate)
        mock_fact2.period_end = date(2024, 3, 31)
        mock_facts = [mock_fact1, mock_fact2]
        loader.parser.parse_filing = Mock(return_value=mock_facts)

        # Mock database operations - use delete and insert methods
        mock_db_filing = Mock(spec=Filing)
        mock_db_filing.id = 2  # New filing ID after override
        mock_database.filings.insert_filing.return_value = mock_db_filing
        mock_database.financial_facts.insert_financial_facts_batch.return_value = [1, 2]
        mock_database.financial_facts.delete_facts_by_filing_id.return_value = True
        mock_database.filings.delete_filing.return_value = True

        result, was_updated = loader._load_single_filing(mock_filing, 1, override=True)

        assert result == 2
        assert was_updated is True
        # Should call delete methods first, then insert
        mock_database.financial_facts.delete_facts_by_filing_id.assert_called_once_with(
            1
        )
        mock_database.filings.delete_filing.assert_called_once_with(1)
        mock_database.filings.insert_filing.assert_called_once()
        mock_database.financial_facts.insert_financial_facts_batch.assert_called_once()

    def test_load_company_filings_method_override(self):
        """Test loading company filings with method-level override."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)  # No constructor override

        # Mock company
        mock_company = Mock()
        mock_company.id = 1
        mock_company.name = "Apple Inc."
        mock_company.ticker = "AAPL"
        mock_database.companies.get_company_by_ticker.return_value = mock_company

        # Mock edgar company and filings
        mock_edgar_company = Mock()
        mock_filing = Mock()
        mock_filing.accession_number = "0001193125-24-000001"
        mock_edgar_company.get_filings.return_value = [mock_filing]
        with patch("filings.sec_xbrl_filings_loader.Company") as mock_company_class:
            mock_company_class.return_value = mock_edgar_company

            # Mock loading single filing with override
            loader._load_single_filing = Mock(
                return_value=(5, True)
            )  # 5 facts, was updated

            result = loader.load_company_filings("AAPL", "10-Q", limit=1, override=True)

            assert result["ticker"] == "AAPL"
            assert result["form"] == "10-Q"
            assert result["filings_loaded"] == 1
            assert result["filings_updated"] == 1
            assert result["total_facts"] == 5
            assert result["company_id"] == 1
            assert result["override_mode"] is True

    @patch("filings.sec_xbrl_filings_loader.Company")
    def test_load_company_filings_no_filings(self, mock_company_class):
        """Test loading when no filings are found."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # Mock company
        mock_company = Mock()
        mock_company.id = 1
        mock_company.name = "Apple Inc."
        mock_company.ticker = "AAPL"
        mock_database.companies.get_company_by_ticker.return_value = mock_company

        # Mock no filings
        mock_edgar_company = Mock()
        mock_edgar_company.get_filings.return_value = []
        mock_company_class.return_value = mock_edgar_company

        result = loader.load_company_filings("AAPL", "10-Q", limit=5)

        assert "message" in result
        assert "No 10-Q filings found" in result["message"]

    def test_load_company_filings_company_error(self):
        """Test loading when company creation fails."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # Mock company creation failure
        mock_database.companies.get_company_by_ticker.return_value = None
        mock_database.companies.get_or_create_company.return_value = None

        result = loader.load_company_filings("INVALID", "10-Q", limit=5)

        assert "error" in result
        assert "Failed to get or create company" in result["error"]

    def test_calculate_fiscal_quarter(self):
        """Test fiscal quarter calculation from period end date."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # Test Q1 (Jan, Feb, Mar)
        assert loader._calculate_fiscal_quarter("2024-01-31") == 4
        assert loader._calculate_fiscal_quarter("2024-02-29") == 4
        assert loader._calculate_fiscal_quarter("2024-03-31") == 1

        # Test Q2 (Apr, May, Jun)
        assert loader._calculate_fiscal_quarter("2024-04-30") == 1
        assert loader._calculate_fiscal_quarter("2024-05-31") == 1
        assert loader._calculate_fiscal_quarter("2024-06-30") == 2

        # Test Q3 (Jul, Aug, Sep)
        assert loader._calculate_fiscal_quarter("2024-07-31") == 2
        assert loader._calculate_fiscal_quarter("2024-08-31") == 2
        assert loader._calculate_fiscal_quarter("2024-09-30") == 3

        # Test Q4 (Oct, Nov, Dec)
        assert loader._calculate_fiscal_quarter("2024-10-31") == 3
        assert loader._calculate_fiscal_quarter("2024-11-30") == 3
        assert loader._calculate_fiscal_quarter("2024-12-31") == 4

        # Test edge cases
        assert loader._calculate_fiscal_quarter(None) is None
        assert loader._calculate_fiscal_quarter("") is None
        assert loader._calculate_fiscal_quarter("invalid-date") is None

    def test_calculate_fiscal_quarter_invalid_month(self):
        """Test fiscal quarter calculation with invalid month."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = SECXBRLFilingsLoader(mock_database)

        # This should return None for invalid month (though this case is unlikely)
        # We'll test with a date that would have an invalid month if parsed incorrectly
        with patch.object(loader, "_parse_date") as mock_parse:
            mock_parse.return_value = Mock()
            mock_parse.return_value.month = 13  # Invalid month
            result = loader._calculate_fiscal_quarter("2024-01-31")
            assert result is None
