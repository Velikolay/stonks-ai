"""Tests for the FilingsLoader."""

from unittest.mock import Mock, patch

from filings import FilingsDatabase
from filings.filings_loader import FilingsLoader
from filings.models.company import Company
from filings.models.filing import Filing
from filings.models.financial_fact import FinancialFact


class TestFilingsLoader:
    """Test the FilingsLoader class."""

    def test_loader_initialization(self):
        """Test loader initialization."""
        mock_database = Mock(spec=FilingsDatabase)
        loader = FilingsLoader(mock_database)

        assert loader.parser is not None
        assert loader.database == mock_database

    def test_loader_initialization_with_custom_parser(self):
        """Test loader initialization with custom parser."""
        mock_database = Mock(spec=FilingsDatabase)
        mock_parser = Mock()
        loader = FilingsLoader(mock_database, parser=mock_parser)

        assert loader.parser == mock_parser
        assert loader.database == mock_database

    def test_get_or_create_company_existing(self):
        """Test getting existing company."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = FilingsLoader(mock_database)

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
        loader = FilingsLoader(mock_database)

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
        loader = FilingsLoader(mock_database)

        # Mock filing doesn't exist
        mock_database.filings.get_filing_by_number.return_value = None

        # Mock filing object
        mock_filing = Mock()
        mock_filing.accession_number = "0001193125-24-000001"
        mock_filing.form = "10-Q"
        mock_filing.filing_date = "2024-01-15"
        mock_filing.period_of_report = (
            "2024-01-15"  # This will be converted to date in the loader
        )
        mock_filing.url = "https://example.com/filing"

        # Mock parser returning facts
        mock_facts = [Mock(spec=FinancialFact), Mock(spec=FinancialFact)]
        loader.parser.parse_filing = Mock(return_value=mock_facts)

        # Mock database insertions
        mock_db_filing = Mock(spec=Filing)
        mock_db_filing.id = 1
        mock_database.filings.insert_filing.return_value = mock_db_filing
        mock_database.financial_facts.insert_financial_facts_batch.return_value = (
            mock_facts
        )

        result = loader._load_single_filing(mock_filing, 1)

        assert result == 2
        mock_database.filings.insert_filing.assert_called_once()
        mock_database.financial_facts.insert_financial_facts_batch.assert_called_once()

    def test_load_single_filing_exists(self):
        """Test loading an existing filing."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = FilingsLoader(mock_database)

        # Mock filing already exists
        existing_filing = Mock(spec=Filing)
        mock_database.filings.get_filing_by_number.return_value = existing_filing

        mock_filing = Mock()
        mock_filing.accession_number = "0001193125-24-000001"

        result = loader._load_single_filing(mock_filing, 1)

        assert result == 0
        mock_database.filings.get_filing_by_number.assert_called_once_with(
            "SEC", "0001193125-24-000001"
        )

    @patch("filings.filings_loader.Company")
    def test_load_company_filings_success(self, mock_company_class):
        """Test successful loading of company filings."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = FilingsLoader(mock_database)

        # Mock company
        mock_company = Mock()
        mock_company.id = 1
        mock_company.name = "Apple Inc."
        mock_company.ticker = "AAPL"
        mock_database.companies.get_company_by_ticker.return_value = mock_company

        # Mock edgar company and filings
        mock_edgar_company = Mock()
        mock_filing1 = Mock()
        mock_filing1.accession_number = "0001193125-24-000001"
        mock_filing2 = Mock()
        mock_filing2.accession_number = "0001193125-24-000002"

        mock_edgar_company.get_filings.return_value = [mock_filing1, mock_filing2]
        mock_company_class.return_value = mock_edgar_company

        # Mock loading single filings
        loader._load_single_filing = Mock(side_effect=[5, 3])

        result = loader.load_company_filings("AAPL", "10-Q", limit=2)

        assert result["ticker"] == "AAPL"
        assert result["form"] == "10-Q"
        assert result["filings_loaded"] == 2
        assert result["total_facts"] == 8
        assert result["company_id"] == 1

    @patch("filings.filings_loader.Company")
    def test_load_company_filings_no_filings(self, mock_company_class):
        """Test loading when no filings are found."""
        mock_database = Mock()
        mock_database.companies = Mock()
        mock_database.filings = Mock()
        mock_database.financial_facts = Mock()
        loader = FilingsLoader(mock_database)

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
        loader = FilingsLoader(mock_database)

        # Mock company creation failure
        mock_database.companies.get_company_by_ticker.return_value = None
        mock_database.companies.get_or_create_company.return_value = None

        result = loader.load_company_filings("INVALID", "10-Q", limit=5)

        assert "error" in result
        assert "Failed to get or create company" in result["error"]
