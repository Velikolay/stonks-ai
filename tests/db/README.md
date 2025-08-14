# Database Tests

This directory contains comprehensive tests for the database module.

## Test Structure

```
tests/db/
├── __init__.py              # Package initialization
├── conftest.py              # Pytest configuration and fixtures
├── test_companies.py        # Company operations tests
├── test_filings.py          # Filing operations tests
├── test_financial_facts.py  # Financial facts operations tests
├── test_integration.py      # Integration tests
├── run_tests.py             # Test runner script
└── README.md                # This file
```

## Test Coverage

### Company Operations (`test_companies.py`)
- ✅ Insert company
- ✅ Get company by ID
- ✅ Get company by ticker
- ✅ Get company by ticker and exchange
- ✅ Get all companies
- ✅ Get or create company
- ✅ Model validation
- ✅ Error handling

### Filing Operations (`test_filings.py`)
- ✅ Insert filing
- ✅ Get filing by ID
- ✅ Get filings by company
- ✅ Get filings by company and form type
- ✅ Get latest filing
- ✅ Get filing by number
- ✅ Get or create filing
- ✅ Model validation
- ✅ Error handling

### Financial Facts Operations (`test_financial_facts.py`)
- ✅ Insert single financial fact
- ✅ Insert batch financial facts
- ✅ Get facts by filing
- ✅ Get facts by metric
- ✅ Get facts by metric with limit
- ✅ Model validation
- ✅ Optional fields handling

### Integration Tests (`test_integration.py`)
- ✅ Complete workflow (company → filing → facts)
- ✅ Multiple companies and filings
- ✅ Get or create operations
- ✅ Data consistency
- ✅ Error handling

## Running Tests

### Prerequisites

1. **Database Setup**: Ensure you have a PostgreSQL database running
2. **Test Database**: Create a test database (e.g., `rag_db_test`)
3. **Environment**: Activate your virtual environment

### Method 1: Using the Test Runner

```bash
# From the project root
python tests/db/run_tests.py
```

### Method 2: Using pytest directly

```bash
# From the project root
pytest tests/db/ -v

# Run specific test file
pytest tests/db/test_companies.py -v

# Run specific test class
pytest tests/db/test_companies.py::TestCompanyOperations -v

# Run specific test method
pytest tests/db/test_companies.py::TestCompanyOperations::test_insert_company -v
```

### Method 3: Using pytest with coverage

```bash
# Install pytest-cov if not already installed
pip install pytest-cov

# Run tests with coverage
pytest tests/db/ -v --cov=db --cov-report=html
```

## Test Database Configuration

The tests use a separate test database to avoid affecting production data. Update the database URL in `conftest.py` if needed:

```python
@pytest.fixture
def test_db_url() -> str:
    """Test database URL."""
    return "postgresql://rag_user:rag_password@localhost:5432/rag_db_test"
```

## Test Fixtures

The tests use several fixtures defined in `conftest.py`:

- `test_db_url`: Test database connection string
- `test_engine`: SQLAlchemy engine for test database
- `db`: Database instance with all operations
- `sample_company`: Sample company data
- `sample_filing`: Sample filing data
- `sample_financial_fact`: Sample financial fact data

## Test Isolation

Each test runs in isolation:
- Test tables are created before each test
- Test data is cleaned up after each test
- No test affects another test's data

## Expected Test Results

When all tests pass, you should see output like:

```
Running Database Tests
==================================================
✅ Database module imported successfully
test_companies.py::TestCompanyOperations::test_insert_company PASSED
test_companies.py::TestCompanyOperations::test_get_company_by_id PASSED
...
test_integration.py::TestDatabaseIntegration::test_full_workflow PASSED
...

✅ All database tests passed!
```

## Troubleshooting

### Common Issues

1. **Database Connection Error**
   - Ensure PostgreSQL is running
   - Check database credentials in `conftest.py`
   - Verify test database exists

2. **Import Error**
   - Ensure you're in the project root directory
   - Check that the `db` module is properly installed/importable

3. **Test Failures**
   - Check database permissions
   - Ensure test database is empty
   - Verify SQLAlchemy version compatibility

### Debug Mode

Run tests with more verbose output:

```bash
pytest tests/db/ -v -s --tb=long
```

This will show:
- Detailed test output
- Print statements
- Full tracebacks for failures
