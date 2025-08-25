# RAG API with GPT-4o mini and PostgreSQL

A Python application that implements a Retrieval-Augmented Generation (RAG) system using LlamaIndex, PostgreSQL with pgvector, and OpenAI's GPT-4o mini LLM. The application provides a FastAPI-based REST API for querying the RAG system and is containerized with Docker Compose.

## Features

- **RAG System**: Built with LlamaIndex for document processing and retrieval
- **Vector Store**: Uses PostgreSQL with pgvector for efficient similarity search
- **LLM**: Powered by OpenAI's GPT-4o mini for high-quality responses
- **API**: FastAPI-based REST API for easy integration
- **Document Management**: Upload and manage documents through the API
- **Persistence**: Documents and embeddings are stored in PostgreSQL
- **Database Migrations**: Alembic-based migration system for schema management
- **Docker**: Complete containerized setup with Docker Compose

## Prerequisites

- Docker and Docker Compose
- OpenAI API key (for GPT-4o mini and embeddings)

## Quick Start with Docker

1. **Clone the repository:**
```bash
git clone <repository-url>
cd stonks-ai-py
```

2. **Set up environment variables:**
```bash
cp env.example .env
```

Edit `.env` and add your OpenAI API key:
```
OPENAI_API_KEY=your_openai_api_key_here
```

3. **Start the services:**
```bash
docker-compose up -d
```

4. **Run database migrations:**
```bash
docker-compose --profile migrate up db_migrate
```

The API will be available at `http://localhost:8000`

## Manual Installation (without Docker)

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Set up PostgreSQL with pgvector:**
   - Install PostgreSQL
   - Install pgvector extension
   - Create database

3. **Set environment variables:**
```bash
cp env.example .env
# Edit .env with your OpenAI API key and database URL
```

4. **Initialize database:**
```bash
python db.py init
```

5. **Run the application:**
```bash
python app.py
```

## Database Management

The project uses Alembic for database migrations. Here are the available commands:

### Using Make (recommended):
```bash
# Initialize database with migrations
make db-init

# Reset database (drop and recreate)
make db-reset

# Show migration status
make db-status

# Show migration history
make db-history
```

### Using Python directly:
```bash
# Initialize database
python db.py init

# Reset database
python db.py reset

# Show status
python db.py status

# Show history
python db.py history
```

### Docker commands:
```bash
# Start services
make docker-up

# Run migrations
make docker-migrate

# Stop services
make docker-down

# View logs
make docker-logs
```

## Development

This project uses several tools to maintain code quality:

### Code Formatting and Linting

The project uses:
- **Black**: Code formatter (88 character line length)
- **isort**: Import sorting
- **flake8**: Linting with custom configuration

### Development Commands

Using Make (recommended):
```bash
# Install dependencies
make install

# Format code
make format

# Run linting
make lint

# Run tests
make test

# Run all checks
make all

# Clean up cache files
make clean
```

Or using the Python script:
```bash
# Run formatting and linting
python format_code.py
```

### Manual Commands

```bash
# Format code
black .
isort .

# Run linting
flake8 .

# Run tests
python test_setup.py
```

### Configuration Files

- `.flake8`: Flake8 configuration
- `pyproject.toml`: Black, isort, and mypy configuration
- `Makefile`: Common development commands

## API Documentation

Once the server is running, you can access the interactive API documentation at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## API Endpoints

### Health Check
- **GET** `/` - Check if the API is running

### Query RAG System
- **POST** `/query` - Query the RAG system
  ```json
  {
    "query": "Your question here",
    "top_k": 3
  }
  ```

### Document Management
- **POST** `/upload` - Upload a document (multipart form data)
- **GET** `/documents/count` - Get the number of documents in the system
- **DELETE** `/documents/clear` - Clear all documents from the system

## Financial Data

The system includes comprehensive financial data management with support for SEC filings, financial facts, and materialized views for efficient querying.

### Database Views

#### Quarterly Financials
A materialized view that provides quarterly financial metrics from 10-Q and 10-K filings, with calculated missing quarters based on annual data.

#### Yearly Financials
A materialized view that provides yearly financial metrics from 10-K filings only, offering a clean annual view of financial data.

### Usage Examples

```python
from filings.db import FilingsDatabase

# Initialize database
db = FilingsDatabase("postgresql://user:pass@localhost/filings")

# Get yearly financials for a company
yearly_metrics = db.yearly_financials.get_metrics_by_company(company_id=1)

# Get quarterly financials for a specific year
quarterly_metrics = db.quarterly_financials.get_metrics_by_company_and_year(
    company_id=1, fiscal_year=2023
)

# Get metrics by concept (e.g., revenue)
revenue_metrics = db.yearly_financials.get_metrics_by_concept("Revenues")

# Refresh materialized views after new data is loaded
db.yearly_financials.refresh_view()
db.quarterly_financials.refresh_view()
```

### Key Features

- **Materialized Views**: Pre-computed views for fast querying of financial metrics
- **Concept Normalization**: Standardized financial concept names across different filings
- **Flexible Filtering**: Query by company, year, statement type, concept, or label
- **Latest Metrics**: Easy access to the most recent financial data
- **Automatic Refresh**: Views can be refreshed to include new data

## Example Usage

### Using curl

1. **Upload a document:**
```bash
curl -X POST "http://localhost:8000/upload" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@your_document.txt"
```

2. **Query the RAG system:**
```bash
curl -X POST "http://localhost:8000/query" \
  -H "accept: application/json" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is the main topic of the document?",
    "top_k": 3
  }'
```

3. **Check document count:**
```bash
curl -X GET "http://localhost:8000/documents/count"
```

### Using Python requests

```python
import requests

# Upload a document
with open('document.txt', 'rb') as f:
    files = {'file': f}
    response = requests.post('http://localhost:8000/upload', files=files)
    print(response.json())

# Query the RAG system
query_data = {
    "query": "What is the main topic of the document?",
    "top_k": 3
}
response = requests.post('http://localhost:8000/query', json=query_data)
result = response.json()
print(f"Answer: {result['answer']}")
print(f"Sources: {result['sources']}")
```

## Configuration

### Environment Variables

- `OPENAI_API_KEY`: Your OpenAI API key (required)
- `OPENAI_MODEL`: OpenAI model to use (default: gpt-4o-mini)
- `DATABASE_URL`: PostgreSQL connection string (for Docker: postgresql://rag_user:rag_password@postgres:5432/rag_db)

### RAG System Configuration

The RAG system can be configured in `rag_system.py`:

- **Chunk Size**: Default 1024 characters
- **Chunk Overlap**: Default 20 characters
- **Temperature**: Default 0.1 for more focused responses
- **Embedding Model**: Uses OpenAI's text-embedding-3-small
- **Vector Dimension**: 1536 (for text-embedding-3-small)

## Project Structure

```
stonks-ai-py/
├── app.py              # FastAPI application
├── rag_system.py       # Core RAG system implementation
├── requirements.txt     # Python dependencies
├── env.example         # Environment variables template
├── docker-compose.yml  # Docker Compose configuration
├── Dockerfile          # Docker image definition
├── init.sql           # PostgreSQL initialization script
├── README.md          # This file
└── data/              # Persistent storage for documents
```

## Technical Details

### Components

1. **LlamaIndex**: Handles document processing, chunking, and retrieval
2. **PostgreSQL + pgvector**: Vector store for efficient similarity search
3. **OpenAI GPT-4o mini**: Large language model for generating responses
4. **OpenAI Embeddings**: For creating document embeddings
5. **FastAPI**: Web framework for the REST API
6. **Docker Compose**: Container orchestration

### Data Flow

1. Documents are uploaded and processed by LlamaIndex
2. Text is chunked and embedded using OpenAI's embedding model
3. Embeddings are stored in PostgreSQL with pgvector
4. Queries are embedded and used to find similar chunks
5. Relevant chunks are sent to GPT-4o mini for answer generation

## Docker Commands

### Start services
```bash
docker-compose up -d
```

### View logs
```bash
docker-compose logs -f
```

### Stop services
```bash
docker-compose down
```

### Rebuild and restart
```bash
docker-compose down
docker-compose up --build -d
```

### Access PostgreSQL
```bash
docker-compose exec postgres psql -U rag_user -d rag_db
```

## Troubleshooting

### Common Issues

1. **Missing OpenAI API Key**: Ensure `OPENAI_API_KEY` is set in your `.env` file
2. **Database Connection Issues**: Check if PostgreSQL container is running and healthy
3. **Import Errors**: Make sure all dependencies are installed with `pip install -r requirements.txt`
4. **Port Already in Use**: Change the port in `
