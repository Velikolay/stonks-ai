.PHONY: help install format lint test clean db-init db-reset db-status \
	ingest refresh overrides-load overrides-check psql diagnose

# Connection string used by the psql-based targets below. Override per-invocation
# with `make psql DATABASE_URL=...`.
DATABASE_URL ?= postgresql://rag_user:rag_password@localhost:5432/rag_db

# Ingest defaults; override on the command line, e.g. `make ingest TICKERS=AAPL,MSFT`
TICKERS ?=
TICKERS_FILE ?=
FORMS ?= 10-K,10-Q
# 0 loads every available XBRL filing, which is what a new company needs.
LIMIT ?= 0
COMPANY_IDS ?=

# Default target
help:
	@echo "Available commands:"
	@echo "  install    - Install dependencies"
	@echo "  format     - Format code with black and isort"
	@echo "  lint       - Run flake8 linting"
	@echo "  typecheck  - Run mypy type checking"
	@echo "  test       - Run tests"
	@echo "  test-cov   - Run tests with coverage"
	@echo "  clean      - Clean up cache files"
	@echo "  all        - Run format, lint, typecheck, and test"
	@echo ""
	@echo "Database commands:"
	@echo "  db-init    - Initialize database with migrations"
	@echo "  db-reset   - Reset database (drop and recreate)"
	@echo "  db-status  - Show migration status"
	@echo "  db-history - Show migration history"
	@echo ""
	@echo "Filings data commands (see AGENTS.md):"
	@echo "  ingest          - Ingest all SEC filings (TICKERS=AAPL,MSFT; LIMIT=N caps per form)"
	@echo "  refresh         - Recompute normalized/quarterly/yearly financials (COMPANY_IDS=1,2)"
	@echo "  overrides-load  - Sync migrations/data/*.csv overrides into the database"
	@echo "  overrides-check - Show what overrides-load would change (dry run)"
	@echo "  diagnose        - Run the data-quality queries in sql/diagnostics"
	@echo "  psql            - Open an interactive psql shell"
	@echo ""
	@echo "Docker commands:"
	@echo "  docker-up          - Start services"
	@echo "  docker-down        - Stop services (preserves data)"
	@echo "  docker-migrate     - Run database migrations"
	@echo "  docker-logs        - View logs"
	@echo "  docker-backup      - Create database backup"
	@echo "  docker-restore     - Restore database from backup"
	@echo "  docker-volume-status - Check volume status"
	@echo "  docker-cleanup     - Safe cleanup (preserves volumes)"
	@echo "  docker-cleanup-all - DANGEROUS: Remove all data"

# Install dependencies
install:
	pip install -r requirements.txt

# Run the application
run:
	python run.py

# Format code
format:
	@echo "🎨 Formatting code..."
	black .
	isort .

# Lint code
lint:
	@echo "🔍 Linting code..."
	flake8 .

# Type checking
typecheck:
	@echo "🔍 Type checking..."
	mypy .

# Run tests
test:
	@echo "🧪 Running tests..."
	pytest

# Run tests with coverage
test-cov:
	@echo "🧪 Running tests with coverage..."
	pytest --cov=. --cov-report=html --cov-report=term

# Clean up
clean:
	@echo "🧹 Cleaning up..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	rm -rf htmlcov/
	rm -rf .coverage

# Run all checks
all: format lint typecheck test
	@echo "✅ All checks completed!"

# Development setup
dev-setup: install format lint typecheck test
	@echo "🚀 Development environment ready!"

# Database commands
db-start:
	@echo "🐳 Starting database..."
	brew services start postgresql@17

db-stop:
	@echo "🐳 Stopping database..."
	brew services stop postgresql@17

db-init:
	@echo "🗄️  Initializing database..."
	alembic upgrade head

db-reset:
	@echo "🔄 Resetting database..."
	alembic downgrade base
	alembic upgrade head

db-status:
	@echo "📊 Migration status..."
	alembic current

db-history:
	@echo "📜 Migration history..."
	alembic history

# Filings data commands (see AGENTS.md for the full workflow)
ingest:
	@echo "📥 Ingesting SEC filings..."
	python -m filings.scripts.ingest \
		$(if $(TICKERS),--tickers $(TICKERS)) \
		$(if $(TICKERS_FILE),--tickers-file $(TICKERS_FILE)) \
		--forms $(FORMS) --limit $(LIMIT)

refresh:
	@echo "🔄 Refreshing financials..."
	@if [ -n "$(COMPANY_IDS)" ]; then \
		psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 \
			-c "CALL refresh_financials(ARRAY[$(COMPANY_IDS)]::int[]);"; \
	else \
		psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 \
			-c "DO 'DECLARE ids int[]; BEGIN SELECT array_agg(id) INTO ids FROM companies; CALL refresh_financials(ids); END';"; \
	fi

overrides-load:
	@echo "📤 Syncing override CSVs..."
	python -m filings.scripts.seed_overrides

overrides-check:
	@echo "🔎 Checking override CSVs (dry run)..."
	python -m filings.scripts.seed_overrides --dry-run

diagnose:
	@for f in sql/diagnostics/*.sql; do \
		echo ""; \
		echo "=== $$f ==="; \
		psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f "$$f" || exit 1; \
	done

psql:
	psql "$(DATABASE_URL)"

# Docker commands
docker-up:
	@echo "🐳 Starting services..."
	docker compose up -d

docker-down:
	@echo "🐳 Stopping services..."
	docker compose down

docker-cleanup:
	@echo "🧹 Cleaning up Docker resources (preserving volumes)..."
	docker system prune -f
	docker image prune -f

docker-cleanup-all:
	@echo "⚠️  WARNING: This will remove ALL Docker resources including volumes!"
	@echo "This will DELETE ALL DATABASE DATA!"
	@read -p "Are you sure? Type 'yes' to continue: " confirm && [ "$$confirm" = "yes" ]
	docker system prune -a -f --volumes

docker-volume-status:
	@echo "📊 Docker volume status..."
	docker volume ls | grep stonks-ai-py

docker-migrate:
	@echo "🗄️  Running database migrations..."
	docker compose --profile migrate up db_migrate

docker-logs:
	@echo "📋 Showing logs..."
	docker compose logs -f

docker-backup:
	@echo "💾 Creating database backup..."
	@mkdir -p backups
	docker compose exec postgres pg_dump -U rag_user rag_db > backups/backup_$$(date +%Y%m%d_%H%M%S).sql
	@echo "✅ Backup created in backups/ directory"

docker-restore:
	@echo "📥 Restoring database from backup..."
	@echo "Available backups:"
	@ls -la backups/*.sql 2>/dev/null || echo "No backups found"
	@read -p "Enter backup filename: " backup_file && \
	docker compose exec -T postgres psql -U rag_user rag_db < backups/$$backup_file
	@echo "✅ Database restored from $$backup_file"
