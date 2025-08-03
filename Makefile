.PHONY: help install format lint test clean db-init db-reset db-status

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

# Docker commands
docker-up:
	@echo "🐳 Starting services..."
	docker compose up -d

docker-down:
	@echo "🐳 Stopping services..."
	docker compose down

docker-migrate:
	@echo "🗄️  Running database migrations..."
	docker compose --profile migrate up db_migrate

docker-logs:
	@echo "📋 Showing logs..."
	docker compose logs -f
