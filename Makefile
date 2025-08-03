.PHONY: help install format lint test clean

# Default target
help:
	@echo "Available commands:"
	@echo "  install    - Install dependencies"
	@echo "  format     - Format code with black and isort"
	@echo "  lint       - Run flake8 linting"
	@echo "  test       - Run tests"
	@echo "  clean      - Clean up cache files"
	@echo "  all        - Run format, lint, and test"

# Install dependencies
install:
	pip install -r requirements.txt

# Format code
format:
	@echo "🎨 Formatting code..."
	black .
	isort .

# Lint code
lint:
	@echo "🔍 Linting code..."
	flake8 .

# Run tests
test:
	@echo "🧪 Running tests..."
	python test_setup.py

# Clean up
clean:
	@echo "🧹 Cleaning up..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +

# Run all checks
all: format lint test
	@echo "✅ All checks completed!"

# Development setup
dev-setup: install format lint test
	@echo "🚀 Development environment ready!"
