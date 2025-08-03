#!/bin/bash

# Development Environment Setup Script for RAG AI Python Project
# This script sets up a complete development environment with all necessary tools

set -e  # Exit on any error

echo "🚀 Setting up Python Development Environment for RAG AI Project"
echo "================================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if Python is installed
check_python() {
    print_info "Checking Python installation..."
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
        print_status "Python $PYTHON_VERSION is installed"
    else
        print_error "Python 3 is not installed. Please install Python 3.8 or higher."
        exit 1
    fi
}

# Create virtual environment
create_venv() {
    print_info "Creating virtual environment..."
    if [ ! -d "venv" ]; then
        python3 -m venv venv
        print_status "Virtual environment created"
    else
        print_warning "Virtual environment already exists"
    fi
}

# Activate virtual environment
activate_venv() {
    print_info "Activating virtual environment..."
    source venv/bin/activate
    print_status "Virtual environment activated"
}

# Install dependencies
install_dependencies() {
    print_info "Installing project dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt
    print_status "Project dependencies installed"
}

# Install development tools
install_dev_tools() {
    print_info "Installing development tools..."
    pip install black isort flake8 mypy pytest pytest-asyncio pytest-cov requests
    print_status "Development tools installed"
}

# Create .env file if it doesn't exist
setup_env() {
    print_info "Setting up environment variables..."
    if [ ! -f ".env" ]; then
        cp env.example .env
        print_warning "Created .env file from template. Please edit it with your API keys."
    else
        print_status ".env file already exists"
    fi
}

# Install pre-commit hooks
setup_pre_commit() {
    print_info "Setting up pre-commit hooks..."
    pip install pre-commit
    if [ ! -f ".pre-commit-config.yaml" ]; then
        cat > .pre-commit-config.yaml << EOF
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
      - id: check-merge-conflict

  - repo: https://github.com/psf/black
    rev: 23.11.0
    hooks:
      - id: black
        language_version: python3

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        args: ["--profile", "black"]

  - repo: https://github.com/pycqa/flake8
    rev: 6.1.0
    hooks:
      - id: flake8
        args: [--max-line-length=88]
EOF
        print_status "Created pre-commit configuration"
    fi
    pre-commit install
    print_status "Pre-commit hooks installed"
}

# Run initial tests
run_tests() {
    print_info "Running initial tests..."
    python test_setup.py
    print_status "Initial tests completed"
}

# Create useful aliases
create_aliases() {
    print_info "Creating useful aliases..."
    cat > .bash_aliases << EOF
# RAG AI Project Aliases
alias rag-run="python run.py"
alias rag-test="python test_setup.py"
alias rag-client="python example_client.py"
alias rag-format="black . && isort ."
alias rag-lint="flake8 ."
alias rag-type="mypy ."
alias rag-clean="find . -type d -name __pycache__ -exec rm -rf {} +"
alias rag-install="pip install -r requirements.txt"
alias rag-dev="pip install -e .[dev]"
EOF
    print_status "Created .bash_aliases file"
}

# Display next steps
show_next_steps() {
    echo ""
    echo "🎉 Development Environment Setup Complete!"
    echo "========================================"
    echo ""
    echo "Next steps:"
    echo "1. Edit .env file with your OPENAI_API_KEY"
    echo "2. Activate virtual environment: source venv/bin/activate"
    echo "3. Run the API server: python run.py"
    echo "4. Test the setup: python test_setup.py"
    echo "5. Try the example client: python example_client.py"
    echo ""
    echo "Useful commands:"
    echo "- Format code: black . && isort ."
    echo "- Lint code: flake8 ."
    echo "- Type check: mypy ."
    echo "- Run tests: pytest"
    echo ""
    echo "VS Code/Cursor:"
    echo "- Install recommended extensions"
    echo "- Use Ctrl+Shift+P to run tasks"
    echo "- Use F5 to debug"
    echo ""
}

# Main execution
main() {
    check_python
    create_venv
    activate_venv
    install_dependencies
    install_dev_tools
    setup_env
    setup_pre_commit
    run_tests
    create_aliases
    show_next_steps
}

# Run main function
main
