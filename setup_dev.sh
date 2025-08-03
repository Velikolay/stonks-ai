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

# Run initial tests
run_tests() {
    print_info "Running initial tests..."
    python test_setup.py
    print_status "Initial tests completed"
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
    echo ""
}

# Main execution
main() {
    check_python
    create_venv
    activate_venv
    install_dependencies
    setup_env
    run_tests
    show_next_steps
}

# Run main function
main
