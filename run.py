#!/usr/bin/env python3
"""
Startup script for the RAG API.
This script checks the environment and starts the FastAPI server.
"""

import os
import sys

from dotenv import load_dotenv


def check_environment():
    """Check if the environment is properly configured."""
    load_dotenv()

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        print("❌ OPENAI_API_KEY is not set!")
        print("\nPlease set your OpenAI API key:")
        print("1. Copy env.example to .env")
        print("2. Edit .env and add your OPENAI_API_KEY")
        print("3. Run this script again")
        return False

    print("✅ Environment is properly configured")
    return True


def main():
    """Main function to start the API server."""
    print("🚀 Starting RAG API Server")
    print("=" * 40)

    # Check environment
    if not check_environment():
        sys.exit(1)

    # Import and run the app
    try:
        import uvicorn

        from app import app

        print("✅ All dependencies loaded successfully")
        print("🌐 Starting server at http://localhost:8000")
        print("📚 API documentation: http://localhost:8000/docs")
        print("🛑 Press Ctrl+C to stop the server")
        print("=" * 40)

        uvicorn.run(app, host="0.0.0.0", port=8000)

    except ImportError as e:
        print(f"❌ Failed to import dependencies: {e}")
        print("\nPlease install dependencies:")
        print("pip install -r requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Failed to start server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
