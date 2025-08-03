#!/usr/bin/env python3
"""
Script to run the RAG API server.
"""

import os
import sys

import uvicorn
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def check_environment() -> bool:
    """Check if required environment variables are set."""
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        print("❌ OPENAI_API_KEY environment variable is not set")
        print("Please set it in your .env file:")
        print("OPENAI_API_KEY=your_openai_api_key_here")
        return False

    print("✅ Environment variables are set")
    return True


def main() -> None:
    """Main function to run the server."""
    print("🚀 Starting RAG API Server")
    print("=" * 40)

    # Check environment
    if not check_environment():
        sys.exit(1)

    # Start server
    print("🌐 Starting server on http://localhost:8000")
    print("📚 API documentation available at http://localhost:8000/docs")
    print("🔄 Press Ctrl+C to stop the server")
    print()

    try:
        uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
