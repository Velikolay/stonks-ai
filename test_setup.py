#!/usr/bin/env python3
"""
Test script to verify the RAG system setup.
This script checks if all required dependencies are installed correctly.
"""

import os
import sys


def test_imports():
    """Test if all required packages can be imported."""
    print("Testing imports...")

    try:
        import llama_index

        print("✅ llama_index imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import llama_index: {e}")
        return False

    try:
        import llama_index.llms.openai

        print("✅ llama_index.llms.openai imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import llama_index.llms.openai: {e}")
        return False

    try:
        import llama_index.embeddings.openai

        print("✅ llama_index.embeddings.openai imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import llama_index.embeddings.openai: {e}")
        return False

    try:
        import llama_index.vector_stores.postgres

        print("✅ llama_index.vector_stores.postgres imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import llama_index.vector_stores.postgres: {e}")
        return False

    try:
        import fastapi

        print("✅ fastapi imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import fastapi: {e}")
        return False

    try:
        import uvicorn

        print("✅ uvicorn imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import uvicorn: {e}")
        return False

    try:
        import openai

        print("✅ openai imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import openai: {e}")
        return False

    try:
        import pg8000

        print("✅ pg8000 imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import pg8000: {e}")
        return False

    try:
        import requests

        print("✅ requests imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import requests: {e}")
        return False

    return True


def test_environment():
    """Test environment variables."""
    print("\nTesting environment...")

    openai_api_key = os.getenv("OPENAI_API_KEY")
    if openai_api_key:
        print("✅ OPENAI_API_KEY is set")
    else:
        print("⚠️  OPENAI_API_KEY is not set (you'll need this to run the app)")

    return True


def test_rag_system():
    """Test RAG system initialization (without API key)."""
    print("\nTesting RAG system initialization...")

    try:
        from rag_system import RAGSystem

        print("✅ RAGSystem class imported successfully")
    except ImportError as e:
        print(f"❌ Failed to import RAGSystem: {e}")
        return False

    # Test initialization without API key (should fail gracefully)
    try:
        # Temporarily remove API key
        original_key = os.environ.get("OPENAI_API_KEY")
        if original_key:
            del os.environ["OPENAI_API_KEY"]

        try:
            rag = RAGSystem()
            print("❌ RAGSystem initialized without API key (unexpected)")
            return False
        except ValueError as e:
            if "OPENAI_API_KEY" in str(e):
                print("✅ RAGSystem correctly requires OPENAI_API_KEY")
            else:
                print(f"❌ Unexpected error: {e}")
                return False
        finally:
            # Restore API key
            if original_key:
                os.environ["OPENAI_API_KEY"] = original_key

    except Exception as e:
        print(f"❌ Error testing RAG system: {e}")
        return False

    return True


def main():
    """Main test function."""
    print("🧪 RAG System Setup Test")
    print("=" * 40)

    all_tests_passed = True

    # Test imports
    if not test_imports():
        all_tests_passed = False

    # Test environment
    if not test_environment():
        all_tests_passed = False

    # Test RAG system
    if not test_rag_system():
        all_tests_passed = False

    print("\n" + "=" * 40)
    if all_tests_passed:
        print("✅ All tests passed! Your RAG system is ready to use.")
        print("\nNext steps:")
        print("1. Set your OPENAI_API_KEY in .env file")
        print("2. Run: python app.py")
        print("3. Test with: python example_client.py")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        print("\nTo install dependencies, run:")
        print("pip install -r requirements.txt")

    return all_tests_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
