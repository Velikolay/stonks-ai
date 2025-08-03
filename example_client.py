#!/usr/bin/env python3
"""
Example client for the RAG API.
This script demonstrates how to interact with the RAG API.
"""

import json
import os
from typing import Any, Dict

import requests


class RAGClient:
    """Client for interacting with the RAG API."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url

    def health_check(self) -> Dict[str, Any]:
        """Check if the API is running."""
        response = requests.get(f"{self.base_url}/")
        return response.json()

    def upload_document(self, file_path: str) -> Dict[str, Any]:
        """Upload a document to the RAG system."""
        with open(file_path, "rb") as f:
            files = {"file": f}
            response = requests.post(f"{self.base_url}/upload", files=files)
            return response.json()

    def query(self, query: str, top_k: int = 3) -> Dict[str, Any]:
        """Query the RAG system."""
        data = {"query": query, "top_k": top_k}
        response = requests.post(f"{self.base_url}/query", json=data)
        return response.json()

    def get_document_count(self) -> Dict[str, Any]:
        """Get the number of documents in the system."""
        response = requests.get(f"{self.base_url}/documents/count")
        return response.json()

    def clear_documents(self) -> Dict[str, Any]:
        """Clear all documents from the system."""
        response = requests.delete(f"{self.base_url}/documents/clear")
        return response.json()


def create_sample_document():
    """Create a sample document for testing."""
    sample_text = """
    Artificial Intelligence and Machine Learning

    Artificial Intelligence (AI) is a branch of computer science that aims to create
    intelligent machines that work and react like humans. Some of the activities
    computers with artificial intelligence are designed for include speech recognition,
    learning, planning, and problem solving.

    Machine Learning is a subset of AI that provides systems the ability to automatically
    learn and improve from experience without being explicitly programmed. Machine learning
    focuses on the development of computer programs that can access data and use it to
    learn for themselves.

    Deep Learning is a subset of machine learning that uses neural networks with multiple
    layers to model and understand complex patterns in data. It has been particularly
    successful in areas like image recognition, natural language processing, and speech
    recognition.

    The field of AI has seen tremendous growth in recent years, with applications
    ranging from virtual assistants like Siri and Alexa to autonomous vehicles and
    medical diagnosis systems. Companies are increasingly adopting AI technologies to
    improve efficiency, reduce costs, and create new products and services.

    However, AI also raises important ethical considerations, including concerns about
    privacy, bias, job displacement, and the potential for misuse. It's important for
    developers and users of AI systems to consider these implications and work towards
    responsible AI development and deployment.
    """

    with open("sample_document.txt", "w") as f:
        f.write(sample_text)

    return "sample_document.txt"


def main():
    """Main function to demonstrate the RAG API."""
    client = RAGClient()

    print("🤖 RAG API Client Demo")
    print("=" * 50)

    # Health check
    print("\n1. Checking API health...")
    try:
        health = client.health_check()
        print(f"✅ API Status: {health}")
    except requests.exceptions.ConnectionError:
        print(
            "❌ Could not connect to API. Make sure the server is running with: python app.py"
        )
        return

    # Create sample document
    print("\n2. Creating sample document...")
    sample_file = create_sample_document()
    print(f"✅ Created: {sample_file}")

    # Upload document
    print("\n3. Uploading document...")
    try:
        upload_result = client.upload_document(sample_file)
        print(f"✅ Upload Result: {upload_result}")
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return

    # Get document count
    print("\n4. Checking document count...")
    count_result = client.get_document_count()
    print(f"✅ Document count: {count_result}")

    # Query examples
    print("\n5. Testing queries...")

    queries = [
        "What is artificial intelligence?",
        "What is machine learning and how does it relate to AI?",
        "What are some applications of AI?",
        "What ethical considerations are mentioned regarding AI?",
    ]

    for i, query in enumerate(queries, 1):
        print(f"\n   Query {i}: {query}")
        try:
            result = client.query(query)
            print(f"   Answer: {result['answer'][:200]}...")
            print(f"   Sources: {result['sources']}")
        except Exception as e:
            print(f"   ❌ Query failed: {e}")

    # Clean up
    print("\n6. Cleaning up...")
    try:
        os.remove(sample_file)
        print(f"✅ Removed: {sample_file}")
    except:
        pass

    print("\n🎉 Demo completed!")


if __name__ == "__main__":
    main()
