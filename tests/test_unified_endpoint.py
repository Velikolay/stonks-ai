#!/usr/bin/env python3
"""
Test script for the unified EDGAR filing processing endpoint.
This demonstrates how to use the single endpoint that handles everything.
"""

import json

import requests

# API base URL
BASE_URL = "http://localhost:8000"


def test_health_check():
    """Test the health check endpoint."""
    print("🔍 Testing health check...")
    try:
        response = requests.get(f"{BASE_URL}/")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        assert response.status_code == 200
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        assert False, f"Health check failed: {e}"


def test_process_filing():
    """Test the unified filing processing endpoint."""
    print("\n📥 Testing unified filing processing...")

    # Test data
    payload = {
        "ticker": "AAPL",
        "company_name": "Apple Inc.",
        "filing_date": "2024-01-15",  # Example date
        "form_type": "10-Q",
        "save_csv": True,
        "csv_filename": "apple_test_filing.csv",
        "store_in_database": True,
    }

    try:
        print(f"Sending request: {json.dumps(payload, indent=2)}")

        response = requests.post(
            f"{BASE_URL}/edgar/process-filing",
            json=payload,
            timeout=300,  # 5 minutes timeout for processing
        )

        print(f"Status: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Success!")
            print(f"Message: {result['message']}")
            print(
                f"Processing Time: {result.get('processing_time_seconds', 'N/A')} seconds"
            )
            print(f"Database Stored: {result.get('database_stored', False)}")
            print(f"CSV Saved: {result.get('csv_saved', False)}")
            print(f"Company ID: {result.get('company_id', 'N/A')}")
            print(f"Filing ID: {result.get('filing_id', 'N/A')}")
            print(f"Facts Count: {result.get('facts_count', 'N/A')}")

            if result.get("error"):
                print(f"⚠️  Warning: {result['error']}")

        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")

    except requests.exceptions.Timeout:
        print("⏰ Request timed out (this is normal for large filings)")
    except Exception as e:
        print(f"❌ Request failed: {e}")


def test_process_filing_csv_only():
    """Test the endpoint with CSV-only processing (no database)."""
    print("\n📄 Testing CSV-only processing...")

    payload = {
        "ticker": "MSFT",
        "company_name": "Microsoft Corporation",
        "filing_date": "2024-01-15",
        "form_type": "10-Q",
        "save_csv": True,
        "csv_filename": "microsoft_test_filing.csv",
        "store_in_database": False,  # CSV only
    }

    try:
        response = requests.post(
            f"{BASE_URL}/edgar/process-filing", json=payload, timeout=300
        )

        print(f"Status: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Success!")
            print(f"Message: {result['message']}")
            print(f"CSV Saved: {result.get('csv_saved', False)}")
            print(f"CSV Filename: {result.get('csv_filename', 'N/A')}")
            print(f"Database Stored: {result.get('database_stored', False)}")
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f"❌ Request failed: {e}")


def main():
    """Run all tests."""
    print("🚀 Testing Unified EDGAR Filing Processing Endpoint")
    print("=" * 60)

    # Test health check
    if not test_health_check():
        print("❌ Health check failed, stopping tests")
        return

    # Test full processing (database + CSV)
    test_process_filing()

    # Test CSV-only processing
    test_process_filing_csv_only()

    print("\n" + "=" * 60)
    print("🎉 Testing complete!")
    print("\n📚 Usage Examples:")
    print(
        "- Full processing (database + CSV): Set store_in_database=True, save_csv=True"
    )
    print("- Database only: Set store_in_database=True, save_csv=False")
    print("- CSV only: Set store_in_database=False, save_csv=True")
    print("- Download only: Set store_in_database=False, save_csv=False")


if __name__ == "__main__":
    main()
