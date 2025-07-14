#!/usr/bin/env python3
"""
Test script to verify user detection in API logging with user_id from payload
"""

import requests
import json
import sys

# Configuration
BASE_URL = "http://localhost:8000"  # Adjust if your server runs on a different port

def test_user_detection():
    """Test user detection with user_id in payload"""
    
    # Test data with user_id in payload
    test_payload = {
        "user_id": 1,  # Assuming user with ID 1 exists
        "project_id": 1,
        "file_type": "kpi",
        "file_name": "test_file.xlsx",
        "sheet_name": "Sheet1",
        "test_data": "This is a test payload"
    }
    
    print("=== Testing User Detection with user_id in Payload ===")
    print(f"Test payload: {json.dumps(test_payload, indent=2)}")
    
    try:
        # Test the TestAPILogging endpoint
        response = requests.post(
            f"{BASE_URL}/api/test-api-logging/",
            json=test_payload,
            headers={'Content-Type': 'application/json'}
        )
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("Response data:")
            print(json.dumps(result, indent=2))
            
            # Check if user detection worked
            if result.get('detected_user'):
                print("✅ User detection successful!")
                print(f"   User ID: {result['detected_user']['id']}")
                print(f"   Username: {result['detected_user']['username']}")
                print(f"   Email: {result['detected_user']['email']}")
            else:
                print("❌ User detection failed - no user found")
                
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to {BASE_URL}")
        print("Make sure your Django server is running on the correct port")
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")

def test_without_user_id():
    """Test without user_id in payload to see fallback behavior"""
    
    # Test data without user_id
    test_payload = {
        "project_id": 1,
        "file_type": "kpi",
        "file_name": "test_file.xlsx",
        "sheet_name": "Sheet1",
        "test_data": "This is a test payload without user_id"
    }
    
    print("\n=== Testing User Detection without user_id in Payload ===")
    print(f"Test payload: {json.dumps(test_payload, indent=2)}")
    
    try:
        # Test the TestAPILogging endpoint
        response = requests.post(
            f"{BASE_URL}/api/test-api-logging/",
            json=test_payload,
            headers={'Content-Type': 'application/json'}
        )
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("Response data:")
            print(json.dumps(result, indent=2))
            
            # Check if user detection worked
            if result.get('detected_user'):
                print("✅ User detection successful (fallback)!")
                print(f"   User ID: {result['detected_user']['id']}")
                print(f"   Username: {result['detected_user']['username']}")
                print(f"   Email: {result['detected_user']['email']}")
            else:
                print("❌ User detection failed - no user found")
                
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to {BASE_URL}")
        print("Make sure your Django server is running on the correct port")
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")

def test_invalid_user_id():
    """Test with invalid user_id to see error handling"""
    
    # Test data with invalid user_id
    test_payload = {
        "user_id": 99999,  # Assuming this user doesn't exist
        "project_id": 1,
        "file_type": "kpi",
        "file_name": "test_file.xlsx",
        "sheet_name": "Sheet1",
        "test_data": "This is a test payload with invalid user_id"
    }
    
    print("\n=== Testing User Detection with Invalid user_id ===")
    print(f"Test payload: {json.dumps(test_payload, indent=2)}")
    
    try:
        # Test the TestAPILogging endpoint
        response = requests.post(
            f"{BASE_URL}/api/test-api-logging/",
            json=test_payload,
            headers={'Content-Type': 'application/json'}
        )
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("Response data:")
            print(json.dumps(result, indent=2))
            
            # Check if user detection fell back to other methods
            if result.get('detected_user'):
                print("✅ User detection successful (fallback after invalid user_id)!")
                print(f"   User ID: {result['detected_user']['id']}")
                print(f"   Username: {result['detected_user']['username']}")
                print(f"   Email: {result['detected_user']['email']}")
            else:
                print("❌ User detection failed - no user found")
                
        else:
            print(f"❌ Request failed with status {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Could not connect to {BASE_URL}")
        print("Make sure your Django server is running on the correct port")
    except Exception as e:
        print(f"❌ Error during test: {str(e)}")

if __name__ == "__main__":
    print("Testing User Detection in API Logging")
    print("=" * 50)
    
    # Run all tests
    test_user_detection()
    test_without_user_id()
    test_invalid_user_id()
    
    print("\n" + "=" * 50)
    print("Test completed!")
    print("\nTo check the logs in Django admin:")
    print("1. Go to http://localhost:8000/admin/")
    print("2. Navigate to API Logs")
    print("3. Check that the 'User' column is populated correctly") 