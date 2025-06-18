#!/usr/bin/env python3
"""
Test script for Project Sharing functionality
This script demonstrates how to use the project sharing API endpoints
"""

import requests
import json

# Configuration
BASE_URL = "http://localhost:8000/api"
TEST_USER_1_EMAIL = "user1@example.com"
TEST_USER_2_EMAIL = "user2@example.com"
TEST_PASSWORD = "testpassword123"

def create_test_user(email, username):
    """Create a test user"""
    url = f"{BASE_URL}/auth/signup/"
    data = {
        "email": email,
        "username": username,
        "password": TEST_PASSWORD
    }
    response = requests.post(url, json=data)
    return response.json()

def signin_user(email):
    """Sign in a user and get user ID"""
    url = f"{BASE_URL}/auth/signin/"
    data = {
        "email": email,
        "password": TEST_PASSWORD
    }
    response = requests.post(url, json=data)
    if response.status_code == 200:
        return response.json().get('user_id')
    return None

def create_test_project(user_id, project_name):
    """Create a test project"""
    url = f"{BASE_URL}/auth/upload/"
    data = {
        "user_id": user_id,
        "project_name": project_name
    }
    # Note: This would normally include file uploads
    # For testing, we'll assume the project exists
    response = requests.post(url, json=data)
    return response.json()

def test_project_sharing():
    """Test the complete project sharing workflow"""
    print("=== Project Sharing Test ===\n")
    
    # Step 1: Create test users
    print("1. Creating test users...")
    user1_response = create_test_user(TEST_USER_1_EMAIL, "user1")
    user2_response = create_test_user(TEST_USER_2_EMAIL, "user2")
    
    print(f"User 1 created: {user1_response}")
    print(f"User 2 created: {user2_response}\n")
    
    # Step 2: Sign in users to get their IDs
    print("2. Signing in users...")
    user1_id = signin_user(TEST_USER_1_EMAIL)
    user2_id = signin_user(TEST_USER_2_EMAIL)
    
    print(f"User 1 ID: {user1_id}")
    print(f"User 2 ID: {user2_id}\n")
    
    if not user1_id or not user2_id:
        print("Failed to get user IDs. Exiting.")
        return
    
    # Step 3: Create a test project (assuming it exists)
    print("3. Creating test project...")
    project_name = "Test Project for Sharing"
    # Note: In a real scenario, you would upload files here
    # For this test, we'll assume project ID 1 exists
    project_id = 1
    
    print(f"Using project ID: {project_id}\n")
    
    # Step 4: Share the entire project
    print("4. Sharing entire project...")
    share_url = f"{BASE_URL}/share-project/"
    share_data = {
        "project_id": project_id,
        "shared_by_user_id": user1_id,
        "shared_with_email": TEST_USER_2_EMAIL,
        "share_type": "project",
        "permission_level": "view"
    }
    
    share_response = requests.post(share_url, json=share_data)
    print(f"Share response: {share_response.json()}\n")
    
    # Step 5: Get shared projects for user 2
    print("5. Getting shared projects for user 2...")
    shared_projects_url = f"{BASE_URL}/get-shared-projects/"
    shared_projects_data = {
        "user_id": user2_id
    }
    
    shared_projects_response = requests.post(shared_projects_url, json=shared_projects_data)
    print(f"Shared projects response: {shared_projects_response.json()}\n")
    
    # Step 6: Get shared project details
    print("6. Getting shared project details...")
    project_details_url = f"{BASE_URL}/get-shared-project-details/"
    project_details_data = {
        "user_id": user2_id,
        "project_id": project_id
    }
    
    project_details_response = requests.post(project_details_url, json=project_details_data)
    print(f"Project details response: {project_details_response.json()}\n")
    
    # Step 7: Share a specific file (if files exist)
    print("7. Sharing specific file...")
    file_share_data = {
        "project_id": project_id,
        "shared_by_user_id": user1_id,
        "shared_with_email": TEST_USER_2_EMAIL,
        "share_type": "file",
        "permission_level": "edit",
        "file_type": "media",
        "file_name": "test_data.xlsx",
        "sheet_name": "Sheet1"
    }
    
    file_share_response = requests.post(share_url, json=file_share_data)
    print(f"File share response: {file_share_response.json()}\n")
    
    # Step 8: Get shared project plots
    print("8. Getting shared project plots...")
    plots_url = f"{BASE_URL}/get-shared-project-plots/"
    plots_data = {
        "user_id": user2_id,
        "project_id": project_id
    }
    
    plots_response = requests.post(plots_url, json=plots_data)
    print(f"Plots response: {plots_response.json()}\n")
    
    # Step 9: Remove project share
    print("9. Removing project share...")
    remove_share_url = f"{BASE_URL}/remove-project-share/"
    remove_share_data = {
        "project_id": project_id,
        "shared_by_user_id": user1_id,
        "shared_with_email": TEST_USER_2_EMAIL
    }
    
    remove_share_response = requests.post(remove_share_url, json=remove_share_data)
    print(f"Remove share response: {remove_share_response.json()}\n")
    
    print("=== Test completed ===")

def test_error_scenarios():
    """Test error scenarios"""
    print("=== Error Scenarios Test ===\n")
    
    # Test 1: Share with non-existent user
    print("1. Testing share with non-existent user...")
    share_url = f"{BASE_URL}/share-project/"
    share_data = {
        "project_id": 1,
        "shared_by_user_id": 1,
        "shared_with_email": "nonexistent@example.com",
        "share_type": "project",
        "permission_level": "view"
    }
    
    response = requests.post(share_url, json=share_data)
    print(f"Response: {response.json()}\n")
    
    # Test 2: Share with invalid permission level
    print("2. Testing share with invalid permission level...")
    share_data["shared_with_email"] = TEST_USER_2_EMAIL
    share_data["permission_level"] = "invalid_level"
    
    response = requests.post(share_url, json=share_data)
    print(f"Response: {response.json()}\n")
    
    # Test 3: File share without required fields
    print("3. Testing file share without required fields...")
    file_share_data = {
        "project_id": 1,
        "shared_by_user_id": 1,
        "shared_with_email": TEST_USER_2_EMAIL,
        "share_type": "file",
        "permission_level": "view"
        # Missing file_type, file_name, sheet_name
    }
    
    response = requests.post(share_url, json=file_share_data)
    print(f"Response: {response.json()}\n")
    
    print("=== Error scenarios test completed ===")

if __name__ == "__main__":
    print("Starting Project Sharing API Tests...\n")
    
    # Run main functionality test
    test_project_sharing()
    
    print("\n" + "="*50 + "\n")
    
    # Run error scenarios test
    test_error_scenarios()
    
    print("\nAll tests completed!") 