#!/usr/bin/env python3
"""
Test script for EDA Plot Download API
"""

import requests
import json
import os

def test_eda_download_api():
    """Test the EDA Plot Download API"""
    
    # API endpoint
    url = "http://127.0.0.1:8000/api/download-eda-plots-excel/"
    
    # Test data - using the values from the error message
    test_data = {
        "user_id": "22",  # String as sent by frontend
        "project_id": 274,  # Integer as sent by frontend
        "plot_ids": []  # Empty array means download all plots
    }
    
    try:
        print("Testing EDA Plot Download API...")
        print(f"URL: {url}")
        print(f"Request data: {json.dumps(test_data, indent=2)}")
        
        # Make the request
        response = requests.post(url, json=test_data)
        
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            # Save the file
            filename = "test_eda_plots.xlsx"
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"✅ Success! Downloaded file saved as: {filename}")
            print(f"File size: {len(response.content)} bytes")
            
            # Check if file exists and has content
            if os.path.exists(filename) and os.path.getsize(filename) > 0:
                print("✅ File created successfully and has content")
                
                # Try to open with pandas to check structure
                try:
                    import pandas as pd
                    xl_file = pd.ExcelFile(filename)
                    print(f"✅ Excel file has {len(xl_file.sheet_names)} sheets:")
                    for sheet_name in xl_file.sheet_names:
                        df = pd.read_excel(filename, sheet_name=sheet_name)
                        print(f"  - {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
                        if len(df) > 0:
                            print(f"    Columns: {list(df.columns)}")
                            print(f"    First few rows:")
                            print(df.head(3).to_string())
                except Exception as e:
                    print(f"⚠️  Could not read Excel file with pandas: {e}")
            else:
                print("❌ File creation failed or file is empty")
                
        else:
            print(f"❌ Error: {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error details: {json.dumps(error_data, indent=2)}")
            except:
                print(f"Error response: {response.text}")
                
    except requests.exceptions.ConnectionError:
        print("❌ Connection error: Make sure the Django server is running on http://127.0.0.1:8000")
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")

def test_api_without_plots():
    """Test the API when no plots exist"""
    
    url = "http://127.0.0.1:8000/api/download-eda-plots-excel/"
    
    # Test with a project that likely has no plots
    test_data = {
        "user_id": "999",  # Non-existent user
        "project_id": 999,  # Non-existent project
        "plot_ids": []
    }
    
    try:
        print("\nTesting API with non-existent project...")
        response = requests.post(url, json=test_data)
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 404:
            print("✅ Correctly returned 404 for non-existent project")
        else:
            print(f"❌ Unexpected response: {response.status_code}")
            try:
                error_data = response.json()
                print(f"Response: {json.dumps(error_data, indent=2)}")
            except:
                print(f"Response: {response.text}")
                
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    print("=" * 50)
    print("EDA Plot Download API Test")
    print("=" * 50)
    
    # Test with valid data
    test_eda_download_api()
    
    # Test with invalid data
    test_api_without_plots()
    
    print("\n" + "=" * 50)
    print("Test completed!")
    print("=" * 50) 