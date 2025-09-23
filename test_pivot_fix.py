#!/usr/bin/env python3
"""
Test script to verify the PivotTableAPI fix for cases with only rows and values (no columns).
This script tests the specific payload provided by the user.
"""

import requests
import json

def test_pivot_table_api():
    """Test the PivotTableAPI with the provided payload"""
    
    # Test payload from the user
    payload = {
        "user_id": "22",
        "project_id": 307,
        "file_type": "kpi",
        "file_name": "Sales Loreal_null (3)",
        "sheet_name": "Sales Loreal_null (3).csv",
        "rows": ["Period_year", "kpi"],
        "columns": [],
        "values": ["value"],
        "aggregation": "sum",
        "mapping_configurations": {},
        "layout_options": {
            "grand_totals": False,
            "row_totals": False,
            "show_empty_items": False,
            "compact_layout": False
        }
    }
    
    # API endpoint (adjust the URL as needed)
    url = "http://localhost:8000/api/pivot-table/"
    
    try:
        print("Testing PivotTableAPI with payload:")
        print(json.dumps(payload, indent=2))
        print("\n" + "="*50 + "\n")
        
        response = requests.post(url, json=payload)
        
        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            response_data = response.json()
            print("✅ SUCCESS: API returned 200")
            print(f"Response keys: {list(response_data.keys())}")
            
            if 'pivot_table' in response_data:
                pivot_data = response_data['pivot_table']
                print(f"Number of pivot rows: {len(pivot_data)}")
                
                if pivot_data:
                    print(f"First row keys: {list(pivot_data[0].keys())}")
                    print(f"First row sample: {pivot_data[0]}")
                    
                    # Check if the 'value' column is present with proper values
                    first_row = pivot_data[0]
                    if 'value' in first_row:
                        print(f"✅ 'value' column found with value: {first_row['value']}")
                    else:
                        print(f"❌ 'value' column not found. Available columns: {list(first_row.keys())}")
                else:
                    print("❌ No pivot data returned")
            else:
                print("❌ 'pivot_table' key not found in response")
                
            if 'columns' in response_data:
                print(f"Series columns: {response_data['columns']}")
                
            if 'summary' in response_data:
                summary = response_data['summary']
                print(f"Summary - Row fields: {summary.get('row_fields', [])}")
                print(f"Summary - Value fields: {summary.get('value_fields', [])}")
                print(f"Summary - Column fields: {summary.get('column_fields', [])}")
                
        else:
            print(f"❌ FAILED: API returned {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error response: {json.dumps(error_data, indent=2)}")
            except:
                print(f"Error response (text): {response.text}")
                
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Make sure the Django server is running on localhost:8000")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    test_pivot_table_api()
