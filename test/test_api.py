#!/usr/bin/env python3

import requests
import json

def test_api():
    base_url = "http://localhost:8000"
    
    print("🧪 Testing Retail Sales API")
    print("=" * 40)
    
    try:
        # Test root endpoint
        print("\n📋 Testing root endpoint...")
        response = requests.get(f"{base_url}/")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            print("Response:", json.dumps(response.json(), indent=2))
        
        # Test summary endpoint
        print("\n📊 Testing summary endpoint...")
        response = requests.get(f"{base_url}/summary/")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Total Records: {data['total_records']:,}")
            print(f"Total Revenue: SGD ${data['total_revenue']:,.2f}")
            print(f"Unique Customers: {data['unique_customers']:,}")
            print(f"Date Range: {data['date_range_start']} to {data['date_range_end']}")
            print(f"Top Product: {data['top_products'][0]['product_name']} (SGD ${data['top_products'][0]['total_revenue']:,.2f})")
        
        # Test sales endpoint with pagination
        print("\n🛒 Testing sales endpoint...")
        response = requests.get(f"{base_url}/sales/?page=1&page_size=5")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Page: {data['page']}, Page Size: {data['page_size']}")
            print(f"Total Records: {data['total_records']:,}")
            print(f"First sale: {data['sales'][0]['product_name']} - SGD ${data['sales'][0]['total_amount_per_product_sgd']}")
        
        # Test products endpoint
        print("\n🏷️ Testing products endpoint...")
        response = requests.get(f"{base_url}/products/")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            products = response.json()
            print(f"Number of products: {len(products)}")
            for i, product in enumerate(products[:3]):
                print(f"{i+1}. {product['product_name']}: {product['total_units_sold']:,} units, SGD ${product['total_revenue']:,.2f}")
        
        # Test specific date
        print("\n📅 Testing date-specific endpoint...")
        response = requests.get(f"{base_url}/sales/by-date/2024-01-01")
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print("Sales on 2024-01-01:")
            print(f"  Transactions: {data['transaction_count']}")
            print(f"  Revenue: SGD ${data['daily_revenue']:,.2f}")
            print(f"  Customers: {data['unique_customers']}")
        
        print("\n✅ API testing completed successfully!")
        
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to API server. Make sure it's running on localhost:8000")
    except Exception as e:
        print(f"❌ Error during testing: {e}")

if __name__ == "__main__":
    test_api()
