#!/usr/bin/env python3
"""
Simple Large Page Test - MVP approach for efficient data retrieval
Uses large page sizes instead of complex streaming for better performance and simplicity
"""

import requests
import time
import json

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"
PAGE_SIZE = 100000  # Use maximum allowed page size

def get_all_records_with_large_pages():
    """Get all records using large page sizes - Simple and efficient for MVP"""
    print(f"🚀 Retrieving all records using large pages (size={PAGE_SIZE:,})...")
    
    all_records = []
    page = 1
    total_start_time = time.perf_counter()
    
    while True:
        print(f"\n📄 Fetching page {page}...")
        page_start_time = time.perf_counter()
        
        try:
            response = requests.get(
                f"{BASE_URL}/api/v1/records/{SCHEMA_NAME}",
                params={
                    "page": page,
                    "size": PAGE_SIZE
                },
                timeout=60  # 1 minute timeout per page
            )
            
            if response.status_code != 200:
                print(f"❌ HTTP Error {response.status_code}: {response.text}")
                break
            
            data = response.json()
            page_records = data["data"]["items"]
            total_records = data["data"]["total"]
            
            page_duration = time.perf_counter() - page_start_time
            page_throughput = len(page_records) / page_duration if page_duration > 0 else 0
            
            print(f"✅ Page {page}: {len(page_records):,} records in {page_duration:.2f}s ({int(page_throughput):,} records/sec)")
            print(f"   Progress: {len(all_records) + len(page_records):,}/{total_records:,} records")
            
            all_records.extend(page_records)
            
            # Check if we got all records
            if len(page_records) < PAGE_SIZE:
                print(f"📊 Reached end of data (got {len(page_records):,} < {PAGE_SIZE:,})")
                break
            
            page += 1
            
        except requests.exceptions.Timeout:
            print(f"❌ Timeout on page {page}")
            break
        except Exception as e:
            print(f"❌ Error on page {page}: {str(e)}")
            break
    
    total_duration = time.perf_counter() - total_start_time
    total_throughput = len(all_records) / total_duration if total_duration > 0 else 0
    
    print(f"\n📊 FINAL RESULTS:")
    print(f"   Total records: {len(all_records):,}")
    print(f"   Total pages: {page}")
    print(f"   Total duration: {total_duration:.2f}s")
    print(f"   Average throughput: {int(total_throughput):,} records/sec")
    print(f"   Average per page: {total_duration/page:.2f}s")
    
    return all_records

def get_record_count():
    """Get total record count first"""
    print("📊 Getting total record count...")
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/records/{SCHEMA_NAME}/count",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            count = data["count"]
            print(f"✅ Total records in database: {count:,}")
            return count
        else:
            print(f"❌ Count failed: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Count error: {str(e)}")
        return None

def test_sample_data():
    """Test with a small sample first"""
    print("🧪 Testing with small sample (1,000 records)...")
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/records/{SCHEMA_NAME}",
            params={"page": 1, "size": 1000},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            records = data["data"]["items"]
            print(f"✅ Sample test passed: {len(records):,} records retrieved")
            
            # Show sample record structure
            if records:
                sample_record = records[0]
                print(f"📋 Sample record structure:")
                print(f"   ID: {sample_record.get('id', 'N/A')}")
                print(f"   Schema: {sample_record.get('schema_name', 'N/A')}")
                print(f"   Data keys: {list(sample_record.get('data', {}).keys())[:5]}...")
                
            return True
        else:
            print(f"❌ Sample test failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Sample test error: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Simple Large Page Test - MVP Data Retrieval")
    print("=" * 60)
    print("This demonstrates efficient data retrieval using large page sizes")
    print("instead of complex streaming - perfect for MVP!")
    print("=" * 60)
    
    # Check server
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        if response.status_code == 200:
            print("✅ Server is running")
        else:
            print("❌ Server not responding correctly")
            exit(1)
    except:
        print("❌ Cannot connect to server")
        exit(1)
    
    # Test with sample first
    if not test_sample_data():
        print("❌ Sample test failed, aborting")
        exit(1)
    
    # Get record count
    total_count = get_record_count()
    if total_count:
        estimated_pages = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
        print(f"📊 Estimated pages needed: {estimated_pages} (with {PAGE_SIZE:,} records per page)")
    
    # Get all records
    print(f"\n{'='*60}")
    all_records = get_all_records_with_large_pages()
    
    if all_records:
        print(f"\n✅ SUCCESS! Retrieved {len(all_records):,} records efficiently")
        print(f"🎯 MVP Approach: Simple, fast, and reliable!")
        print(f"📈 No complex streaming needed - just use large page sizes!")
    else:
        print(f"\n❌ Failed to retrieve records") 