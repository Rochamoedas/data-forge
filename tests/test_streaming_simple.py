#!/usr/bin/env python3
"""
Simple Streaming Test - Quick verification that streaming works
"""

import requests
import json
import time

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"

def test_simple_streaming():
    """Simple streaming test with optimized batch size"""
    print("🧪 Testing streaming with optimized batch size (50,000 records)...")
    
    start_time = time.perf_counter()
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/high-performance/stream-arrow-batches/{SCHEMA_NAME}?batch_size=50000",
            stream=True,
            timeout=60  # 1 minute timeout for optimized test
        )
        
        print(f"📡 Response status: {response.status_code}")
        
        if response.status_code == 200:
            batch_count = 0
            total_records = 0
            line_count = 0
            
            for line in response.iter_lines(decode_unicode=True):
                line_count += 1
                if line and line.strip():
                    try:
                        line_data = json.loads(line.strip())
                        
                        if "stream_type" in line_data:
                            print(f"📡 Stream started: {line_data.get('schema_name')} with batch size {line_data.get('batch_size')}")
                        
                        elif "batch_number" in line_data:
                            batch_count += 1
                            batch_size_actual = line_data.get("batch_size", 0)
                            total_records += batch_size_actual
                            
                            elapsed = time.perf_counter() - start_time
                            current_throughput = total_records / elapsed if elapsed > 0 else 0
                            
                            print(f"📦 Batch {batch_count}: {batch_size_actual:,} records (Total: {total_records:,}, {int(current_throughput):,} records/sec)")
                        
                        elif "stream_complete" in line_data:
                            print(f"✅ Stream completed: {line_data}")
                            break
                            
                    except json.JSONDecodeError as e:
                        print(f"⚠️ JSON decode error on line {line_count}: {e}")
                        continue
            
            duration = time.perf_counter() - start_time
            throughput = total_records / duration if duration > 0 else 0
            
            print(f"\n📊 RESULTS:")
            print(f"   Duration: {duration:.2f}s")
            print(f"   Batches: {batch_count}")
            print(f"   Records: {total_records:,}")
            print(f"   Throughput: {int(throughput):,} records/sec")
            print(f"   Lines processed: {line_count}")
            
            return True
        else:
            print(f"❌ HTTP Error {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 Simple Streaming Test")
    print("=" * 50)
    
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
    
    # Run test
    success = test_simple_streaming()
    
    if success:
        print("\n✅ Streaming test PASSED!")
    else:
        print("\n❌ Streaming test FAILED!") 