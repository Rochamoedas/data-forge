#!/usr/bin/env python3
"""
Simple Streaming Test - Debug performance issues
"""

import requests
import json
import time

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"

def test_simple_streaming(batch_size: int = 250000):
    """Simple streaming test with detailed logging"""
    print(f"🧪 Testing streaming with batch_size={batch_size:,}")
    
    start_time = time.perf_counter()
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/high-performance/stream-arrow-batches/{SCHEMA_NAME}?batch_size={batch_size}",
            stream=True,
            timeout=180  # 3 minutes timeout
        )
        
        print(f"📡 Response status: {response.status_code}")
        print(f"📡 Response headers: {dict(response.headers)}")
        
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
                            print(f"📡 Stream metadata: {line_data}")
                        
                        elif "batch_number" in line_data:
                            batch_count += 1
                            batch_size_actual = line_data.get("batch_size", 0)
                            total_records += batch_size_actual
                            
                            elapsed = time.perf_counter() - start_time
                            current_throughput = total_records / elapsed if elapsed > 0 else 0
                            
                            print(f"📦 Batch {batch_count}: {batch_size_actual:,} records")
                            print(f"   Total so far: {total_records:,} records in {elapsed:.2f}s")
                            print(f"   Current throughput: {int(current_throughput):,} records/sec")
                            print(f"   Memory usage: Processing batch {batch_count}")
                        
                        elif "stream_complete" in line_data:
                            print(f"✅ Stream completed: {line_data}")
                            break
                            
                    except json.JSONDecodeError as e:
                        print(f"⚠️ JSON decode error on line {line_count}: {e}")
                        print(f"   Line content: {line[:100]}...")
                        continue
                else:
                    print(f"⚠️ Empty line {line_count}")
            
            duration = time.perf_counter() - start_time
            throughput = total_records / duration if duration > 0 else 0
            
            print(f"\n📊 FINAL RESULTS:")
            print(f"   Total duration: {duration:.2f}s")
            print(f"   Total batches: {batch_count}")
            print(f"   Total records: {total_records:,}")
            print(f"   Average throughput: {int(throughput):,} records/sec")
            print(f"   Total lines processed: {line_count}")
            
            return True
        else:
            print(f"❌ HTTP Error {response.status_code}: {response.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out")
        return False
    except Exception as e:
        print(f"❌ Request failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 SIMPLE STREAMING TEST")
    print("=" * 50)
    
    # Test with optimized batch size
    success = test_simple_streaming(250000)
    
    if success:
        print("\n✅ Streaming test completed successfully!")
    else:
        print("\n❌ Streaming test failed!")
        
        # Try with smaller batch size
        print("\n🔄 Trying with smaller batch size...")
        success = test_simple_streaming(100000)
        
        if success:
            print("\n✅ Smaller batch size worked!")
        else:
            print("\n❌ Even smaller batch size failed!") 