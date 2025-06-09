#!/usr/bin/env python3
"""
Streaming Performance Test - Optimized for High-End Hardware
Tests different batch sizes to find optimal performance for 16GB RAM, i7 10th gen
"""

import requests
import json
import time
from typing import Dict, Any

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"

def test_streaming_performance(batch_size: int, test_name: str) -> Dict[str, Any]:
    """Test streaming performance with specific batch size"""
    print(f"\n🧪 Testing {test_name} (batch_size={batch_size:,})")
    
    start_time = time.perf_counter()
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/high-performance/stream-arrow-batches/{SCHEMA_NAME}?batch_size={batch_size}",
            stream=True,
            timeout=120  # Generous timeout
        )
        
        if response.status_code == 200:
            batch_count = 0
            total_records = 0
            first_batch_time = None
            
            for line in response.iter_lines(decode_unicode=True):
                if line and line.strip():
                    try:
                        line_data = json.loads(line.strip())
                        
                        if "batch_number" in line_data:
                            batch_count += 1
                            batch_size_actual = line_data.get("batch_size", 0)
                            total_records += batch_size_actual
                            
                            if batch_count == 1:
                                first_batch_time = time.perf_counter() - start_time
                                print(f"  ⚡ First batch received in {first_batch_time:.2f}s ({batch_size_actual:,} records)")
                            
                            if batch_count % 5 == 0:  # Log every 5th batch
                                elapsed = time.perf_counter() - start_time
                                current_throughput = total_records / elapsed if elapsed > 0 else 0
                                print(f"  📊 Batch {batch_count}: {total_records:,} records in {elapsed:.2f}s ({int(current_throughput):,} records/sec)")
                        
                        elif "stream_complete" in line_data:
                            final_batches = line_data.get('total_batches', batch_count)
                            final_records = line_data.get('total_records', total_records)
                            print(f"  ✅ Stream completed: {final_batches} batches, {final_records:,} total records")
                            
                    except json.JSONDecodeError:
                        continue
            
            duration = time.perf_counter() - start_time
            throughput = total_records / duration if duration > 0 else 0
            
            return {
                "success": True,
                "test_name": test_name,
                "batch_size": batch_size,
                "total_duration_sec": duration,
                "first_batch_time_sec": first_batch_time,
                "total_batches": batch_count,
                "total_records": total_records,
                "avg_throughput_rps": int(throughput),
                "records_per_batch": total_records // batch_count if batch_count > 0 else 0
            }
        else:
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text}"
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"Request failed: {str(e)}"
        }

def main():
    print("🚀 STREAMING PERFORMANCE TEST - High-End Hardware Optimization")
    print("=" * 80)
    print("Hardware: 16GB RAM, i7 10th gen")
    print("Target: Optimize streaming performance for 500K+ records")
    print("=" * 80)
    
    # Test different batch sizes
    test_configs = [
        (50000, "Conservative (50K per batch)"),
        (100000, "Moderate (100K per batch)"),
        (250000, "Aggressive (250K per batch)"),
        (500000, "Maximum (500K per batch)")
    ]
    
    results = []
    
    for batch_size, test_name in test_configs:
        result = test_streaming_performance(batch_size, test_name)
        results.append(result)
        
        if result["success"]:
            print(f"✅ {test_name}: {result['avg_throughput_rps']:,} records/sec, {result['total_batches']} batches, {result['total_duration_sec']:.2f}s total")
        else:
            print(f"❌ {test_name}: {result['error']}")
        
        time.sleep(2)  # Brief pause between tests
    
    # Performance comparison
    print("\n" + "=" * 80)
    print("📊 PERFORMANCE COMPARISON")
    print("=" * 80)
    
    successful_results = [r for r in results if r["success"]]
    
    if successful_results:
        best_result = max(successful_results, key=lambda x: x["avg_throughput_rps"])
        
        print(f"🏆 BEST PERFORMANCE: {best_result['test_name']}")
        print(f"   Batch Size: {best_result['batch_size']:,} records")
        print(f"   Throughput: {best_result['avg_throughput_rps']:,} records/sec")
        print(f"   Total Batches: {best_result['total_batches']}")
        print(f"   First Batch Time: {best_result['first_batch_time_sec']:.2f}s")
        print(f"   Total Duration: {best_result['total_duration_sec']:.2f}s")
        
        print(f"\n📈 PERFORMANCE COMPARISON:")
        baseline = successful_results[0] if successful_results else None
        
        for result in successful_results:
            if baseline:
                improvement = result["avg_throughput_rps"] / baseline["avg_throughput_rps"]
                batch_reduction = baseline["total_batches"] / result["total_batches"] if result["total_batches"] > 0 else 1
                print(f"   {result['test_name']}: {improvement:.1f}x throughput, {batch_reduction:.1f}x fewer batches")
            else:
                print(f"   {result['test_name']}: {result['avg_throughput_rps']:,} records/sec")
        
        print(f"\n💡 RECOMMENDATION:")
        if best_result["batch_size"] >= 250000:
            print(f"   ✅ Use batch_size={best_result['batch_size']:,} for optimal performance on your hardware")
            print(f"   ✅ This reduces network overhead and maximizes throughput")
            print(f"   ✅ For 500K records: ~{500000 // best_result['batch_size']} batches instead of 50")
        else:
            print(f"   ⚠️ Consider testing even larger batch sizes for your high-end hardware")

if __name__ == "__main__":
    main() 