#!/usr/bin/env python3
"""
🚀 Performance Benchmark Script
Test the optimized high-performance data processor

Expected Performance Targets for i7 10th Gen + 16GB RAM + SSD:
- Reads: 500K-2M rows/second  
- Writes: 200K-1M rows/second
- Bulk operations: 100-500 requests/second
"""

import asyncio
import time
import json
from typing import Dict, Any, List
import httpx
import random
from datetime import datetime

# Test data generator
def generate_production_data(num_records: int) -> List[Dict[str, Any]]:
    """Generate realistic production data for testing"""
    data = []
    for i in range(num_records):
        record = {
            "field_code": i % 1000,
            "field_name": f"Field_{i % 1000}",
            "well_code": i % 100,
            "well_reference": f"WELL_REF_{i % 100:03d}",
            "well_name": f"Well_{i % 100}",
            "production_period": f"2024-{(i % 12) + 1:02d}-01",
            "days_on_production": 30,
            "oil_production_kbd": round(100.0 + (i * 0.1) + random.uniform(-10, 10), 2),
            "gas_production_mmcfd": round(50.0 + (i * 0.05) + random.uniform(-5, 5), 2),
            "liquids_production_kbd": round(25.0 + (i * 0.02) + random.uniform(-2, 2), 2),
            "water_production_kbd": round(10.0 + (i * 0.01) + random.uniform(-1, 1), 2),
            "data_source": "performance_test",
            "source_data": "benchmark_data",
            "partition_0": f"partition_{i % 10}"
        }
        data.append(record)
    return data

async def benchmark_bulk_insert(client: httpx.AsyncClient, schema_name: str, data: List[Dict], endpoint: str) -> Dict:
    """Benchmark bulk insert performance"""
    print(f"🔄 Testing {endpoint} bulk insert with {len(data):,} records...")
    
    start_time = time.perf_counter()
    
    try:
        if "high-performance" in endpoint:
            # High-performance endpoint
            response = await client.post(
                f"http://localhost:8080/api/v1/high-performance/ultra-fast-bulk/{schema_name}",
                json=data,
                timeout=300  # 5 minute timeout for large datasets
            )
        else:
            # Traditional endpoint
            response = await client.post(
                f"http://localhost:8080/api/v1/records/bulk",
                json={
                    "schema_name": schema_name,
                    "data": data
                },
                timeout=300
            )
        
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        if response.status_code == 201:
            result = response.json()
            throughput = len(data) / (duration_ms / 1000) if duration_ms > 0 else 0
            
            return {
                "success": True,
                "endpoint": endpoint,
                "records": len(data),
                "duration_ms": duration_ms,
                "throughput_rps": int(throughput),
                "response": result
            }
        else:
            return {
                "success": False,
                "endpoint": endpoint,
                "error": f"HTTP {response.status_code}: {response.text}",
                "duration_ms": duration_ms
            }
            
    except Exception as e:
        duration_ms = (time.perf_counter() - start_time) * 1000
        return {
            "success": False,
            "endpoint": endpoint,
            "error": str(e),
            "duration_ms": duration_ms
        }

async def benchmark_query(client: httpx.AsyncClient, schema_name: str, endpoint: str, limit: int = 10000) -> Dict:
    """Benchmark query performance"""
    print(f"🔄 Testing {endpoint} query with limit {limit:,}...")
    
    start_time = time.perf_counter()
    
    try:
        if "high-performance" in endpoint:
            # High-performance endpoint
            response = await client.get(
                f"http://localhost:8080/api/v1/high-performance/ultra-fast-query/{schema_name}",
                params={"limit": limit},
                timeout=60
            )
        else:
            # Traditional endpoint
            response = await client.get(
                f"http://localhost:8080/api/v1/records/{schema_name}",
                params={"size": limit, "page": 1},
                timeout=60
            )
        
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        if response.status_code == 200:
            result = response.json()
            
            # Extract record count based on endpoint type
            if "high-performance" in endpoint:
                record_count = result.get("record_count", 0)
            else:
                record_count = len(result.get("data", {}).get("items", []))
            
            throughput = record_count / (duration_ms / 1000) if duration_ms > 0 else 0
            
            return {
                "success": True,
                "endpoint": endpoint,
                "records": record_count,
                "duration_ms": duration_ms,
                "throughput_rps": int(throughput),
                "limit": limit
            }
        else:
            return {
                "success": False,
                "endpoint": endpoint,
                "error": f"HTTP {response.status_code}: {response.text}",
                "duration_ms": duration_ms
            }
            
    except Exception as e:
        duration_ms = (time.perf_counter() - start_time) * 1000
        return {
            "success": False,
            "endpoint": endpoint,
            "error": str(e),
            "duration_ms": duration_ms
        }

async def run_comprehensive_benchmark():
    """Run comprehensive performance benchmark"""
    print("🚀 Starting Comprehensive Performance Benchmark")
    print("=" * 60)
    print("Hardware: i7 10th Gen + 16GB RAM + SSD")
    print("Expected Performance:")
    print("- Reads: 500K-2M rows/second")
    print("- Writes: 200K-1M rows/second")
    print("- Bulk operations: 100-500 requests/second")
    print("=" * 60)
    
    schema_name = "production_data"
    results = []
    
    async with httpx.AsyncClient() as client:
        # Test data sizes
        test_sizes = [1000, 10000, 50000, 100000]
        
        for size in test_sizes:
            print(f"\n📊 TESTING WITH {size:,} RECORDS")
            print("-" * 40)
            
            # Generate test data
            test_data = generate_production_data(size)
            
            # Test bulk insert - High Performance
            hp_insert_result = await benchmark_bulk_insert(
                client, schema_name, test_data, "high-performance"
            )
            results.append(hp_insert_result)
            
            if hp_insert_result["success"]:
                print(f"✅ High-Performance Insert: {hp_insert_result['throughput_rps']:,} records/sec")
            else:
                print(f"❌ High-Performance Insert Failed: {hp_insert_result['error']}")
            
            # Test bulk insert - Traditional (for comparison)
            if size <= 10000:  # Only test traditional with smaller datasets
                trad_insert_result = await benchmark_bulk_insert(
                    client, schema_name, test_data, "traditional"
                )
                results.append(trad_insert_result)
                
                if trad_insert_result["success"]:
                    print(f"✅ Traditional Insert: {trad_insert_result['throughput_rps']:,} records/sec")
                    
                    # Calculate improvement
                    if hp_insert_result["success"]:
                        improvement = hp_insert_result["throughput_rps"] / trad_insert_result["throughput_rps"]
                        print(f"🚀 Performance Improvement: {improvement:.1f}x faster")
                else:
                    print(f"❌ Traditional Insert Failed: {trad_insert_result['error']}")
            
            # Test queries with different sizes
            query_limits = [1000, 10000, min(size, 50000)]
            
            for limit in query_limits:
                # High-performance query
                hp_query_result = await benchmark_query(
                    client, schema_name, "high-performance", limit
                )
                results.append(hp_query_result)
                
                if hp_query_result["success"]:
                    print(f"✅ High-Performance Query ({limit:,}): {hp_query_result['throughput_rps']:,} records/sec")
                else:
                    print(f"❌ High-Performance Query Failed: {hp_query_result['error']}")
                
                # Traditional query (for comparison)
                if limit <= 10000:
                    trad_query_result = await benchmark_query(
                        client, schema_name, "traditional", limit
                    )
                    results.append(trad_query_result)
                    
                    if trad_query_result["success"]:
                        print(f"✅ Traditional Query ({limit:,}): {trad_query_result['throughput_rps']:,} records/sec")
                        
                        # Calculate improvement
                        if hp_query_result["success"]:
                            improvement = hp_query_result["throughput_rps"] / trad_query_result["throughput_rps"]
                            print(f"🚀 Query Improvement: {improvement:.1f}x faster")
                    else:
                        print(f"❌ Traditional Query Failed: {trad_query_result['error']}")
            
            # Small delay between test sizes
            await asyncio.sleep(2)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 BENCHMARK SUMMARY")
    print("=" * 60)
    
    # Analyze results
    hp_inserts = [r for r in results if r["success"] and r["endpoint"] == "high-performance" and "records" in r]
    hp_queries = [r for r in results if r["success"] and r["endpoint"] == "high-performance" and "limit" in r]
    
    if hp_inserts:
        max_insert_throughput = max(r["throughput_rps"] for r in hp_inserts)
        avg_insert_throughput = sum(r["throughput_rps"] for r in hp_inserts) / len(hp_inserts)
        print(f"🚀 Insert Performance:")
        print(f"   Max Throughput: {max_insert_throughput:,} records/sec")
        print(f"   Avg Throughput: {int(avg_insert_throughput):,} records/sec")
        
        # Check if we're meeting targets
        if max_insert_throughput >= 200000:
            print(f"   ✅ MEETING TARGET (200K+ records/sec)")
        else:
            print(f"   ⚠️  BELOW TARGET (expected 200K+ records/sec)")
    
    if hp_queries:
        max_query_throughput = max(r["throughput_rps"] for r in hp_queries)
        avg_query_throughput = sum(r["throughput_rps"] for r in hp_queries) / len(hp_queries)
        print(f"🚀 Query Performance:")
        print(f"   Max Throughput: {max_query_throughput:,} records/sec")
        print(f"   Avg Throughput: {int(avg_query_throughput):,} records/sec")
        
        # Check if we're meeting targets
        if max_query_throughput >= 500000:
            print(f"   ✅ MEETING TARGET (500K+ records/sec)")
        else:
            print(f"   ⚠️  BELOW TARGET (expected 500K+ records/sec)")
    
    # Save detailed results
    with open(f"benchmark_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📄 Detailed results saved to benchmark_results_*.json")

if __name__ == "__main__":
    asyncio.run(run_comprehensive_benchmark()) 