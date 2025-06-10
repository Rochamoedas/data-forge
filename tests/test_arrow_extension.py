#!/usr/bin/env python3
"""
🚀 Arrow Extension Test
Test DuckDB Arrow extension installation and functionality
"""

import duckdb
import polars as pl
import pyarrow as pa
import time
from typing import Dict, Any

def test_arrow_extension() -> Dict[str, Any]:
    """Test Arrow extension installation and functionality"""
    print("🚀 Testing DuckDB Arrow Extension")
    print("=" * 50)
    
    results = {
        "arrow_extension_available": False,
        "arrow_install_success": False,
        "arrow_load_success": False,
        "arrow_query_success": False,
        "performance_test": {}
    }
    
    try:
        # Create DuckDB connection
        conn = duckdb.connect(":memory:")
        
        # Test 1: Check if Arrow extension is available
        print("📦 Checking Arrow extension availability...")
        try:
            # Try to install Arrow extension
            conn.execute("INSTALL arrow")
            results["arrow_install_success"] = True
            print("✅ Arrow extension installed successfully")
            
            # Try to load Arrow extension
            conn.execute("LOAD arrow")
            results["arrow_load_success"] = True
            print("✅ Arrow extension loaded successfully")
            
            results["arrow_extension_available"] = True
            
        except Exception as e:
            print(f"❌ Arrow extension not available: {e}")
            return results
        
        # Test 2: Create test data and table
        print("\n📊 Creating test data...")
        test_data = [
            {"id": i, "name": f"test_{i}", "value": i * 1.5}
            for i in range(10000)
        ]
        
        # Create Polars DataFrame
        df = pl.DataFrame(test_data)
        
        # Convert to Arrow
        arrow_table = df.to_arrow()
        
        # Create table in DuckDB
        conn.execute("CREATE TABLE test_table AS SELECT * FROM arrow_table")
        print("✅ Test table created with Arrow data")
        
        # Test 3: Query using Arrow
        print("\n🚀 Testing Arrow query performance...")
        
        # Traditional query
        start_time = time.perf_counter()
        traditional_result = conn.execute("SELECT * FROM test_table LIMIT 5000").fetchall()
        traditional_duration = (time.perf_counter() - start_time) * 1000
        
        # Arrow query
        start_time = time.perf_counter()
        arrow_result = conn.execute("SELECT * FROM test_table LIMIT 5000").arrow()
        arrow_df = pl.from_arrow(arrow_result)
        arrow_duration = (time.perf_counter() - start_time) * 1000
        
        results["arrow_query_success"] = True
        results["performance_test"] = {
            "traditional_duration_ms": traditional_duration,
            "arrow_duration_ms": arrow_duration,
            "improvement_factor": traditional_duration / arrow_duration if arrow_duration > 0 else 0,
            "records_tested": 5000
        }
        
        print(f"📈 Performance Results:")
        print(f"  Traditional: {traditional_duration:.2f}ms")
        print(f"  Arrow: {arrow_duration:.2f}ms")
        print(f"  Improvement: {results['performance_test']['improvement_factor']:.2f}x")
        
        # Test 4: Large dataset test
        print("\n🔥 Testing with larger dataset...")
        large_data = [
            {"id": i, "name": f"large_test_{i}", "value": i * 2.5, "category": f"cat_{i % 10}"}
            for i in range(100000)
        ]
        
        large_df = pl.DataFrame(large_data)
        large_arrow = large_df.to_arrow()
        
        conn.execute("DROP TABLE IF EXISTS large_test")
        conn.execute("CREATE TABLE large_test AS SELECT * FROM large_arrow")
        
        # Test Arrow performance on large dataset
        start_time = time.perf_counter()
        large_arrow_result = conn.execute("SELECT * FROM large_test WHERE value > 50000 LIMIT 10000").arrow()
        large_arrow_df = pl.from_arrow(large_arrow_result)
        large_arrow_duration = (time.perf_counter() - start_time) * 1000
        
        results["performance_test"]["large_dataset"] = {
            "duration_ms": large_arrow_duration,
            "records_processed": len(large_arrow_df),
            "throughput_rps": len(large_arrow_df) / (large_arrow_duration / 1000) if large_arrow_duration > 0 else 0
        }
        
        print(f"🚀 Large dataset test:")
        print(f"  Duration: {large_arrow_duration:.2f}ms")
        print(f"  Records: {len(large_arrow_df):,}")
        print(f"  Throughput: {int(results['performance_test']['large_dataset']['throughput_rps']):,} records/sec")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Arrow extension test failed: {e}")
        results["error"] = str(e)
    
    return results

def test_polars_arrow_integration():
    """Test Polars ↔ Arrow integration"""
    print("\n🔗 Testing Polars ↔ Arrow Integration")
    print("=" * 50)
    
    try:
        # Create test data
        data = {
            "integers": list(range(10000)),
            "floats": [i * 1.5 for i in range(10000)],
            "strings": [f"test_string_{i}" for i in range(10000)],
            "booleans": [i % 2 == 0 for i in range(10000)]
        }
        
        # Test Polars → Arrow → Polars roundtrip
        print("🔄 Testing Polars → Arrow → Polars roundtrip...")
        
        start_time = time.perf_counter()
        
        # Polars DataFrame
        df_original = pl.DataFrame(data)
        
        # Convert to Arrow
        arrow_table = df_original.to_arrow()
        
        # Convert back to Polars
        df_roundtrip = pl.from_arrow(arrow_table)
        
        roundtrip_duration = (time.perf_counter() - start_time) * 1000
        
        # Verify data integrity
        data_matches = df_original.equals(df_roundtrip)
        
        print(f"✅ Roundtrip completed in {roundtrip_duration:.2f}ms")
        print(f"✅ Data integrity: {'PASSED' if data_matches else 'FAILED'}")
        
        # Test zero-copy performance
        print("\n⚡ Testing zero-copy performance...")
        
        large_data = {
            "col1": list(range(1000000)),
            "col2": [i * 2.5 for i in range(1000000)]
        }
        
        large_df = pl.DataFrame(large_data)
        
        start_time = time.perf_counter()
        large_arrow = large_df.to_arrow()
        conversion_duration = (time.perf_counter() - start_time) * 1000
        
        print(f"🚀 Large dataset conversion (1M records): {conversion_duration:.2f}ms")
        print(f"📊 Throughput: {int(1000000 / (conversion_duration / 1000)):,} records/sec")
        
    except Exception as e:
        print(f"❌ Polars ↔ Arrow integration test failed: {e}")

if __name__ == "__main__":
    print("🧪 DuckDB Arrow Extension & Polars Integration Tests")
    print("=" * 60)
    
    # Test Arrow extension
    arrow_results = test_arrow_extension()
    
    # Test Polars integration
    test_polars_arrow_integration()
    
    print("\n" + "=" * 60)
    print("📋 SUMMARY")
    print("=" * 60)
    
    if arrow_results["arrow_extension_available"]:
        print("✅ Arrow extension is working properly")
        if arrow_results.get("performance_test"):
            perf = arrow_results["performance_test"]
            print(f"🚀 Performance improvement: {perf.get('improvement_factor', 0):.2f}x faster")
    else:
        print("❌ Arrow extension is not available")
        print("💡 This explains why you see 'no Arrow extension needed' in logs")
    
    print("\n🎯 RECOMMENDATIONS:")
    if arrow_results["arrow_extension_available"]:
        print("  • Arrow extension is working - you should see performance improvements")
        print("  • Large datasets (>10K records) will benefit most from Arrow")
    else:
        print("  • Install DuckDB with Arrow support: pip install duckdb[arrow]")
        print("  • Or use the optimized fallback methods (still very fast)")
        print("  • The system will automatically use the best available method") 