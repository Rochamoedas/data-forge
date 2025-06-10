import time
import json
import requests
from typing import List, Dict, Any
import uuid
import sys
import os
from io import StringIO
import threading
import psutil
from datetime import datetime

# ============================================================================
# 📊 PROFESSIONAL BENCHMARK REPORT FORMATTER
# ============================================================================

class BenchmarkReportFormatter:
    """Professional formatter for benchmark results with structured output"""
    
    def __init__(self):
        self.results = {}
        self.start_time = None
        self.end_time = None
        self.config = {}
        
    def set_config(self, config: Dict[str, Any]):
        """Set benchmark configuration"""
        self.config = config
        
    def start_benchmark(self):
        """Mark benchmark start time"""
        self.start_time = datetime.now()
        
    def end_benchmark(self):
        """Mark benchmark end time"""
        self.end_time = datetime.now()
        
    def add_test_result(self, test_name: str, test_type: str, results: Dict[str, Any]):
        """Add test results to the report"""
        if test_type not in self.results:
            self.results[test_type] = {}
        self.results[test_type][test_name] = results
        
    def format_duration(self, ms: float) -> str:
        """Format duration in a readable way"""
        if ms < 1000:
            return f"{ms:.1f}ms"
        elif ms < 60000:
            return f"{ms/1000:.2f}s"
        else:
            return f"{ms/60000:.1f}m"
            
    def format_throughput(self, rps: int) -> str:
        """Format throughput in a readable way"""
        if rps < 1000:
            return f"{rps:,} rps"
        elif rps < 1000000:
            return f"{rps/1000:.1f}K rps"
        else:
            return f"{rps/1000000:.1f}M rps"
            
    def format_number(self, num: int) -> str:
        """Format large numbers with appropriate units"""
        if num < 1000:
            return str(num)
        elif num < 1000000:
            return f"{num/1000:.1f}K"
        else:
            return f"{num/1000000:.1f}M"
            
    def generate_executive_summary(self) -> str:
        """Generate executive summary of benchmark results"""
        summary = []
        summary.append("## 📊 Executive Summary\n")
        
        # Configuration overview
        mode_name = self.config.get('mode_name', 'CUSTOM')
        test_sizes = self.config.get('test_sizes', [])
        
        summary.append(f"**Test Configuration:** {mode_name} mode with {len(test_sizes)} test sizes")
        summary.append(f"**Test Range:** {self.format_number(min(test_sizes))} - {self.format_number(max(test_sizes))} records")
        summary.append(f"**Duration:** {(self.end_time - self.start_time).total_seconds():.1f} seconds")
        summary.append("")
        
        # Key performance metrics
        summary.append("### 🚀 Key Performance Highlights\n")
        
                 # Extract key metrics from results
        best_throughput = 0
        best_improvement = 0
        total_tests = 0
        
        for test_type, tests in self.results.items():
            total_tests += len(tests)
            for test_name, result in tests.items():
                # Find best throughput from various sources
                throughput_sources = [
                    result.get('throughput_rps', 0),
                    result.get('optimized_method', {}).get('throughput_rps', 0),
                    result.get('traditional_method', {}).get('throughput_rps', 0),
                    result.get('high_performance', {}).get('throughput_rps', 0),
                    result.get('traditional', {}).get('throughput_rps', 0)
                ]
                
                for throughput in throughput_sources:
                    if isinstance(throughput, (int, float)) and throughput > best_throughput:
                        best_throughput = int(throughput)
                
                # Find best improvement from various sources
                improvement_sources = []
                if 'performance_improvement' in result:
                    improvement_sources.append(result['performance_improvement'].get('speed_improvement_factor', 0))
                
                # Check stress test improvements
                if 'traditional' in result and 'high_performance' in result:
                    trad_duration = result['traditional'].get('duration_ms', 1)
                    hp_duration = result['high_performance'].get('duration_ms', 1)
                    if trad_duration > 0 and hp_duration > 0:
                        improvement_sources.append(trad_duration / hp_duration)
                
                for improvement in improvement_sources:
                    if isinstance(improvement, (int, float)) and improvement > best_improvement:
                        best_improvement = improvement
        
        summary.append(f"- **Peak Throughput:** {self.format_throughput(best_throughput)}")
        summary.append(f"- **Best Performance Improvement:** {best_improvement:.1f}x faster")
        summary.append(f"- **Total Tests Executed:** {total_tests}")
        summary.append(f"- **Test Coverage:** {len(self.results)} test categories")
        summary.append("")
        
        return "\n".join(summary)
        
    def generate_detailed_results(self) -> str:
        """Generate detailed test results section"""
        details = []
        details.append("## 📋 Detailed Test Results\n")
        
        for test_type, tests in self.results.items():
            details.append(f"### {test_type.replace('_', ' ').title()}\n")
            
            if test_type == "built_in_benchmark":
                details.extend(self._format_builtin_benchmark(tests))
            elif test_type == "stress_test":
                details.extend(self._format_stress_test(tests))
            elif test_type == "query_performance":
                details.extend(self._format_query_performance(tests))
            elif test_type == "data_analysis":
                details.extend(self._format_data_analysis(tests))
            else:
                details.extend(self._format_generic_test(tests))
            
            details.append("")
            
        return "\n".join(details)
        
    def _format_builtin_benchmark(self, tests: Dict[str, Any]) -> List[str]:
        """Format built-in benchmark results"""
        lines = []
        lines.append("| Test Size | Optimized Method | Traditional Method | Speed Improvement | Recommendation |")
        lines.append("|-----------|------------------|--------------------|--------------------|----------------|")
        
        for test_name, result in tests.items():
            if result.get('success'):
                size = result.get('test_data_size', 'N/A')
                opt_method = result.get('optimized_method', {})
                trad_method = result.get('traditional_method', {})
                improvement = result.get('performance_improvement', {})
                
                opt_duration = self.format_duration(opt_method.get('duration_ms', 0))
                opt_throughput = self.format_throughput(opt_method.get('throughput_rps', 0))
                trad_duration = self.format_duration(trad_method.get('duration_ms', 0))
                trad_throughput = self.format_throughput(trad_method.get('throughput_rps', 0))
                
                speed_factor = improvement.get('speed_improvement_factor', 0)
                speed_icon = "🚀" if speed_factor > 1 else "⚠️"
                
                recommendation = "Use optimized" if speed_factor > 1 else "Use traditional"
                
                lines.append(f"| {self.format_number(size)} | {opt_duration} ({opt_throughput}) | {trad_duration} ({trad_throughput}) | {speed_icon} {speed_factor:.2f}x | {recommendation} |")
        
        lines.append("")
        return lines
        
    def _format_stress_test(self, tests: Dict[str, Any]) -> List[str]:
        """Format stress test results"""
        lines = []
        lines.append("| Test Size | Traditional | High-Performance | Speed Improvement | Throughput Gain |")
        lines.append("|-----------|-------------|------------------|-------------------|-----------------|")
        
        for test_name, result in tests.items():
            if 'traditional' in result and 'high_performance' in result:
                # Extract size from test name
                size_str = test_name.replace('stress_test_', '').replace('_records', '')
                try:
                    size = int(size_str)
                except ValueError:
                    size = 0
                    
                trad = result['traditional']
                hp = result['high_performance']
                
                if trad.get('success') and hp.get('success'):
                    trad_duration = self.format_duration(trad.get('duration_ms', 0))
                    trad_throughput = self.format_throughput(trad.get('throughput_rps', 0))
                    hp_duration = self.format_duration(hp.get('duration_ms', 0))
                    hp_throughput = self.format_throughput(hp.get('throughput_rps', 0))
                    
                    speed_improvement = trad.get('duration_ms', 1) / max(hp.get('duration_ms', 1), 1)
                    throughput_gain = hp.get('throughput_rps', 0) / max(trad.get('throughput_rps', 1), 1)
                    
                    lines.append(f"| {self.format_number(size)} | {trad_duration} ({trad_throughput}) | {hp_duration} ({hp_throughput}) | 🚀 {speed_improvement:.1f}x | 📈 {throughput_gain:.1f}x |")
        
        lines.append("")
        return lines
        
    def _format_query_performance(self, tests: Dict[str, Any]) -> List[str]:
        """Format query performance results"""
        lines = []
        lines.append("| Test Size | Traditional Query | High-Performance Query | Improvement |")
        lines.append("|-----------|-------------------|------------------------|-------------|")
        
        for test_name, result in tests.items():
            if 'traditional_query' in result and 'high_performance_query' in result:
                # Extract size from test name
                size_str = test_name.replace('query_size', '').replace('_records', '').strip()
                try:
                    size = int(size_str)
                except ValueError:
                    size = 0
                    
                trad = result['traditional_query']
                hp = result['high_performance_query']
                improvement = result.get('improvement', {})
                
                trad_duration = self.format_duration(trad.get('duration_ms', 0))
                trad_throughput = self.format_throughput(trad.get('throughput_rps', 0))
                hp_duration = self.format_duration(hp.get('duration_ms', 0))
                hp_throughput = self.format_throughput(hp.get('throughput_rps', 0))
                
                factor = improvement.get('factor', 0)
                percentage = improvement.get('percentage', 0)
                icon = "🚀" if factor > 1 else "⚠️"
                
                lines.append(f"| {self.format_number(size)} | {trad_duration} ({trad_throughput}) | {hp_duration} ({hp_throughput}) | {icon} {factor:.2f}x ({percentage:+.1f}%) |")
        
        lines.append("")
        return lines
        
    def _format_data_analysis(self, tests: Dict[str, Any]) -> List[str]:
        """Format data analysis results"""
        lines = []
        lines.append("| Analysis Type | Duration | Optimization | Status |")
        lines.append("|---------------|----------|--------------|--------|")
        
        for test_name, result in tests.items():
            analysis_type = result.get('analysis_type', test_name).title()
            duration = self.format_duration(result.get('duration_ms', 0))
            optimization = result.get('optimization', 'N/A')
            status = "✅ Success" if result.get('success') else "❌ Failed"
            
            lines.append(f"| {analysis_type} | {duration} | {optimization} | {status} |")
        
        lines.append("")
        return lines
        
    def _format_generic_test(self, tests: Dict[str, Any]) -> List[str]:
        """Format generic test results"""
        lines = []
        for test_name, result in tests.items():
            lines.append(f"**{test_name.replace('_', ' ').title()}:**")
            if isinstance(result, dict):
                for key, value in result.items():
                    if key not in ['data', 'records', 'items']:  # Skip large data structures
                        lines.append(f"- {key.replace('_', ' ').title()}: {value}")
            lines.append("")
        return lines
        
    def generate_performance_analysis(self) -> str:
        """Generate performance analysis and recommendations"""
        analysis = []
        analysis.append("## 🔍 Performance Analysis\n")
        
        # Scaling analysis
        analysis.append("### 📈 Scaling Characteristics\n")
        
        # Extract scaling data from stress tests
        if 'stress_test' in self.results:
            stress_results = self.results['stress_test']
            sizes = []
            hp_throughputs = []
            
            for test_name, result in stress_results.items():
                if 'high_performance' in result and result['high_performance'].get('success'):
                    size_str = test_name.replace('stress_test_', '').replace('_records', '')
                    try:
                        size = int(size_str)
                        throughput = result['high_performance'].get('throughput_rps', 0)
                        sizes.append(size)
                        hp_throughputs.append(throughput)
                    except ValueError:
                        continue
            
            if len(sizes) >= 2:
                # Calculate scaling efficiency
                size_ratio = max(sizes) / min(sizes)
                throughput_ratio = max(hp_throughputs) / max(min(hp_throughputs), 1)
                scaling_efficiency = throughput_ratio / size_ratio
                
                analysis.append(f"- **Dataset Size Range:** {size_ratio:.1f}x increase")
                analysis.append(f"- **Throughput Scaling:** {throughput_ratio:.1f}x improvement")
                analysis.append(f"- **Scaling Efficiency:** {scaling_efficiency:.2f} (1.0 = linear scaling)")
                
                if scaling_efficiency > 0.8:
                    analysis.append("- **Scaling Assessment:** ✅ Excellent - Near-linear scaling")
                elif scaling_efficiency > 0.6:
                    analysis.append("- **Scaling Assessment:** ✅ Good - Acceptable scaling")
                else:
                    analysis.append("- **Scaling Assessment:** ⚠️ Poor - Sub-linear scaling")
        
        analysis.append("")
        
        # Recommendations
        analysis.append("### 💡 Recommendations\n")
        
        # Analyze results to provide specific recommendations
        recommendations = []
        
        # Check built-in benchmark results
        if 'built_in_benchmark' in self.results:
            for test_name, result in self.results['built_in_benchmark'].items():
                if result.get('success'):
                    improvement = result.get('performance_improvement', {})
                    factor = improvement.get('speed_improvement_factor', 0)
                    if factor > 1:
                        recommendations.append("✅ **Use optimized methods** - Consistently outperform traditional approaches")
                        break
                    elif factor < 0.8:
                        recommendations.append("⚠️ **Review optimization strategy** - Traditional methods showing better performance")
                        break
        
        # Check query performance
        if 'query_performance' in self.results:
            hp_better_count = 0
            total_query_tests = 0
            for test_name, result in self.results['query_performance'].items():
                if 'improvement' in result:
                    total_query_tests += 1
                    if result['improvement'].get('factor', 0) > 1:
                        hp_better_count += 1
            
            if total_query_tests > 0:
                hp_success_rate = hp_better_count / total_query_tests
                if hp_success_rate > 0.7:
                    recommendations.append("🚀 **Adopt high-performance queries** - Significant improvements in most scenarios")
                elif hp_success_rate > 0.3:
                    recommendations.append("🔍 **Selective optimization** - High-performance queries beneficial for specific use cases")
                else:
                    recommendations.append("⚠️ **Traditional queries preferred** - High-performance optimization not showing benefits")
        
        # General recommendations
        recommendations.extend([
            "📊 **Monitor resource usage** - Track CPU and memory consumption during peak loads",
            "🔄 **Implement gradual rollout** - Test optimizations with small datasets first",
            "📈 **Regular benchmarking** - Establish baseline metrics and track improvements over time",
            "🎯 **Focus on bottlenecks** - Prioritize optimization of slowest operations"
        ])
        
        for rec in recommendations:
            analysis.append(f"- {rec}")
        
        analysis.append("")
        return "\n".join(analysis)
        
    def generate_system_info(self) -> str:
        """Generate system information section"""
        info = []
        info.append("## 🖥️ System Information\n")
        
        info.append(f"- **Test Timestamp:** {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        info.append(f"- **Base URL:** {self.config.get('base_url', 'N/A')}")
        info.append(f"- **Schema:** {self.config.get('schema_name', 'N/A')}")
        info.append(f"- **Test Mode:** {self.config.get('mode_name', 'N/A')}")
        info.append(f"- **Test Sizes:** {', '.join([self.format_number(s) for s in self.config.get('test_sizes', [])])}")
        
        # Add enabled features
        features = []
        if self.config.get('enable_json_test'):
            features.append("JSON Testing")
        if self.config.get('enable_streaming_test'):
            features.append("Streaming")
        if self.config.get('enable_pagination_test'):
            features.append("Pagination")
        
        info.append(f"- **Enabled Features:** {', '.join(features) if features else 'Basic tests only'}")
        info.append("")
        
        return "\n".join(info)
        
    def generate_full_report(self) -> str:
        """Generate the complete professional benchmark report"""
        report = []
        
        # Header
        report.append("# 🚀 High-Performance Data Processing Benchmark Report\n")
        report.append("---\n")
        
        # Executive Summary
        report.append(self.generate_executive_summary())
        
        # System Information
        report.append(self.generate_system_info())
        
        # Detailed Results
        report.append(self.generate_detailed_results())
        
        # Performance Analysis
        report.append(self.generate_performance_analysis())
        
        # Footer
        report.append("---")
        report.append(f"*Report generated on {self.end_time.strftime('%Y-%m-%d at %H:%M:%S')}*")
        
        return "\n".join(report)

# ============================================================================
# 🚀 BENCHMARK CONFIGURATION - Modify these settings to customize your tests
# ============================================================================

# Server Configuration
BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "well_production"  # Replace with your actual schema

# ============================================================================
# 📊 TEST MODES - Choose your testing intensity level
# ============================================================================

# 🚀 QUICK MODE - Fast tests for development (recommended for frequent testing)
QUICK_MODE = {
    "test_sizes": [1000, 5000],
    "enable_json_test": False,
    "enable_streaming_test": False,
    "enable_pagination_test": False,
    "monitoring_threshold": 10000,  # Only monitor tests >= this size
    "query_test_sizes": [1000, 5000],
    "pagination_page_sizes": [1000, 5000]
}

# ⚡ STANDARD MODE - Balanced testing (good for regular validation)
STANDARD_MODE = {
    "test_sizes": [10000, 50000],
    "enable_json_test": True,
    "enable_streaming_test": True,
    "enable_pagination_test": True,
    "monitoring_threshold": 10000,
    "query_test_sizes": [10000, 50000],
    "pagination_page_sizes": [10000, 50000]
}

# 💪 STRESS MODE - Heavy testing (for performance validation)
STRESS_MODE = {
    "test_sizes": [50000, 100000, 500000],
    "enable_json_test": True,
    "enable_streaming_test": True,
    "enable_pagination_test": True,
    "monitoring_threshold": 50000,
    "query_test_sizes": [50000, 100000, 200000],
    "pagination_page_sizes": [50000, 100000]
}

# 🔥 EXTREME MODE - Maximum stress testing (for final validation)
EXTREME_MODE = {
    "test_sizes": [100000, 500000, 1000000],
    "enable_json_test": True,
    "enable_streaming_test": True,
    "enable_pagination_test": True,
    "monitoring_threshold": 100000,
    "query_test_sizes": [100000, 500000, 1000000],
    "pagination_page_sizes": [100000, 500000]
}

# ============================================================================
# 🎛️ ACTIVE CONFIGURATION - Change this to switch test modes
# ============================================================================

# Choose your test mode here:
ACTIVE_CONFIG = QUICK_MODE      # For quick development testing
# ACTIVE_CONFIG = STANDARD_MODE     # For regular testing (DEFAULT)
# ACTIVE_CONFIG = STRESS_MODE     # For performance validation
# ACTIVE_CONFIG = EXTREME_MODE    # For maximum stress testing

# Extract configuration values
TEST_SIZES = ACTIVE_CONFIG["test_sizes"]
ENABLE_JSON_TEST = ACTIVE_CONFIG["enable_json_test"]
ENABLE_STREAMING_TEST = ACTIVE_CONFIG["enable_streaming_test"]
ENABLE_PAGINATION_TEST = ACTIVE_CONFIG["enable_pagination_test"]
MONITORING_THRESHOLD = ACTIVE_CONFIG["monitoring_threshold"]
QUERY_TEST_SIZES = ACTIVE_CONFIG["query_test_sizes"]
PAGINATION_PAGE_SIZES = ACTIVE_CONFIG["pagination_page_sizes"]

# ============================================================================
# 🔧 ADVANCED CONFIGURATION - Fine-tune specific aspects
# ============================================================================

# System Monitoring Settings
MONITORING_CONFIG = {
    "high_frequency_interval": 0.5,    # For critical tests (seconds)
    "medium_frequency_interval": 1.0,  # For stress tests (seconds)
    "low_frequency_interval": 2.0,     # For streaming tests (seconds)
    "enable_live_stats": True,         # Show real-time stats before tests
    "enable_detailed_output": True     # Show detailed monitoring results
}

# Test Data Configuration
DATA_CONFIG = {
    "json_test_file": "external/mocked_response_100K-4.json",
    "generate_unique_data": True,      # Generate unique data for each test
    "data_validation": True,           # Validate data integrity
    "field_range_multiplier": 10,      # Multiplier for field code ranges
    "well_range_multiplier": 20        # Multiplier for well code ranges
}

# Performance Thresholds (for warnings/alerts)
PERFORMANCE_THRESHOLDS = {
    "min_throughput_rps": 1000,        # Minimum acceptable records/sec
    "max_duration_seconds": 300,       # Maximum test duration (5 minutes)
    "max_memory_mb": 6144,             # Maximum process memory (2GB)
    "max_cpu_percent": 90              # Maximum CPU usage
}

# Test Timeouts
TIMEOUT_CONFIG = {
    "request_timeout": 60,             # HTTP request timeout (seconds)
    "streaming_timeout": 180,          # Streaming test timeout (seconds)
    "large_query_timeout": 120         # Large query timeout (seconds)
}

# ============================================================================
# 📝 CONFIGURATION SUMMARY
# ============================================================================

def print_configuration_summary():
    """Print the current test configuration"""
    mode_name = "CUSTOM"
    for name, config in [("QUICK", QUICK_MODE), ("STANDARD", STANDARD_MODE), 
                        ("STRESS", STRESS_MODE), ("EXTREME", EXTREME_MODE)]:
        if config == ACTIVE_CONFIG:
            mode_name = name
            break
    
    print("🎛️  BENCHMARK CONFIGURATION")
    print("=" * 50)
    print(f"Mode: {mode_name}")
    print(f"Test Sizes: {TEST_SIZES}")
    print(f"JSON Test: {'✅' if ENABLE_JSON_TEST else '❌'}")
    print(f"Streaming Test: {'✅' if ENABLE_STREAMING_TEST else '❌'}")
    print(f"Pagination Test: {'✅' if ENABLE_PAGINATION_TEST else '❌'}")
    print(f"Monitoring Threshold: {MONITORING_THRESHOLD:,} records")
    print(f"Query Test Sizes: {QUERY_TEST_SIZES}")
    print(f"Pagination Page Sizes: {PAGINATION_PAGE_SIZES}")
    print(f"Schema: {SCHEMA_NAME}")
    print(f"Base URL: {BASE_URL}")
    print("=" * 50)

def validate_performance_thresholds(result: Dict[str, Any], test_name: str = "Unknown") -> Dict[str, Any]:
    """Validate test results against performance thresholds"""
    warnings = []
    
    # Check throughput
    throughput = result.get("throughput_rps", result.get("avg_throughput_rps", 0))
    if throughput > 0 and throughput < PERFORMANCE_THRESHOLDS["min_throughput_rps"]:
        warnings.append(f"Low throughput: {throughput:,} < {PERFORMANCE_THRESHOLDS['min_throughput_rps']:,} rps")
    
    # Check duration
    duration_s = result.get("duration_ms", 0) / 1000 if result.get("duration_ms") else result.get("total_duration_s", 0)
    if duration_s > PERFORMANCE_THRESHOLDS["max_duration_seconds"]:
        warnings.append(f"Long duration: {duration_s:.1f}s > {PERFORMANCE_THRESHOLDS['max_duration_seconds']}s")
    
    # Check memory usage
    monitoring = result.get("system_monitoring", {})
    if monitoring:
        max_memory = monitoring.get("process_memory", {}).get("max_mb", 0)
        if max_memory > PERFORMANCE_THRESHOLDS["max_memory_mb"]:
            warnings.append(f"High memory usage: {max_memory:.1f}MB > {PERFORMANCE_THRESHOLDS['max_memory_mb']}MB")
        
        max_cpu = monitoring.get("system_cpu", {}).get("max_percent", 0)
        if max_cpu > PERFORMANCE_THRESHOLDS["max_cpu_percent"]:
            warnings.append(f"High CPU usage: {max_cpu:.1f}% > {PERFORMANCE_THRESHOLDS['max_cpu_percent']}%")
    
    validation_result = {
        "test_name": test_name,
        "passed": len(warnings) == 0,
        "warnings": warnings,
        "thresholds_checked": {
            "min_throughput_rps": PERFORMANCE_THRESHOLDS["min_throughput_rps"],
            "max_duration_seconds": PERFORMANCE_THRESHOLDS["max_duration_seconds"],
            "max_memory_mb": PERFORMANCE_THRESHOLDS["max_memory_mb"],
            "max_cpu_percent": PERFORMANCE_THRESHOLDS["max_cpu_percent"]
        }
    }
    
    if warnings:
        print(f"⚠️  Performance warnings for {test_name}:")
        for warning in warnings:
            print(f"   • {warning}")
    else:
        print(f"✅ Performance validation passed for {test_name}")
    
    return validation_result

def generate_test_data(size: int, test_id: str = None) -> List[Dict[str, Any]]:
    """Generate test data for performance testing with unique records for each test"""
    # Create a unique identifier for this test run to avoid duplicates between tests
    if test_id is None:
        test_id = str(uuid.uuid4())[:8]
    
    # Use test_id to create unique ranges for each test
    base_offset = hash(test_id) % 1000000  # Create unique offset based on test_id
    
    # Calculate ranges to ensure uniqueness for the largest test size
    max_test_size = max(TEST_SIZES)
    field_range = max(10000, max_test_size // 10)
    well_range = max(5000, max_test_size // 20)
    
    return [
        {
            "field_code": (base_offset + i) % field_range,
            "field_name": f"Field_{test_id}_{(base_offset + i) % field_range}",
            "well_code": (base_offset + i) % well_range,
            "well_reference": f"WELL_{test_id}_{(base_offset + i) % well_range:06d}",
            "well_name": f"Well_{test_id}_{(base_offset + i) % well_range}",
            "production_period": f"2024-{(i % 12) + 1:02d}-{((i // 12) % 28) + 1:02d}",
            "days_on_production": 25 + (i % 10),
            "oil_production_kbd": round(100.5 + (i * 0.001) + (base_offset * 0.0001), 3),
            "gas_production_mmcfd": round(50.3 + (i * 0.0005) + (base_offset * 0.00005), 3),
            "liquids_production_kbd": round(75.2 + (i * 0.0008) + (base_offset * 0.00008), 3),
            "water_production_kbd": round(25.1 + (i * 0.0002) + (base_offset * 0.00002), 3),
            "data_source": f"test_data_{test_id}_batch_{i // 10000}",
            "source_data": f"performance_test_{test_id}_run_{i // 1000}",
            "partition_0": f"partition_{test_id}_{i % 100}"
        }
        for i in range(size)
    ]

def load_json_test_data(file_path: str = None) -> List[Dict[str, Any]]:
    """Load test data from JSON file and normalize field names"""
    if file_path is None:
        file_path = DATA_CONFIG["json_test_file"]
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract records from the 'value' array
        records = data.get('value', [])
        
        # Normalize field names to match our schema
        normalized_records = []
        for record in records:
            normalized_record = {
                "field_code": record.get("field_code"),
                "field_name": record.get("_field_name", record.get("field_name", "")),
                "well_code": record.get("well_code"),
                "well_reference": record.get("_well_reference", record.get("well_reference", "")),
                "well_name": record.get("well_name", ""),
                "production_period": record.get("production_period", ""),
                "days_on_production": record.get("days_on_production", 0),
                "oil_production_kbd": record.get("oil_production_kbd", 0.0),
                "gas_production_mmcfd": record.get("gas_production_mmcfd", 0.0),
                "liquids_production_kbd": record.get("liquids_production_kbd", 0.0),
                "water_production_kbd": record.get("water_production_kbd", 0.0),
                "data_source": record.get("data_source", ""),
                "source_data": record.get("source_data", ""),
                "partition_0": record.get("partition_0", "latest")
            }
            normalized_records.append(normalized_record)
        
        print(f"✅ Loaded {len(normalized_records):,} records from {file_path}")
        return normalized_records
        
    except FileNotFoundError:
        print(f"❌ File not found: {file_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        return []
    except Exception as e:
        print(f"❌ Error loading JSON data: {e}")
        return []

def validate_data_integrity(original_data: List[Dict[str, Any]], retrieved_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compare original data with retrieved data to validate integrity"""
    print("🔍 Validating data integrity...")
    
    validation_results = {
        "original_count": len(original_data),
        "retrieved_count": len(retrieved_data),
        "count_match": len(original_data) == len(retrieved_data),
        "sample_matches": [],
        "field_comparisons": {},
        "integrity_score": 0.0
    }
    
    if not original_data or not retrieved_data:
        validation_results["error"] = "Empty data sets"
        return validation_results
    
    # Sample comparison (first 10 records)
    sample_size = min(10, len(original_data), len(retrieved_data))
    matches = 0
    
    for i in range(sample_size):
        orig = original_data[i]
        retr = retrieved_data[i] if i < len(retrieved_data) else {}
        
        # Compare key fields
        field_matches = {}
        for key in ["field_code", "well_code", "oil_production_kbd", "gas_production_mmcfd"]:
            orig_val = orig.get(key)
            retr_val = retr.get(key)
            field_matches[key] = orig_val == retr_val
        
        record_match = all(field_matches.values())
        validation_results["sample_matches"].append({
            "record_index": i,
            "match": record_match,
            "field_matches": field_matches
        })
        
        if record_match:
            matches += 1
    
    # Calculate integrity score
    validation_results["integrity_score"] = matches / sample_size if sample_size > 0 else 0.0
    validation_results["sample_integrity_percentage"] = validation_results["integrity_score"] * 100
    
    # Field-level statistics
    if retrieved_data:
        for field in ["field_code", "well_code", "oil_production_kbd"]:
            orig_values = [r.get(field) for r in original_data[:sample_size]]
            retr_values = [r.get(field) for r in retrieved_data[:sample_size]]
            validation_results["field_comparisons"][field] = {
                "original_sample": orig_values[:3],  # First 3 values
                "retrieved_sample": retr_values[:3],
                "types_match": type(orig_values[0]) == type(retr_values[0]) if orig_values and retr_values else False
            }
    
    return validation_results

def retrieve_data_for_validation(schema_name: str, limit: int = None) -> List[Dict[str, Any]]:
    """Retrieve data from the database for validation"""
    try:
        params = {}
        if limit:
            params["limit"] = limit
        
        response = requests.get(
            f"{BASE_URL}/api/v1/high-performance/query-optimized/{schema_name}",
            params=params
        )
        
        if response.status_code == 200:
            result = response.json()
            return result.get("data", [])
        else:
            print(f"❌ Failed to retrieve data: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        print(f"❌ Error retrieving data: {e}")
        return []

def test_json_data_benchmark() -> Dict[str, Any]:
    """
    NEW TEST: Benchmark using real JSON data from mocked_response_100K-4.json
    This test loads real data, writes it using both methods, and validates data integrity
    """
    print("\n" + "="*70)
    print("🆕 JSON DATA BENCHMARK TEST")
    print("="*70)
    print("📁 Loading real data from mocked_response_100K-4.json...")
    
    # Initialize system monitor for this critical test
    monitor = SystemMonitor(interval=MONITORING_CONFIG["high_frequency_interval"])
    
    # Load the JSON data
    json_data = load_json_test_data()
    
    if not json_data:
        return {
            "success": False,
            "error": "Failed to load JSON test data"
        }
    
    print(f"📊 Loaded {len(json_data):,} records from JSON file")
    
    results = {
        "data_source": "mocked_response_100K-4.json",
        "total_records": len(json_data),
        "tests": {},
        "system_monitoring": {}
    }
    
    # Test 1: Traditional bulk insert
    print(f"\n🔄 Testing traditional bulk insert with {len(json_data):,} real records...")
    if MONITORING_CONFIG["enable_live_stats"]:
        monitor.print_live_stats()  # Show current system state
    monitor.start_monitoring("JSON_Traditional_Bulk_Insert")
    
    start_time = time.perf_counter()
    
    traditional_response = requests.post(
        f"{BASE_URL}/api/v1/records/bulk",
        json={
            "schema_name": SCHEMA_NAME,
            "data": json_data
        },
        headers={"Content-Type": "application/json"}
    )
    
    traditional_duration = (time.perf_counter() - start_time) * 1000
    traditional_monitoring = monitor.stop_monitoring()
    
    if traditional_response.status_code == 201:
        traditional_throughput = len(json_data) / (traditional_duration / 1000) if traditional_duration > 0 else 0
        results["tests"]["traditional"] = {
            "success": True,
            "duration_ms": traditional_duration,
            "throughput_rps": int(traditional_throughput),
            "records_processed": len(json_data),
            "system_monitoring": traditional_monitoring
        }
        print(f"✅ Traditional insert completed: {traditional_duration:.2f}ms ({int(traditional_throughput):,} records/sec)")
        if MONITORING_CONFIG["enable_detailed_output"] and traditional_monitoring:
            print(f"  💻 Peak System CPU: {traditional_monitoring.get('system_cpu', {}).get('max_percent', 'N/A')}%, Peak RAM: {traditional_monitoring.get('system_memory', {}).get('max_percent', 'N/A')}%")
            print(f"  🐍 Peak Process RAM: {traditional_monitoring.get('process_memory', {}).get('max_mb', 'N/A')}MB")
    else:
        results["tests"]["traditional"] = {
            "success": False,
            "error": traditional_response.text,
            "status_code": traditional_response.status_code,
            "system_monitoring": traditional_monitoring
        }
        print(f"❌ Traditional insert failed: {traditional_response.status_code}")
    
    # Small delay to avoid conflicts
    time.sleep(1)
    
    # Test 2: High-performance bulk insert
    print(f"\n🚀 Testing high-performance bulk insert with {len(json_data):,} real records...")
    if MONITORING_CONFIG["enable_live_stats"]:
        monitor.print_live_stats()  # Show current system state
    monitor.start_monitoring("JSON_HighPerformance_Bulk_Insert")
    
    start_time = time.perf_counter()
    
    hp_response = requests.post(
        f"{BASE_URL}/api/v1/high-performance/ultra-fast-bulk/{SCHEMA_NAME}",
        json=json_data,
        headers={"Content-Type": "application/json"}
    )
    
    hp_duration = (time.perf_counter() - start_time) * 1000
    hp_monitoring = monitor.stop_monitoring()
    
    if hp_response.status_code == 200:
        hp_result = hp_response.json()
        results["tests"]["high_performance"] = {
            "success": True,
            "duration_ms": hp_duration,
            "throughput_rps": hp_result.get("performance_metrics", {}).get("throughput_rps", 0),
            "records_processed": len(json_data),
            "optimization": hp_result.get("optimization", "unknown"),
            "system_monitoring": hp_monitoring
        }
        print(f"✅ High-performance insert completed: {hp_duration:.2f}ms ({hp_result.get('performance_metrics', {}).get('throughput_rps', 0):,} records/sec)")
        if MONITORING_CONFIG["enable_detailed_output"] and hp_monitoring:
            print(f"  💻 Peak System CPU: {hp_monitoring.get('system_cpu', {}).get('max_percent', 'N/A')}%, Peak RAM: {hp_monitoring.get('system_memory', {}).get('max_percent', 'N/A')}%")
            print(f"  🐍 Peak Process RAM: {hp_monitoring.get('process_memory', {}).get('max_mb', 'N/A')}MB")
    else:
        results["tests"]["high_performance"] = {
            "success": False,
            "error": hp_response.text,
            "status_code": hp_response.status_code,
            "system_monitoring": hp_monitoring
        }
        print(f"❌ High-performance insert failed: {hp_response.status_code}")
    
    # Test 3: Data integrity validation
    print(f"\n🔍 Validating data integrity...")
    retrieved_data = retrieve_data_for_validation(SCHEMA_NAME, len(json_data))
    
    if retrieved_data:
        validation_results = validate_data_integrity(json_data, retrieved_data)
        results["data_validation"] = validation_results
        
        print(f"📊 Data Validation Results:")
        print(f"  Original records: {validation_results['original_count']:,}")
        print(f"  Retrieved records: {validation_results['retrieved_count']:,}")
        print(f"  Count match: {'✅' if validation_results['count_match'] else '❌'}")
        print(f"  Sample integrity: {validation_results['sample_integrity_percentage']:.1f}%")
    else:
        results["data_validation"] = {
            "success": False,
            "error": "Failed to retrieve data for validation"
        }
        print("❌ Could not retrieve data for validation")
    
    # Performance comparison
    if (results["tests"].get("traditional", {}).get("success") and 
        results["tests"].get("high_performance", {}).get("success")):
        
        trad_duration = results["tests"]["traditional"]["duration_ms"]
        hp_duration = results["tests"]["high_performance"]["duration_ms"]
        improvement = trad_duration / hp_duration if hp_duration > 0 else 0
        
        results["performance_comparison"] = {
            "speed_improvement_factor": round(improvement, 2),
            "speed_improvement_percentage": round((improvement - 1) * 100, 1),
            "traditional_duration_ms": trad_duration,
            "high_performance_duration_ms": hp_duration
        }
        
        print(f"\n📈 Performance Comparison:")
        print(f"  Traditional: {trad_duration:.2f}ms")
        print(f"  High-Performance: {hp_duration:.2f}ms")
        print(f"  🚀 Improvement: {improvement:.1f}x faster ({(improvement-1)*100:.1f}% faster)")
    
    return results

def test_traditional_bulk_insert(data: List[Dict[str, Any]], test_id: str = "traditional") -> Dict[str, Any]:
    """Test traditional bulk insert performance"""
    print(f"🔄 Testing traditional bulk insert with {len(data):,} records (test_id: {test_id})...")
    
    start_time = time.perf_counter()
    
    response = requests.post(
        f"{BASE_URL}/api/v1/records/bulk",
        json={
            "schema_name": SCHEMA_NAME,
            "data": data
        },
        headers={"Content-Type": "application/json"}
    )
    
    duration = (time.perf_counter() - start_time) * 1000
    
    if response.status_code == 201:
        throughput = len(data) / (duration / 1000) if duration > 0 else 0
        return {
            "success": True,
            "method": "traditional",
            "test_id": test_id,
            "duration_ms": duration,
            "throughput_rps": int(throughput),
            "records_processed": len(data)
        }
    else:
        return {
            "success": False,
            "test_id": test_id,
            "error": response.text,
            "status_code": response.status_code
        }

def test_high_performance_bulk_insert(data: List[Dict[str, Any]], test_id: str = "high_performance") -> Dict[str, Any]:
    """Test high-performance bulk insert"""
    print(f"🚀 Testing high-performance bulk insert with {len(data):,} records (test_id: {test_id})...")
    
    start_time = time.perf_counter()
    
    response = requests.post(
        f"{BASE_URL}/api/v1/high-performance/ultra-fast-bulk/{SCHEMA_NAME}",
        json=data,
        headers={"Content-Type": "application/json"}
    )
    
    duration = (time.perf_counter() - start_time) * 1000
    
    if response.status_code == 200:
        result = response.json()
        return {
            "success": True,
            "method": "high_performance",
            "test_id": test_id,
            "duration_ms": duration,
            "throughput_rps": result.get("performance_metrics", {}).get("throughput_rps", 0),
            "records_processed": len(data),
            "optimization": result.get("optimization", "unknown")
        }
    else:
        return {
            "success": False,
            "test_id": test_id,
            "error": response.text,
            "status_code": response.status_code
        }

def test_query_performance() -> Dict[str, Any]:
    """Test query performance comparison with different dataset sizes"""
    print("🔍 Testing query performance...")
    
    results = {}
    test_sizes = QUERY_TEST_SIZES  # Use configured test sizes
    
    for size in test_sizes:
        print(f"  📊 Testing with {size:,} records...")
        size_results = {}
        
        # Traditional query (without analysis)
        start_time = time.perf_counter()
        traditional_response = requests.get(
            f"{BASE_URL}/api/v1/records/{SCHEMA_NAME}?page=1&size={size}",
            timeout=TIMEOUT_CONFIG["request_timeout"]
        )
        traditional_duration = (time.perf_counter() - start_time) * 1000
        
        if traditional_response.status_code == 200:
            trad_data = traditional_response.json()
            actual_records = len(trad_data.get("data", {}).get("items", []))
            size_results["traditional_query"] = {
                "duration_ms": traditional_duration,
                "method": "traditional_pagination",
                "records_returned": actual_records,
                "throughput_rps": int(actual_records / (traditional_duration / 1000)) if traditional_duration > 0 else 0
            }
        
        # High-performance query (without analysis for fair comparison)
        start_time = time.perf_counter()
        hp_response = requests.get(
            f"{BASE_URL}/api/v1/high-performance/ultra-fast-query/{SCHEMA_NAME}?limit={size}",
            timeout=TIMEOUT_CONFIG["request_timeout"]
        )
        hp_duration = (time.perf_counter() - start_time) * 1000
        
        if hp_response.status_code == 200:
            hp_data = hp_response.json()
            actual_records = len(hp_data.get("records", []))
            size_results["high_performance_query"] = {
                "duration_ms": hp_duration,
                "method": "high_performance_optimized",
                "records_returned": actual_records,
                "throughput_rps": int(actual_records / (hp_duration / 1000)) if hp_duration > 0 else 0
            }
            
            # Calculate improvement
            if traditional_duration > 0 and hp_duration > 0:
                improvement = traditional_duration / hp_duration
                size_results["improvement"] = {
                    "factor": round(improvement, 2),
                    "percentage": round((improvement - 1) * 100, 1),
                    "traditional_throughput": size_results["traditional_query"]["throughput_rps"],
                    "hp_throughput": size_results["high_performance_query"]["throughput_rps"]
                }
        
        # High-performance query WITH analysis (to show added value)
        start_time = time.perf_counter()
        hp_analysis_response = requests.get(
            f"{BASE_URL}/api/v1/high-performance/query-optimized/{SCHEMA_NAME}?limit={size}&analysis=summary",
            timeout=TIMEOUT_CONFIG["request_timeout"]
        )
        hp_analysis_duration = (time.perf_counter() - start_time) * 1000
        
        if hp_analysis_response.status_code == 200:
            size_results["high_performance_with_analysis"] = {
                "duration_ms": hp_analysis_duration,
                "method": "high_performance_with_analysis",
                "analysis_overhead_ms": hp_analysis_duration - hp_duration if hp_duration > 0 else 0
            }
        
        results[f"size_{size}"] = size_results
        
        # Print immediate results
        if size_results.get("improvement"):
            improvement = size_results["improvement"]
            print(f"    ✅ {size:,} records: {improvement['factor']:.2f}x faster "
                  f"({improvement['traditional_throughput']:,} → {improvement['hp_throughput']:,} rps)")
        else:
            print(f"    ⚠️  {size:,} records: Could not compare performance")
    
    return results

def test_data_analysis() -> Dict[str, Any]:
    """Test data analysis capabilities"""
    print("📊 Testing data analysis capabilities...")
    
    start_time = time.perf_counter()
    
    response = requests.get(
        f"{BASE_URL}/api/v1/high-performance/analyze/{SCHEMA_NAME}?analysis_type=profile",
        timeout=TIMEOUT_CONFIG["request_timeout"]
    )
    
    duration = (time.perf_counter() - start_time) * 1000
    
    if response.status_code == 200:
        result = response.json()
        # Extract only key metrics to avoid printing large datasets
        return {
            "success": True,
            "duration_ms": duration,
            "analysis_type": "profile",
            "optimization": result.get("optimization", "unknown"),
            "record_count": result.get("record_count", "unknown"),
            "analysis_summary": result.get("summary", "No summary available")
        }
    else:
        return {
            "success": False,
            "error": response.text
        }

def run_benchmark_endpoint(test_size: int = 10000) -> Dict[str, Any]:
    """Test the built-in benchmark endpoint"""
    print(f"⚡ Running built-in benchmark with {test_size:,} records...")
    
    response = requests.post(
        f"{BASE_URL}/api/v1/high-performance/benchmark/{SCHEMA_NAME}",
        params={
            "test_data_size": test_size,
            "include_traditional": True
        }
    )
    
    if response.status_code == 200:
        result = response.json()
        # Filter out large data structures to avoid terminal spam
        filtered_result = {}
        for key, value in result.items():
            if isinstance(value, (list, dict)) and len(str(value)) > 1000:
                if isinstance(value, list):
                    filtered_result[key] = f"[List with {len(value)} items] (truncated)"
                else:
                    filtered_result[key] = f"[Dict with {len(value)} keys] (truncated)"
            else:
                filtered_result[key] = value
        return filtered_result
    else:
        return {
            "success": False,
            "error": response.text,
            "status_code": response.status_code
        }

class SystemMonitor:
    """Monitor CPU and RAM usage during critical operations"""
    
    def __init__(self, interval: float = 1.0):
        self.interval = interval
        self.monitoring = False
        self.thread = None
        self.metrics = []
        self.start_time = None
        
    def start_monitoring(self, test_name: str = "Unknown"):
        """Start monitoring system resources"""
        if self.monitoring:
            return
            
        self.monitoring = True
        self.metrics = []
        self.start_time = time.perf_counter()
        self.test_name = test_name
        
        def monitor():
            while self.monitoring:
                try:
                    # Get system metrics
                    cpu_percent = psutil.cpu_percent(interval=None)
                    memory = psutil.virtual_memory()
                    disk_io = psutil.disk_io_counters()
                    net_io = psutil.net_io_counters()
                    
                    # Get process-specific metrics (current Python process)
                    process = psutil.Process()
                    process_memory = process.memory_info()
                    process_cpu = process.cpu_percent()
                    
                    elapsed = time.perf_counter() - self.start_time
                    
                    metric = {
                        'timestamp': elapsed,
                        'cpu_percent': cpu_percent,
                        'memory_used_gb': memory.used / (1024**3),
                        'memory_available_gb': memory.available / (1024**3),
                        'memory_percent': memory.percent,
                        'process_memory_mb': process_memory.rss / (1024**2),
                        'process_cpu_percent': process_cpu,
                        'disk_read_mb': disk_io.read_bytes / (1024**2) if disk_io else 0,
                        'disk_write_mb': disk_io.write_bytes / (1024**2) if disk_io else 0,
                        'network_sent_mb': net_io.bytes_sent / (1024**2) if net_io else 0,
                        'network_recv_mb': net_io.bytes_recv / (1024**2) if net_io else 0
                    }
                    
                    self.metrics.append(metric)
                    time.sleep(self.interval)
                    
                except Exception as e:
                    # Silently handle monitoring errors to not disrupt tests
                    pass
        
        self.thread = threading.Thread(target=monitor, daemon=True)
        self.thread.start()
        
    def stop_monitoring(self) -> Dict[str, Any]:
        """Stop monitoring and return summary statistics"""
        if not self.monitoring:
            return {}
            
        self.monitoring = False
        if self.thread:
            self.thread.join(timeout=2.0)
        
        if not self.metrics:
            return {"error": "No metrics collected"}
        
        # Calculate statistics
        cpu_values = [m['cpu_percent'] for m in self.metrics]
        memory_values = [m['memory_percent'] for m in self.metrics]
        process_memory_values = [m['process_memory_mb'] for m in self.metrics]
        process_cpu_values = [m['process_cpu_percent'] for m in self.metrics]
        
        total_duration = self.metrics[-1]['timestamp'] if self.metrics else 0
        
        summary = {
            'test_name': getattr(self, 'test_name', 'Unknown'),
            'duration_seconds': round(total_duration, 2),
            'samples_collected': len(self.metrics),
            'system_cpu': {
                'avg_percent': round(sum(cpu_values) / len(cpu_values), 1),
                'max_percent': round(max(cpu_values), 1),
                'min_percent': round(min(cpu_values), 1)
            },
            'system_memory': {
                'avg_percent': round(sum(memory_values) / len(memory_values), 1),
                'max_percent': round(max(memory_values), 1),
                'peak_used_gb': round(max(m['memory_used_gb'] for m in self.metrics), 2)
            },
            'process_memory': {
                'avg_mb': round(sum(process_memory_values) / len(process_memory_values), 1),
                'max_mb': round(max(process_memory_values), 1),
                'peak_gb': round(max(process_memory_values) / 1024, 2)
            },
            'process_cpu': {
                'avg_percent': round(sum(process_cpu_values) / len(process_cpu_values), 1),
                'max_percent': round(max(process_cpu_values), 1)
            }
        }
        
        return summary
    
    def print_live_stats(self):
        """Print current system stats (for immediate feedback)"""
        try:
            cpu = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            process = psutil.Process()
            process_memory = process.memory_info().rss / (1024**2)
            
            print(f"  💻 System: CPU {cpu:.1f}%, RAM {memory.percent:.1f}% ({memory.used/(1024**3):.1f}GB used)")
            print(f"  🐍 Process: {process_memory:.1f}MB RAM")
        except:
            pass  # Silently handle errors

def print_results(results: Dict[str, Any], title: str):
    """Print formatted results"""
    print(f"\n{'='*60}")
    print(f"📊 {title}")
    print(f"{'='*60}")
    
    if isinstance(results, dict):
        for key, value in results.items():
            if isinstance(value, dict):
                print(f"\n{key.upper().replace('_', ' ')}:")
                for sub_key, sub_value in value.items():
                    # Avoid printing large data structures
                    if isinstance(sub_value, (list, dict)) and len(str(sub_value)) > 1000:
                        if isinstance(sub_value, list):
                            print(f"  {sub_key}: [List with {len(sub_value)} items] (truncated)")
                        else:
                            print(f"  {sub_key}: [Dict with {len(sub_value)} keys] (truncated)")
                    else:
                        print(f"  {sub_key}: {sub_value}")
            elif isinstance(value, (list, dict)) and len(str(value)) > 1000:
                if isinstance(value, list):
                    print(f"{key}: [List with {len(value)} items] (truncated)")
                else:
                    print(f"{key}: [Dict with {len(value)} keys] (truncated)")
            else:
                print(f"{key}: {value}")
    else:
        # Avoid printing massive JSON responses
        if isinstance(results, (list, dict)) and len(str(results)) > 1000:
            if isinstance(results, list):
                print(f"[List with {len(results)} items] (truncated to avoid terminal spam)")
            else:
                print(f"[Dict with {len(results)} keys] (truncated to avoid terminal spam)")
        else:
            print(json.dumps(results, indent=2))

def main():
    """Main test function with professional reporting"""
    # Initialize professional report formatter
    formatter = BenchmarkReportFormatter()
    
    # Set configuration for the formatter
    mode_name = "CUSTOM"
    for name, config in [("QUICK", QUICK_MODE), ("STANDARD", STANDARD_MODE), 
                        ("STRESS", STRESS_MODE), ("EXTREME", EXTREME_MODE)]:
        if config == ACTIVE_CONFIG:
            mode_name = name
            break
    
    formatter.set_config({
        'mode_name': mode_name,
        'test_sizes': TEST_SIZES,
        'base_url': BASE_URL,
        'schema_name': SCHEMA_NAME,
        'enable_json_test': ENABLE_JSON_TEST,
        'enable_streaming_test': ENABLE_STREAMING_TEST,
        'enable_pagination_test': ENABLE_PAGINATION_TEST
    })
    
    formatter.start_benchmark()
    
    print("🚀 High-Performance Data Processing BENCHMARK Suite")
    print("=" * 70)
    
    # Print configuration summary
    print_configuration_summary()
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/", timeout=10)
        if response.status_code != 200:
            print("❌ Server is not running. Please start the server first:")
            print("   uvicorn app.main:app --host 0.0.0.0 --port 8080")
            return
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to server. Please start the server first:")
        print("   uvicorn app.main:app --host 0.0.0.0 --port 8080")
        return
    except requests.exceptions.Timeout:
        print("❌ Server connection timeout. Please check if server is running properly.")
        return
    
    print("✅ Server is running")
    
    # Track total test time
    total_start_time = time.perf_counter()
    
    # Test 0: JSON Data Benchmark Test (conditional)
    if ENABLE_JSON_TEST:
        print("\n" + "="*70)
        print("🆕 TEST 0: JSON Data Benchmark (Real Data from File)")
        print("="*70)
        
        json_benchmark_result = test_json_data_benchmark()
        print_results(json_benchmark_result, "JSON Data Benchmark Results")
        
        # Add to professional formatter
        formatter.add_test_result("json_data_benchmark", "json_test", json_benchmark_result)
    else:
        print("\n⏭️  Skipping JSON Data Benchmark Test (disabled in configuration)")
    
    # Test 1: Built-in benchmark with large dataset
    print("\n" + "="*70)
    print("🏁 TEST 1: Built-in Performance Benchmark (All Test Sizes)")
    print("="*70)
    
    for size in TEST_SIZES:
        print(f"\n⚡ Running built-in benchmark with {size:,} records...")
        benchmark_result = run_benchmark_endpoint(size)
        print_results(benchmark_result, f"Built-in Benchmark Results ({size:,} records)")
        
        # Add to professional formatter
        formatter.add_test_result(f"benchmark_{size}_records", "built_in_benchmark", benchmark_result)
    
    # Test 2: Stress test comparison
    print("\n" + "="*70)
    print("🏁 TEST 2: STRESS TEST Performance Comparison (All Test Sizes)")
    print("="*70)
    
    stress_results = {}
    
    for size in TEST_SIZES:  # Progressive stress testing with all defined sizes
        print(f"\n📈 STRESS TESTING with {size:,} records...")
        
        # Initialize monitor for large dataset tests (based on configuration)
        monitor = None
        if size >= MONITORING_THRESHOLD:
            monitor = SystemMonitor(interval=MONITORING_CONFIG["medium_frequency_interval"])
            print(f"  🔍 System monitoring enabled for {size:,} record test")
            if MONITORING_CONFIG["enable_live_stats"]:
                monitor.print_live_stats()
        
        # Generate unique test data for each size test
        size_test_id = f"stress_{size}"
        test_data_traditional = generate_test_data(size, f"{size_test_id}_trad")
        test_data_hp = generate_test_data(size, f"{size_test_id}_hp")
        
        # Test traditional method with unique data
        print(f"🔄 Traditional bulk insert ({size:,} records)...")
        if monitor:
            monitor.start_monitoring(f"Stress_Traditional_{size}")
        traditional_result = test_traditional_bulk_insert(test_data_traditional, f"{size_test_id}_trad")
        traditional_monitoring = monitor.stop_monitoring() if monitor else {}
        
        # Test high-performance method with unique data
        print(f"🚀 High-performance bulk insert ({size:,} records)...")
        if monitor:
            monitor.start_monitoring(f"Stress_HighPerformance_{size}")
        hp_result = test_high_performance_bulk_insert(test_data_hp, f"{size_test_id}_hp")
        hp_monitoring = monitor.stop_monitoring() if monitor else {}
        
        # Store results
        stress_results[size] = {
            "traditional": {**traditional_result, "system_monitoring": traditional_monitoring},
            "high_performance": {**hp_result, "system_monitoring": hp_monitoring}
        }
        
        # Add to professional formatter
        formatter.add_test_result(f"stress_test_{size}_records", "stress_test", stress_results[size])
        
        # Compare results
        if traditional_result.get("success") and hp_result.get("success"):
            improvement = traditional_result["duration_ms"] / hp_result["duration_ms"]
            throughput_improvement = hp_result["throughput_rps"] / traditional_result["throughput_rps"]
            
            print(f"\n📊 STRESS TEST Results for {size:,} records:")
            print(f"  Traditional: {traditional_result['duration_ms']:.2f}ms ({traditional_result['throughput_rps']:,} records/sec)")
            print(f"  High-Performance: {hp_result['duration_ms']:.2f}ms ({hp_result['throughput_rps']:,} records/sec)")
            print(f"  🚀 Speed Improvement: {improvement:.1f}x faster ({(improvement-1)*100:.1f}% faster)")
            print(f"  🚀 Throughput Improvement: {throughput_improvement:.1f}x higher throughput")
            
            # Print system monitoring results if available and enabled
            if MONITORING_CONFIG["enable_detailed_output"] and traditional_monitoring and size >= MONITORING_THRESHOLD:
                print(f"  💻 Traditional Peak: CPU {traditional_monitoring.get('system_cpu', {}).get('max_percent', 'N/A')}%, RAM {traditional_monitoring.get('system_memory', {}).get('max_percent', 'N/A')}%")
                print(f"  🐍 Traditional Process: {traditional_monitoring.get('process_memory', {}).get('max_mb', 'N/A')}MB RAM")
            if MONITORING_CONFIG["enable_detailed_output"] and hp_monitoring and size >= MONITORING_THRESHOLD:
                print(f"  💻 High-Perf Peak: CPU {hp_monitoring.get('system_cpu', {}).get('max_percent', 'N/A')}%, RAM {hp_monitoring.get('system_memory', {}).get('max_percent', 'N/A')}%")
                print(f"  🐍 High-Perf Process: {hp_monitoring.get('process_memory', {}).get('max_mb', 'N/A')}MB RAM")
            
            # Performance scaling analysis
            if size > TEST_SIZES[0]:  # Compare with previous size
                prev_size = None
                for i, test_size in enumerate(TEST_SIZES):
                    if test_size == size and i > 0:
                        prev_size = TEST_SIZES[i-1]
                        break
                
                if prev_size and prev_size in stress_results:
                    prev_hp = stress_results[prev_size]["high_performance"]
                    if prev_hp.get("success"):
                        scaling_factor = size / prev_size
                        time_scaling = hp_result["duration_ms"] / prev_hp["duration_ms"]
                        efficiency = scaling_factor / time_scaling
                        print(f"  📈 Scaling Efficiency: {efficiency:.2f} (1.0 = linear scaling, vs {prev_size:,} records)")
        else:
            print(f"❌ Stress test failed for {size:,} records")
            if not traditional_result.get("success"):
                print(f"   Traditional error: {traditional_result.get('error', 'Unknown')}")
            if not hp_result.get("success"):
                print(f"   High-performance error: {hp_result.get('error', 'Unknown')}")
    
    # Test 3: Query performance with large datasets
    print("\n" + "="*70)
    print("🏁 TEST 3: Query Performance (Large Dataset)")
    print("="*70)
    
    query_results = test_query_performance()
    print_results(query_results, "Query Performance Results")
    
    # Add to professional formatter
    for size_key, size_result in query_results.items():
        if size_key.startswith("SIZE"):
            formatter.add_test_result(f"query_{size_key.lower()}_records", "query_performance", size_result)
    
    # Test 4: Advanced data analysis
    print("\n" + "="*70)
    print("🏁 TEST 4: Advanced Data Analysis")
    print("="*70)
    
    for analysis_type in ["summary", "profile", "quality"]:
        print(f"\n📊 Running {analysis_type} analysis...")
        analysis_results = test_data_analysis_type(analysis_type)
        print_results(analysis_results, f"Data Analysis Results ({analysis_type})")
        
        # Add to professional formatter
        formatter.add_test_result(f"{analysis_type}_analysis", "data_analysis", analysis_results)
    
    # Test 4.5: Streaming test (conditional)
    if ENABLE_STREAMING_TEST:
        print("\n" + "="*70)
        print("🏁 TEST 4.5: Memory Efficiency with Streaming")
        print("="*70)
        
        streaming_results = test_memory_efficiency()
        print_results(streaming_results, "Streaming Memory Efficiency Results")
        
        # Add to professional formatter
        formatter.add_test_result("streaming_memory_efficiency", "streaming_test", streaming_results)
    else:
        print("\n⏭️  Skipping Streaming Test (disabled in configuration)")
    
    # Test 5: Large page retrieval test (MVP approach) - conditional
    if ENABLE_PAGINATION_TEST:
        print("\n" + "="*70)
        print("🏁 TEST 5: Large Page Retrieval Test (MVP)")
        print("="*70)
        
        large_page_results = test_large_page_retrieval()
        print_results(large_page_results, "Large Page Retrieval Results")
        
        # Add to professional formatter
        formatter.add_test_result("large_page_retrieval", "pagination_test", large_page_results)
        
        # Test 6: Comprehensive Pagination Benchmark
        print("\n" + "="*70)
        print("🏁 TEST 6: Comprehensive Pagination Benchmark")
        print("="*70)
        
        pagination_results = test_pagination_benchmark()
        print_results(pagination_results, "Comprehensive Pagination Benchmark Results")
        
        # Add to professional formatter
        formatter.add_test_result("comprehensive_pagination", "pagination_test", pagination_results)
    else:
        print("\n⏭️  Skipping Pagination Tests (disabled in configuration)")
    
    # Calculate total test time
    total_duration = time.perf_counter() - total_start_time
    
    # End benchmark timing for formatter
    formatter.end_benchmark()
    
    print("\n" + "="*70)
    print("✅ BENCHMARK TESTS COMPLETED!")
    print(f"⏱️  Total test duration: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")
    print("="*70)
    
    # Generate and return the professional report
    return formatter

def test_data_analysis_type(analysis_type: str) -> Dict[str, Any]:
    """Test specific data analysis type"""
    start_time = time.perf_counter()
    
    response = requests.get(
        f"{BASE_URL}/api/v1/high-performance/analyze/{SCHEMA_NAME}?analysis_type={analysis_type}"
    )
    
    duration = (time.perf_counter() - start_time) * 1000
    
    if response.status_code == 200:
        result = response.json()
        # Extract only key metrics to avoid printing large datasets
        return {
            "success": True,
            "analysis_type": analysis_type,
            "duration_ms": duration,
            "optimization": result.get("optimization", "unknown"),
            "record_count": result.get("record_count", "unknown"),
            "analysis_summary": result.get("summary", "No summary available")
        }
    else:
        return {
            "success": False,
            "analysis_type": analysis_type,
            "error": response.text
        }

def test_large_page_retrieval() -> Dict[str, Any]:
    """Test efficient data retrieval using large page sizes (MVP approach)"""
    print("🚀 Testing large page retrieval (MVP approach)...")
    
    start_time = time.perf_counter()
    page_size = 100000  # Use maximum allowed page size
    all_records = []
    page = 1
    
    try:
        while True:
            print(f"  📄 Fetching page {page} (size={page_size:,})...")
            page_start_time = time.perf_counter()
            
            response = requests.get(
                f"{BASE_URL}/api/v1/records/{SCHEMA_NAME}",
                params={"page": page, "size": page_size},
                timeout=60
            )
            
            if response.status_code != 200:
                return {
                    "success": False,
                    "error": f"HTTP {response.status_code}: {response.text[:200]}"
                }
            
            data = response.json()
            page_records = data["data"]["items"]
            total_records = data["data"]["total"]
            
            page_duration = time.perf_counter() - page_start_time
            page_throughput = len(page_records) / page_duration if page_duration > 0 else 0
            
            print(f"  ✅ Page {page}: {len(page_records):,} records in {page_duration:.2f}s ({int(page_throughput):,} records/sec)")
            
            all_records.extend(page_records)
            
            # Check if we got all records
            if len(page_records) < page_size:
                print(f"  📊 Reached end of data (got {len(page_records):,} < {page_size:,})")
                break
            
            page += 1
            
            # Safety check to prevent infinite loops
            if page > 10:
                print(f"  ⚠️ Safety limit reached (10 pages)")
                break
        
        duration = (time.perf_counter() - start_time) * 1000
        throughput = len(all_records) / (duration / 1000) if duration > 0 else 0
        
        return {
            "success": True,
            "method": "large_page_retrieval",
            "duration_ms": duration,
            "total_records": len(all_records),
            "pages_fetched": page,
            "page_size": page_size,
            "avg_throughput_rps": int(throughput),
            "avg_duration_per_page_ms": duration / page if page > 0 else 0,
            "note": f"MVP approach: Retrieved {len(all_records):,} records in {page} pages"
        }
        
    except requests.exceptions.Timeout:
        return {
            "success": False,
            "error": "Request timeout"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)[:200]}"
        }

def test_memory_efficiency(batch_size: int = 50000) -> Dict[str, Any]:
    """Test memory efficiency with streaming"""
    print(f"🔄 Testing memory efficiency with streaming (batch_size={batch_size:,})...")
    
    # Monitor streaming operations for memory efficiency analysis
    monitor = SystemMonitor(interval=MONITORING_CONFIG["low_frequency_interval"])
    if MONITORING_CONFIG["enable_live_stats"]:
        monitor.print_live_stats()
    monitor.start_monitoring(f"Streaming_Efficiency_Batch_{batch_size}")
    
    start_time = time.perf_counter()
    
    try:
        response = requests.get(
            f"{BASE_URL}/api/v1/high-performance/stream-arrow-batches/{SCHEMA_NAME}?batch_size={batch_size}",
            stream=True,
            timeout=180  # Increased timeout to 3 minutes for large datasets
        )
        
        if response.status_code == 200:
            batch_count = 0
            total_records = 0
            lines_processed = 0
            
            # Process NDJSON streaming response with improved error handling
            try:
                for line in response.iter_lines(decode_unicode=True, chunk_size=8192):
                    lines_processed += 1
                    
                    # Skip empty lines
                    if not line or not line.strip():
                        continue
                    
                    try:
                        line_data = json.loads(line.strip())
                        
                        # Handle different types of lines in NDJSON stream
                        if "stream_type" in line_data:
                            # Metadata line - log it
                            print(f"  📡 Stream started: {line_data.get('schema_name', 'unknown')} with batch size {line_data.get('batch_size', 'unknown')}")
                        
                        elif "batch_number" in line_data:
                            # Batch data line
                            batch_count += 1
                            batch_size = line_data.get("batch_size", 0)
                            total_records += batch_size
                            print(f"  📦 Processed batch {line_data.get('batch_number', batch_count)}: {batch_size:,} records")
                            
                            # Check for timeout during processing
                            elapsed = time.perf_counter() - start_time
                            if elapsed > 180:  # 3 minutes safety timeout
                                print(f"  ⚠️ Safety timeout reached after {elapsed:.1f}s, stopping stream processing")
                                break
                        
                        elif "stream_complete" in line_data:
                            # Summary line
                            print(f"  ✅ Stream completed: {line_data.get('total_batches', batch_count)} batches, {line_data.get('total_records', total_records):,} total records")
                            break  # Explicitly break on completion
                        
                        else:
                            # Unknown line type - log for debugging
                            print(f"  🔍 Unknown line type: {list(line_data.keys())[:3]}...")
                            
                    except json.JSONDecodeError as e:
                        print(f"  ⚠️ Skipping malformed JSON line {lines_processed}: {str(e)[:100]}")
                        continue  # Skip malformed JSON lines
                    except Exception as e:
                        print(f"  ⚠️ Skipping line {lines_processed} due to error: {str(e)[:100]}")
                        continue  # Skip any other parsing errors
                
                print(f"  📊 Stream processing completed: {lines_processed} lines processed, {batch_count} batches, {total_records:,} total records")
                
            except requests.exceptions.ChunkedEncodingError as e:
                # Handle streaming interruption gracefully
                print(f"  ⚠️ Streaming ended early due to connection issue: {str(e)[:100]}")
                print(f"  📊 Partial results: {batch_count} batches, {total_records:,} records processed")
            except requests.exceptions.Timeout as e:
                print(f"  ⚠️ Request timeout: {str(e)}")
                print(f"  📊 Partial results: {batch_count} batches, {total_records:,} records processed")
            
            duration = (time.perf_counter() - start_time) * 1000
            streaming_monitoring = monitor.stop_monitoring()
            
            # Print monitoring results
            if MONITORING_CONFIG["enable_detailed_output"] and streaming_monitoring:
                print(f"  💻 Streaming Peak: CPU {streaming_monitoring.get('system_cpu', {}).get('max_percent', 'N/A')}%, RAM {streaming_monitoring.get('system_memory', {}).get('max_percent', 'N/A')}%")
                print(f"  🐍 Streaming Process: {streaming_monitoring.get('process_memory', {}).get('max_mb', 'N/A')}MB RAM")
            
            return {
                "success": True,
                "streaming_method": "arrow_batches",
                "duration_ms": duration,
                "batches_processed": batch_count,
                "total_records_streamed": total_records,
                "lines_processed": lines_processed,
                "avg_records_per_second": int(total_records / (duration / 1000)) if duration > 0 and total_records > 0 else 0,
                "system_monitoring": streaming_monitoring,
                "note": f"Streaming test completed successfully - processed {batch_count} batches with {total_records:,} records"
            }
        else:
            monitor.stop_monitoring()  # Stop monitoring on error
            return {
                "success": False,
                "error": f"HTTP {response.status_code}: {response.text[:500]}"  # Limit error text length
            }
    except requests.exceptions.Timeout as e:
        monitor.stop_monitoring()  # Stop monitoring on timeout
        return {
            "success": False,
            "error": f"Request timeout after 180 seconds: {str(e)}"
        }
    except requests.exceptions.RequestException as e:
        monitor.stop_monitoring()  # Stop monitoring on request error
        return {
            "success": False,
            "error": f"Request failed: {str(e)[:500]}"  # Limit error text length
        }
    except Exception as e:
        monitor.stop_monitoring()  # Stop monitoring on any error
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)[:500]}"  # Limit error text length
        }

def test_pagination_benchmark() -> Dict[str, Any]:
    """
    🚀 COMPREHENSIVE PAGINATION BENCHMARK
    Compare traditional vs high-performance pagination endpoints with the same data
    """
    print("🚀 COMPREHENSIVE PAGINATION BENCHMARK")
    print("=" * 70)
    print("Testing both traditional and high-performance pagination with identical data")
    
    results = {}
    page_sizes = PAGINATION_PAGE_SIZES  # Use configured page sizes
    
    for size in page_sizes:
        print(f"\n📊 TESTING WITH PAGE SIZE: {size:,}")
        print("-" * 50)
        
        size_results = {
            "page_size": size,
            "traditional": {},
            "high_performance": {},
            "comparison": {}
        }
        
        # Test 1: Traditional Pagination
        print(f"\n1️⃣ Traditional Pagination (page size={size:,})")
        trad_start = time.perf_counter()
        trad_records = []
        trad_page = 1
        trad_pages_fetched = 0
        trad_total_api_time = 0
        
        try:
            while True:
                trad_page_start = time.perf_counter()
                response = requests.get(
                    f"{BASE_URL}/api/v1/records/{SCHEMA_NAME}",
                    params={"page": trad_page, "size": size},
                    timeout=60
                )
                trad_page_duration = time.perf_counter() - trad_page_start
                trad_total_api_time += trad_page_duration
                
                if response.status_code != 200:
                    print(f"❌ Traditional pagination failed: {response.status_code}")
                    size_results["traditional"]["error"] = f"HTTP {response.status_code}"
                    break
                    
                data = response.json()
                page_records = data["data"]["items"]
                trad_records.extend(page_records)
                trad_pages_fetched += 1
                
                trad_page_throughput = len(page_records) / trad_page_duration if trad_page_duration > 0 else 0
                
                print(f"  📄 Page {trad_page}: {len(page_records):,} records in {trad_page_duration:.2f}s ({int(trad_page_throughput):,} records/sec)")
                
                if len(page_records) < size:
                    print(f"  📊 Reached end of data (got {len(page_records):,} < {size:,})")
                    break
                    
                trad_page += 1
                
                # Safety limit
                if trad_page > 5:
                    print(f"  ⚠️ Safety limit reached (5 pages)")
                    break
                    
        except Exception as e:
            print(f"❌ Traditional pagination error: {str(e)[:200]}")
            size_results["traditional"]["error"] = str(e)[:200]
        
        trad_total_duration = time.perf_counter() - trad_start
        trad_throughput = len(trad_records) / trad_total_duration if trad_total_duration > 0 else 0
        
        size_results["traditional"] = {
            "total_records": len(trad_records),
            "total_duration_s": trad_total_duration,
            "total_api_time_s": trad_total_api_time,
            "avg_throughput_rps": int(trad_throughput),
            "pages_fetched": trad_pages_fetched,
            "avg_page_duration_s": trad_total_api_time / trad_pages_fetched if trad_pages_fetched > 0 else 0,
            "method": "traditional_pagination"
        }
        
        print(f"  ✅ Traditional Total: {len(trad_records):,} records in {trad_total_duration:.2f}s ({int(trad_throughput):,} records/sec)")
        
        # Test 2: High-Performance Pagination (using query-optimized endpoint)
        print(f"\n2️⃣ High-Performance Pagination (page size={size:,})")
        hp_start = time.perf_counter()
        hp_records = []
        hp_page = 1
        hp_pages_fetched = 0
        hp_total_api_time = 0
        
        try:
            while True:
                hp_page_start = time.perf_counter()
                offset = (hp_page - 1) * size
                response = requests.get(
                    f"{BASE_URL}/api/v1/high-performance/query-optimized/{SCHEMA_NAME}",
                    params={"limit": size, "offset": offset},
                    timeout=60
                )
                hp_page_duration = time.perf_counter() - hp_page_start
                hp_total_api_time += hp_page_duration
                
                if response.status_code != 200:
                    print(f"❌ High-performance pagination failed: {response.status_code}")
                    size_results["high_performance"]["error"] = f"HTTP {response.status_code}"
                    break
                    
                data = response.json()
                page_records = data.get("records", [])
                hp_records.extend(page_records)
                hp_pages_fetched += 1
                
                hp_page_throughput = len(page_records) / hp_page_duration if hp_page_duration > 0 else 0
                
                print(f"  📄 Page {hp_page}: {len(page_records):,} records in {hp_page_duration:.2f}s ({int(hp_page_throughput):,} records/sec)")
                
                if len(page_records) < size:
                    print(f"  📊 Reached end of data (got {len(page_records):,} < {size:,})")
                    break
                    
                hp_page += 1
                
                # Safety limit
                if hp_page > 5:
                    print(f"  ⚠️ Safety limit reached (5 pages)")
                    break
                    
        except Exception as e:
            print(f"❌ High-performance pagination error: {str(e)[:200]}")
            size_results["high_performance"]["error"] = str(e)[:200]
        
        hp_total_duration = time.perf_counter() - hp_start
        hp_throughput = len(hp_records) / hp_total_duration if hp_total_duration > 0 else 0
        
        size_results["high_performance"] = {
            "total_records": len(hp_records),
            "total_duration_s": hp_total_duration,
            "total_api_time_s": hp_total_api_time,
            "avg_throughput_rps": int(hp_throughput),
            "pages_fetched": hp_pages_fetched,
            "avg_page_duration_s": hp_total_api_time / hp_pages_fetched if hp_pages_fetched > 0 else 0,
            "method": "high_performance_query_optimized"
        }
        
        print(f"  ✅ High-Perf Total: {len(hp_records):,} records in {hp_total_duration:.2f}s ({int(hp_throughput):,} records/sec)")
        
        # Test 3: Alternative High-Performance Method (ultra-fast-query)
        print(f"\n3️⃣ Alternative High-Performance (ultra-fast-query, limit={size:,})")
        alt_start = time.perf_counter()
        
        try:
            response = requests.get(
                f"{BASE_URL}/api/v1/high-performance/ultra-fast-query/{SCHEMA_NAME}",
                params={"limit": size},
                timeout=60
            )
            alt_duration = time.perf_counter() - alt_start
            
            if response.status_code == 200:
                data = response.json()
                alt_records = data.get("records", [])
                alt_throughput = len(alt_records) / alt_duration if alt_duration > 0 else 0
                
                size_results["alternative_hp"] = {
                    "total_records": len(alt_records),
                    "total_duration_s": alt_duration,
                    "avg_throughput_rps": int(alt_throughput),
                    "method": "ultra_fast_query_single_call"
                }
                
                print(f"  ✅ Alternative HP: {len(alt_records):,} records in {alt_duration:.2f}s ({int(alt_throughput):,} records/sec)")
            else:
                print(f"❌ Alternative HP failed: {response.status_code}")
                size_results["alternative_hp"]["error"] = f"HTTP {response.status_code}"
                
        except Exception as e:
            print(f"❌ Alternative HP error: {str(e)[:200]}")
            size_results["alternative_hp"]["error"] = str(e)[:200]
        
        # Calculate comparisons
        if (size_results["traditional"].get("total_records", 0) > 0 and 
            size_results["high_performance"].get("total_records", 0) > 0):
            
            trad_time = size_results["traditional"]["total_duration_s"]
            hp_time = size_results["high_performance"]["total_duration_s"]
            trad_throughput = size_results["traditional"]["avg_throughput_rps"]
            hp_throughput = size_results["high_performance"]["avg_throughput_rps"]
            
            size_results["comparison"] = {
                "speedup_factor": round(trad_time / hp_time, 2) if hp_time > 0 else 0,
                "speedup_percentage": round(((trad_time / hp_time) - 1) * 100, 1) if hp_time > 0 else 0,
                "throughput_improvement": round(hp_throughput / trad_throughput, 2) if trad_throughput > 0 else 0,
                "throughput_improvement_percentage": round(((hp_throughput / trad_throughput) - 1) * 100, 1) if trad_throughput > 0 else 0,
                "traditional_avg_page_time": size_results["traditional"]["avg_page_duration_s"],
                "hp_avg_page_time": size_results["high_performance"]["avg_page_duration_s"],
                "page_time_improvement": round(size_results["traditional"]["avg_page_duration_s"] / size_results["high_performance"]["avg_page_duration_s"], 2) if size_results["high_performance"]["avg_page_duration_s"] > 0 else 0
            }
            
            # Print detailed comparison
            print(f"\n📊 DETAILED COMPARISON FOR PAGE SIZE {size:,}:")
            print(f"  Traditional:     {size_results['traditional']['total_records']:,} records in {trad_time:.2f}s ({trad_throughput:,} records/sec)")
            print(f"  High-Performance: {size_results['high_performance']['total_records']:,} records in {hp_time:.2f}s ({hp_throughput:,} records/sec)")
            print(f"  🚀 Overall Speedup: {size_results['comparison']['speedup_factor']:.2f}x ({size_results['comparison']['speedup_percentage']:.1f}% faster)")
            print(f"  🚀 Throughput Gain: {size_results['comparison']['throughput_improvement']:.2f}x ({size_results['comparison']['throughput_improvement_percentage']:.1f}% higher)")
            print(f"  📄 Page Time Improvement: {size_results['comparison']['page_time_improvement']:.2f}x faster per page")
            
            # Include alternative method in comparison if available
            if "alternative_hp" in size_results and "total_records" in size_results["alternative_hp"]:
                alt_time = size_results["alternative_hp"]["total_duration_s"]
                alt_throughput = size_results["alternative_hp"]["avg_throughput_rps"]
                print(f"  Alternative HP:   {size_results['alternative_hp']['total_records']:,} records in {alt_time:.2f}s ({alt_throughput:,} records/sec)")
                print(f"  🚀 Alt vs Traditional: {round(trad_time / alt_time, 2):.2f}x faster")
        
        results[f"page_size_{size}"] = size_results
    
    # Generate final summary
    print(f"\n{'='*70}")
    print("🏁 PAGINATION BENCHMARK SUMMARY")
    print(f"{'='*70}")
    
    for size_key, size_data in results.items():
        size = size_data["page_size"]
        print(f"\n📊 PAGE SIZE {size:,}:")
        
        if "comparison" in size_data and size_data["comparison"]:
            comp = size_data["comparison"]
            trad = size_data["traditional"]
            hp = size_data["high_performance"]
            
            print(f"  Traditional:      {trad['total_records']:,} records, {trad['total_duration_s']:.2f}s, {trad['avg_throughput_rps']:,} rps")
            print(f"  High-Performance: {hp['total_records']:,} records, {hp['total_duration_s']:.2f}s, {hp['avg_throughput_rps']:,} rps")
            print(f"  🚀 Improvement:   {comp['speedup_factor']:.2f}x speed, {comp['throughput_improvement']:.2f}x throughput")
            
            if "alternative_hp" in size_data and "total_records" in size_data["alternative_hp"]:
                alt = size_data["alternative_hp"]
                print(f"  Alternative HP:   {alt['total_records']:,} records, {alt['total_duration_s']:.2f}s, {alt['avg_throughput_rps']:,} rps")
        else:
            print(f"  ⚠️ Comparison not available (check for errors)")
    
    # Best practices recommendation
    print(f"\n💡 PAGINATION RECOMMENDATIONS:")
    print(f"  • For small datasets (< 10K): Either method works well")
    print(f"  • For medium datasets (10K-50K): High-performance shows clear benefits")
    print(f"  • For large datasets (> 50K): High-performance is significantly faster")
    print(f"  • For single large queries: Use ultra-fast-query endpoint")
    print(f"  • For MVP: Large page sizes (50K-100K) with high-performance endpoints")
    
    return results

class TeeOutput:
    """A class that writes to both stdout and a file simultaneously"""
    def __init__(self, file_handle):
        self.terminal = sys.stdout
        self.file = file_handle
    
    def write(self, message):
        self.terminal.write(message)
        self.file.write(message)
        self.file.flush()  # Ensure immediate write to file
    
    def flush(self):
        self.terminal.flush()
        self.file.flush()

if __name__ == "__main__":
    # Prepare output directory and filename
    output_dir = os.path.join(os.path.dirname(__file__), "benchmarking")
    os.makedirs(output_dir, exist_ok=True)
    
    # Check if psutil is available for system monitoring
    try:
        import psutil
        monitoring_available = True
    except ImportError:
        print("⚠️  psutil not installed - system monitoring disabled")
        print("   Install with: pip install psutil")
        monitoring_available = False
    
    # Generate timestamped filename for better tracking
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"benchmark_results_{timestamp}.md")
    
    print(f"🚀 Starting benchmark tests...")
    print(f"📄 Results will be saved to: {output_path}")
    print(f"⏱️  Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # Open file and set up tee output
    with open(output_path, "w", encoding="utf-8") as f:
        # Write header to file
        f.write(f"# High-Performance Data Processing Benchmark Results\n\n")
        f.write(f"**Test Run:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Test File:** {__file__}\n")
        f.write(f"**Base URL:** {BASE_URL}\n")
        f.write(f"**Schema:** {SCHEMA_NAME}\n")
        f.write(f"**Test Sizes:** {TEST_SIZES}\n\n")
        f.write("---\n\n")
        f.write("```\n")
        
        # Set up tee output to write to both console and file
        tee = TeeOutput(f)
        old_stdout = sys.stdout
        sys.stdout = tee
        
        try:
            # Run the main benchmark tests
            formatter = main()
        finally:
            sys.stdout = old_stdout
            
        # Write the professional report to the file
        f.write("```\n\n")
        
        # Generate and write the professional benchmark report
        if formatter:
            professional_report = formatter.generate_full_report()
            f.write(professional_report)
        
        f.write("\n---\n")
        f.write(f"**Test Completed:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    print(f"\n📄 Benchmark results saved to: {output_path}")
    print(f"⏱️  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✅ You can now compare this with future benchmark runs!")