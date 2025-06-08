# run_tests.py
"""
Simple test runner for the React FastAPI V12 project.
"""
import subprocess
import sys


def run_tests():
    """Run different test suites and provide a summary."""
    test_results = {}
    
    print("🧪 Running React FastAPI V12 Test Suite")
    print("=" * 50)
    
    # Test categories
    test_suites = [
        ("Hello World Tests", "tests/test_hello_world.py"),
        ("Domain Entity Tests", "tests/test_domain/test_entities.py"),
        ("Domain Exception Tests", "tests/test_domain/test_exceptions.py"),
        ("API Tests", "tests/test_api/test_main.py"),
        ("Configuration Tests", "tests/test_config/test_settings.py"),
        ("All Tests", "tests/"),
    ]
    
    for suite_name, test_path in test_suites:
        print(f"\n📋 Running {suite_name}...")
        print("-" * 40)
        
        try:
            # Run pytest with minimal output
            result = subprocess.run(
                ["pytest", test_path, "-v", "--tb=short"],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                test_results[suite_name] = "✅ PASSED"
                print(f"✅ {suite_name}: PASSED")
            else:
                test_results[suite_name] = "❌ FAILED"
                print(f"❌ {suite_name}: FAILED")
                # Print a few lines of error output
                if result.stdout:
                    lines = result.stdout.split('\n')
                    error_lines = [line for line in lines if 'FAILED' in line or 'ERROR' in line]
                    for line in error_lines[:3]:  # Show first 3 error lines
                        print(f"   {line}")
                        
        except subprocess.TimeoutExpired:
            test_results[suite_name] = "⏰ TIMEOUT"
            print(f"⏰ {suite_name}: TIMEOUT")
        except Exception as e:
            test_results[suite_name] = f"💥 ERROR: {str(e)}"
            print(f"💥 {suite_name}: ERROR - {str(e)}")
    
    # Print summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for result in test_results.values() if "PASSED" in result)
    total = len(test_results)
    
    for suite_name, result in test_results.items():
        print(f"{result} {suite_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} test suites passed")
    
    if passed == total:
        print("🎉 All test suites passed!")
        return True
    else:
        print("⚠️  Some test suites failed. Check the output above for details.")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
