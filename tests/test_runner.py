# test_runner.py
"""
Test runner script for the React FastAPI project.
This script provides different ways to run tests with various configurations.
"""
import subprocess
import sys
import os


def run_command(command, description):
    """Run a command and print its description."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {command}")
    print(f"{'='*60}")
    
    result = subprocess.run(command, shell=True, capture_output=False)
    return result.returncode == 0


def main():
    """Main test runner function."""
    print("React FastAPI V12 Test Runner")
    print("Choose a test option:")
    print("1. Run all tests")
    print("2. Run unit tests only")
    print("3. Run integration tests only")
    print("4. Run API tests only")
    print("5. Run with coverage")
    print("6. Run hello world test")
    print("7. Run verbose tests")
    print("8. Run specific test file")
    print("0. Exit")
    
    choice = input("\nEnter your choice (0-8): ").strip()
    
    if choice == "1":
        return run_command("pytest tests/", "All tests")
    
    elif choice == "2":
        return run_command("pytest tests/ -m 'not integration'", "Unit tests only")
    
    elif choice == "3":
        return run_command("pytest tests/ -m integration", "Integration tests only")
    
    elif choice == "4":
        return run_command("pytest tests/test_api/", "API tests only")
    
    elif choice == "5":
        return run_command("pytest --cov=app --cov-report=html --cov-report=term tests/", "Tests with coverage")
    
    elif choice == "6":
        return run_command("pytest tests/test_hello_world.py -v", "Hello world test")
    
    elif choice == "7":
        return run_command("pytest tests/ -v", "Verbose tests")
    
    elif choice == "8":
        test_file = input("Enter test file path (e.g., tests/test_api/test_main.py): ").strip()
        if test_file:
            return run_command(f"pytest {test_file} -v", f"Specific test file: {test_file}")
        else:
            print("No test file specified.")
            return False
    
    elif choice == "0":
        print("Exiting...")
        return True
    
    else:
        print("Invalid choice. Please try again.")
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
