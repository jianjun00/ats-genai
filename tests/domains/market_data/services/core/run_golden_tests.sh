#!/bin/bash
"""
UnifiedMarketDataManager Golden File Test Runner

ULTRA-COMPREHENSIVE test execution with:
1. Test data setup from real FirstRate minute bars
2. Golden file generation and comparison  
3. Automated test environment management
4. Clear success/failure reporting

USAGE:
  ./run_golden_tests.sh                    # Run tests with existing golden files
  ./run_golden_tests.sh --setup            # Setup test data first
  ./run_golden_tests.sh --update-golden    # Update golden files
  ./run_golden_tests.sh --clean            # Clean test artifacts
"""

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DATA_DIR="$SCRIPT_DIR/test_data"
GOLDEN_FILES_DIR="$TEST_DATA_DIR/golden_files"

# Functions
print_header() {
    echo -e "${BLUE}==========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}==========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

setup_test_data() {
    print_header "Setting up test data"
    
    echo "📊 Extracting real FirstRate minute bar data for testing..."
    cd "$SCRIPT_DIR"
    
    # Set Python path and run setup
    export PYTHONPATH="$SCRIPT_DIR/../../../../../src"
    python test_data/setup_test_data.py
    
    if [[ $? -eq 0 ]]; then
        print_success "Test data setup completed"
    else
        print_error "Test data setup failed"
        exit 1
    fi
}

check_test_data() {
    echo "🔍 Checking test data availability..."
    
    if [[ ! -d "$TEST_DATA_DIR/firstrate" ]]; then
        print_warning "Test data not found. Setting up..."
        setup_test_data
    else
        print_success "Test data found"
    fi
}

run_tests() {
    local update_golden=$1
    
    print_header "Running UnifiedMarketDataManager Golden Tests"
    
    cd "$SCRIPT_DIR"
    
    # Set Python path
    export PYTHONPATH="$SCRIPT_DIR/../../../../../src"
    
    # Build pytest command
    local pytest_cmd="python -m pytest test_unified_market_data_manager_golden.py -v"
    
    if [[ "$update_golden" == "true" ]]; then
        pytest_cmd="$pytest_cmd --update-golden"
        print_warning "Running tests with golden file updates enabled"
    fi
    
    echo "🧪 Executing: $pytest_cmd"
    echo ""
    
    # Run tests
    if eval $pytest_cmd; then
        print_success "All golden file tests passed!"
        return 0
    else
        print_error "Some tests failed"
        return 1
    fi
}

show_golden_files() {
    print_header "Golden Files Status"
    
    if [[ -d "$GOLDEN_FILES_DIR" ]]; then
        echo "📋 Golden files by test suite:"
        
        # Show organized structure
        for test_dir in "$GOLDEN_FILES_DIR"/*/; do
            if [[ -d "$test_dir" ]]; then
                local test_suite_name=$(basename "$test_dir")
                echo "   📁 $test_suite_name/"
                
                for file in "$test_dir"/*.json; do
                    if [[ -f "$file" ]]; then
                        local size=$(wc -c < "$file" | tr -d ' ')
                        local modified=$(stat -c %y "$file" 2>/dev/null || stat -f %Sm "$file" 2>/dev/null || echo "unknown")
                        echo "      📄 $(basename "$file") (${size} bytes, modified: $modified)"
                    fi
                done
            fi
        done
        
        # Also show any loose files (legacy)
        for file in "$GOLDEN_FILES_DIR"/*.json; do
            if [[ -f "$file" ]]; then
                local size=$(wc -c < "$file" | tr -d ' ')
                echo "   📄 $(basename "$file") (${size} bytes, legacy location)"
            fi
        done
    else
        print_warning "No golden files directory found"
    fi
}

clean_artifacts() {
    print_header "Cleaning test artifacts"
    
    # Remove golden files
    if [[ -d "$GOLDEN_FILES_DIR" ]]; then
        echo "🗑️  Removing golden files..."
        rm -rf "$GOLDEN_FILES_DIR"
        print_success "Golden files removed"
    fi
    
    # Remove test data  
    if [[ -d "$TEST_DATA_DIR/firstrate" ]]; then
        echo "🗑️  Removing test data..."
        rm -rf "$TEST_DATA_DIR/firstrate"
        print_success "Test data removed"
    fi
    
    # Remove Python cache
    find "$SCRIPT_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find "$SCRIPT_DIR" -name "*.pyc" -delete 2>/dev/null || true
    
    print_success "Cleanup completed"
}

show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "OPTIONS:"
    echo "  --setup           Setup test data from real FirstRate minute bars"
    echo "  --update-golden   Update golden files with current test results"
    echo "  --clean           Clean all test artifacts and golden files"
    echo "  --status          Show golden files status"
    echo "  --help            Show this help message"
    echo ""
    echo "EXAMPLES:"
    echo "  $0                    # Run tests with existing golden files"
    echo "  $0 --setup           # Setup test data first, then run tests"
    echo "  $0 --update-golden   # Update golden files with current results"
    echo ""
}

# Main execution
main() {
    local setup_data=false
    local update_golden=false
    local clean_only=false
    local show_status=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --setup)
                setup_data=true
                shift
                ;;
            --update-golden)
                update_golden=true
                shift
                ;;
            --clean)
                clean_only=true
                shift
                ;;
            --status)
                show_status=true
                shift
                ;;
            --help)
                show_usage
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Execute based on arguments
    if [[ "$clean_only" == "true" ]]; then
        clean_artifacts
        exit 0
    fi
    
    if [[ "$show_status" == "true" ]]; then
        show_golden_files
        exit 0
    fi
    
    print_header "UnifiedMarketDataManager Golden File Tests"
    echo "📅 $(date)"
    echo "📁 Test directory: $SCRIPT_DIR"
    echo ""
    
    # Setup data if requested
    if [[ "$setup_data" == "true" ]]; then
        setup_test_data
    else
        check_test_data
    fi
    
    # Show current golden files status
    show_golden_files
    
    # Run tests
    if run_tests "$update_golden"; then
        echo ""
        print_success "🎉 All golden file tests completed successfully!"
        
        if [[ "$update_golden" == "true" ]]; then
            echo ""
            show_golden_files
        fi
        
        exit 0
    else
        echo ""
        print_error "💥 Some tests failed. Check output above for details."
        exit 1
    fi
}

# Run main function with all arguments
main "$@"