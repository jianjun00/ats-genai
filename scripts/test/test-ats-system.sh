#!/bin/bash
set -euo pipefail

# ATS System Testing Script
# Comprehensive testing suite for the deployed ATS 3-service system

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="ats-dev"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_RESULTS_DIR="${SCRIPT_DIR}/results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Test counters
TESTS_TOTAL=0
TESTS_PASSED=0
TESTS_FAILED=0

# Service endpoints (assuming port-forwarding is set up)
MINUTE_SERVICE_URL="http://localhost:8081"
EOD_SERVICE_URL="http://localhost:8082"
ANALYTICS_SERVICE_URL="http://localhost:8080"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_test() {
    echo -e "${CYAN}[TEST]${NC} $1"
}

# Test utility functions
increment_test_counter() {
    ((TESTS_TOTAL++))
}

record_test_pass() {
    ((TESTS_PASSED++))
    log_success "✅ $1"
}

record_test_fail() {
    ((TESTS_FAILED++))
    log_error "❌ $1"
}

# HTTP test utility
test_http_endpoint() {
    local url="$1"
    local expected_status="${2:-200}"
    local description="$3"
    
    increment_test_counter
    log_test "Testing: $description"
    log_info "  URL: $url"
    
    local response_file="${TEST_RESULTS_DIR}/response_${TESTS_TOTAL}.json"
    local status_code
    
    if status_code=$(curl -s -o "$response_file" -w "%{http_code}" "$url" 2>/dev/null); then
        if [[ "$status_code" == "$expected_status" ]]; then
            record_test_pass "$description (HTTP $status_code)"
            return 0
        else
            record_test_fail "$description - Expected HTTP $expected_status, got $status_code"
            return 1
        fi
    else
        record_test_fail "$description - Connection failed"
        return 1
    fi
}

# JSON validation utility
validate_json_field() {
    local file="$1"
    local field="$2"
    local expected_value="$3"
    local description="$4"
    
    increment_test_counter
    log_test "Validating: $description"
    
    if [[ ! -f "$file" ]]; then
        record_test_fail "$description - Response file not found"
        return 1
    fi
    
    local actual_value
    if actual_value=$(jq -r "$field" "$file" 2>/dev/null); then
        if [[ "$actual_value" == "$expected_value" ]]; then
            record_test_pass "$description"
            return 0
        else
            record_test_fail "$description - Expected '$expected_value', got '$actual_value'"
            return 1
        fi
    else
        record_test_fail "$description - Failed to extract field '$field'"
        return 1
    fi
}

# Print banner
print_banner() {
    echo -e "${BLUE}"
    echo "=============================================="
    echo "     ATS System Comprehensive Test Suite"
    echo "=============================================="
    echo -e "${NC}"
    echo "Namespace: ${NAMESPACE}"
    echo "Test Results Dir: ${TEST_RESULTS_DIR}"
    echo "Timestamp: ${TIMESTAMP}"
    echo ""
}

# Setup test environment
setup_test_environment() {
    log_info "Setting up test environment..."
    
    # Create results directory
    mkdir -p "${TEST_RESULTS_DIR}"
    
    # Check if services are accessible
    log_info "Checking service accessibility..."
    
    local services=(
        "${MINUTE_SERVICE_URL}:Minute Service"
        "${EOD_SERVICE_URL}:EOD Service"
        "${ANALYTICS_SERVICE_URL}:Analytics Service"
    )
    
    for service_info in "${services[@]}"; do
        local url=$(echo "$service_info" | cut -d: -f1-2)
        local name=$(echo "$service_info" | cut -d: -f3)
        
        if ! curl -s --connect-timeout 5 "$url/health" > /dev/null; then
            log_error "$name is not accessible at $url"
            log_info "Make sure to run port forwarding:"
            log_info "  kubectl port-forward -n ${NAMESPACE} svc/$(echo "$name" | tr '[:upper:]' '[:lower:]' | sed 's/ /-/g') $(echo "$url" | sed 's/.*://')&"
            return 1
        fi
    done
    
    log_success "Test environment setup completed"
}

# Test 1: Basic Health Checks
test_health_checks() {
    echo ""
    echo "================================================"
    echo "Test Suite 1: Basic Health Checks"
    echo "================================================"
    
    # Test minute service health
    test_http_endpoint \
        "${MINUTE_SERVICE_URL}/health" \
        "200" \
        "Minute Service health check"
    
    # Test EOD service health
    test_http_endpoint \
        "${EOD_SERVICE_URL}/health" \
        "200" \
        "EOD Service health check"
    
    # Test analytics service health
    test_http_endpoint \
        "${ANALYTICS_SERVICE_URL}/health" \
        "200" \
        "Analytics Service health check"
}

# Test 2: Service Metrics
test_service_metrics() {
    echo ""
    echo "================================================"
    echo "Test Suite 2: Service Metrics"
    echo "================================================"
    
    # Test minute service metrics
    test_http_endpoint \
        "${MINUTE_SERVICE_URL}/metrics" \
        "200" \
        "Minute Service metrics endpoint"
    
    # Test EOD service metrics
    test_http_endpoint \
        "${EOD_SERVICE_URL}/metrics" \
        "200" \
        "EOD Service metrics endpoint"
    
    # Test analytics service metrics
    test_http_endpoint \
        "${ANALYTICS_SERVICE_URL}/metrics" \
        "200" \
        "Analytics Service metrics endpoint"
}

# Test 3: Service Integration
test_service_integration() {
    echo ""
    echo "================================================"
    echo "Test Suite 3: Service Integration"
    echo "================================================"
    
    # Test analytics service can reach data services
    increment_test_counter
    log_test "Testing inter-service connectivity"
    
    local response_file="${TEST_RESULTS_DIR}/integration_test.json"
    if curl -s "${ANALYTICS_SERVICE_URL}/health" -o "$response_file"; then
        # Check if analytics service reports healthy data services
        if jq -e '.services[] | select(.service_name == "minute-service" or .service_name == "eod-service") | select(.status == "healthy")' "$response_file" > /dev/null 2>&1; then
            record_test_pass "Analytics service can communicate with data services"
        else
            record_test_fail "Analytics service cannot communicate with data services"
        fi
    else
        record_test_fail "Failed to get analytics service health"
    fi
}

# Test 4: API Functionality
test_api_functionality() {
    echo ""
    echo "================================================"
    echo "Test Suite 4: API Functionality"
    echo "================================================"
    
    # Test minute service collection trigger
    increment_test_counter
    log_test "Testing minute service collection trigger"
    
    local response_file="${TEST_RESULTS_DIR}/minute_collect.json"
    if curl -s -X POST "${MINUTE_SERVICE_URL}/collect" \
        -H "Content-Type: application/json" \
        -d '{"symbols": "AAPL", "vendors": "polygon"}' \
        -o "$response_file"; then
        
        if jq -e '.message' "$response_file" > /dev/null 2>&1; then
            record_test_pass "Minute service collection trigger works"
        else
            record_test_fail "Minute service collection trigger failed"
        fi
    else
        record_test_fail "Minute service collection trigger connection failed"
    fi
    
    # Test EOD service collection trigger
    increment_test_counter
    log_test "Testing EOD service collection trigger"
    
    local response_file="${TEST_RESULTS_DIR}/eod_collect.json"
    if curl -s -X POST "${EOD_SERVICE_URL}/collect" \
        -H "Content-Type: application/json" \
        -d '{"symbols": "AAPL", "vendors": "polygon", "days_back": 1}' \
        -o "$response_file"; then
        
        if jq -e '.message' "$response_file" > /dev/null 2>&1; then
            record_test_pass "EOD service collection trigger works"
        else
            record_test_fail "EOD service collection trigger failed"
        fi
    else
        record_test_fail "EOD service collection trigger connection failed"
    fi
    
    # Test analytics data quality report
    test_http_endpoint \
        "${ANALYTICS_SERVICE_URL}/data-quality/report" \
        "200" \
        "Analytics service data quality report"
}

# Test 5: WebSocket Connectivity
test_websocket_connectivity() {
    echo ""
    echo "================================================"
    echo "Test Suite 5: WebSocket Connectivity"
    echo "================================================"
    
    increment_test_counter
    log_test "Testing WebSocket connection"
    
    # Use a simple WebSocket test client
    if command -v websocat &> /dev/null; then
        local ws_url="ws://localhost:8080/ws"
        local response
        
        # Send ping and expect pong within 5 seconds
        if response=$(echo '{"type":"ping"}' | timeout 5 websocat "$ws_url" 2>/dev/null); then
            if [[ "$response" == *"pong"* ]]; then
                record_test_pass "WebSocket connectivity works"
            else
                record_test_fail "WebSocket ping/pong failed: $response"
            fi
        else
            record_test_fail "WebSocket connection failed or timed out"
        fi
    else
        log_warning "websocat not available, skipping WebSocket test"
        log_info "Install with: cargo install websocat"
    fi
}

# Test 6: Data Validation
test_data_validation() {
    echo ""
    echo "================================================"
    echo "Test Suite 6: Data Validation"
    echo "================================================"
    
    # Test health response structure
    increment_test_counter
    log_test "Validating analytics health response structure"
    
    local response_file="${TEST_RESULTS_DIR}/analytics_health.json"
    if curl -s "${ANALYTICS_SERVICE_URL}/health" -o "$response_file"; then
        local required_fields=(
            ".status"
            ".data_quality_score"
            ".system_metrics"
            ".timestamp"
        )
        
        local valid_fields=0
        for field in "${required_fields[@]}"; do
            if jq -e "$field" "$response_file" > /dev/null 2>&1; then
                ((valid_fields++))
            fi
        done
        
        if [[ $valid_fields -eq ${#required_fields[@]} ]]; then
            record_test_pass "Analytics health response has all required fields"
        else
            record_test_fail "Analytics health response missing fields ($valid_fields/${#required_fields[@]} found)"
        fi
    else
        record_test_fail "Failed to get analytics health response"
    fi
}

# Test 7: Performance Tests
test_performance() {
    echo ""
    echo "================================================"
    echo "Test Suite 7: Performance Tests"
    echo "================================================"
    
    # Test response time
    increment_test_counter
    log_test "Testing analytics service response time"
    
    local start_time end_time response_time
    start_time=$(date +%s%N)
    
    if curl -s "${ANALYTICS_SERVICE_URL}/health" > /dev/null; then
        end_time=$(date +%s%N)
        response_time=$(( (end_time - start_time) / 1000000 ))  # Convert to milliseconds
        
        if [[ $response_time -lt 1000 ]]; then  # Less than 1 second
            record_test_pass "Analytics service responds quickly (${response_time}ms)"
        else
            record_test_fail "Analytics service too slow (${response_time}ms)"
        fi
    else
        record_test_fail "Performance test connection failed"
    fi
    
    # Test concurrent requests
    increment_test_counter
    log_test "Testing concurrent request handling"
    
    local concurrent_requests=5
    local success_count=0
    
    for i in $(seq 1 $concurrent_requests); do
        if curl -s "${ANALYTICS_SERVICE_URL}/health" > /dev/null &; then
            ((success_count++))
        fi
    done
    
    wait  # Wait for all background requests to complete
    
    if [[ $success_count -eq $concurrent_requests ]]; then
        record_test_pass "Handles $concurrent_requests concurrent requests"
    else
        record_test_fail "Only handled $success_count/$concurrent_requests concurrent requests"
    fi
}

# Test 8: Error Handling
test_error_handling() {
    echo ""
    echo "================================================"
    echo "Test Suite 8: Error Handling"
    echo "================================================"
    
    # Test invalid endpoints
    test_http_endpoint \
        "${ANALYTICS_SERVICE_URL}/nonexistent" \
        "404" \
        "Proper 404 response for invalid endpoints"
    
    # Test invalid request methods
    increment_test_counter
    log_test "Testing invalid HTTP methods"
    
    local status_code
    if status_code=$(curl -s -X DELETE "${ANALYTICS_SERVICE_URL}/health" -w "%{http_code}" -o /dev/null 2>/dev/null); then
        if [[ "$status_code" == "405" ]]; then
            record_test_pass "Proper 405 response for invalid methods"
        else
            record_test_fail "Expected 405 for invalid method, got $status_code"
        fi
    else
        record_test_fail "Failed to test invalid HTTP method"
    fi
}

# Test 9: Kubernetes Integration
test_kubernetes_integration() {
    echo ""
    echo "================================================"
    echo "Test Suite 9: Kubernetes Integration"
    echo "================================================"
    
    # Test pod status
    increment_test_counter
    log_test "Checking pod status in Kubernetes"
    
    local pod_count
    if pod_count=$(kubectl get pods -n "${NAMESPACE}" --field-selector=status.phase=Running -o name | wc -l); then
        if [[ $pod_count -gt 0 ]]; then
            record_test_pass "Kubernetes pods are running ($pod_count pods)"
        else
            record_test_fail "No running pods found in namespace $NAMESPACE"
        fi
    else
        record_test_fail "Failed to check Kubernetes pod status"
    fi
    
    # Test service endpoints
    increment_test_counter
    log_test "Checking Kubernetes service endpoints"
    
    local services=("ats-minute-service" "ats-eod-service" "ats-analytics-service")
    local ready_services=0
    
    for service in "${services[@]}"; do
        if kubectl get endpoints "$service" -n "${NAMESPACE}" -o jsonpath='{.subsets[*].addresses[*].ip}' | grep -q .; then
            ((ready_services++))
        fi
    done
    
    if [[ $ready_services -eq ${#services[@]} ]]; then
        record_test_pass "All Kubernetes services have endpoints ($ready_services/${#services[@]})"
    else
        record_test_fail "Only $ready_services/${#services[@]} services have endpoints"
    fi
}

# Test 10: Security Tests
test_security() {
    echo ""
    echo "================================================"
    echo "Test Suite 10: Security Tests"
    echo "================================================"
    
    # Test CORS headers
    increment_test_counter
    log_test "Checking CORS headers"
    
    local cors_header
    if cors_header=$(curl -s -I "${ANALYTICS_SERVICE_URL}/health" | grep -i "access-control-allow-origin" || true); then
        if [[ -n "$cors_header" ]]; then
            record_test_pass "CORS headers present"
        else
            record_test_fail "CORS headers missing"
        fi
    else
        record_test_fail "Failed to check CORS headers"
    fi
    
    # Test for sensitive information exposure
    increment_test_counter
    log_test "Checking for sensitive information exposure"
    
    local response_file="${TEST_RESULTS_DIR}/security_check.json"
    if curl -s "${ANALYTICS_SERVICE_URL}/health" -o "$response_file"; then
        local sensitive_patterns=("password" "secret" "key" "token")
        local found_sensitive=false
        
        for pattern in "${sensitive_patterns[@]}"; do
            if grep -qi "$pattern" "$response_file"; then
                found_sensitive=true
                break
            fi
        done
        
        if [[ "$found_sensitive" == false ]]; then
            record_test_pass "No sensitive information exposed in API responses"
        else
            record_test_fail "Potential sensitive information found in API response"
        fi
    else
        record_test_fail "Failed to check for sensitive information"
    fi
}

# Generate test report
generate_test_report() {
    local report_file="${TEST_RESULTS_DIR}/test_report_${TIMESTAMP}.md"
    
    log_info "Generating test report: $report_file"
    
    cat > "$report_file" <<EOF
# ATS System Test Report

**Test Run Date**: $(date)  
**Namespace**: $NAMESPACE  
**Total Tests**: $TESTS_TOTAL  
**Tests Passed**: $TESTS_PASSED  
**Tests Failed**: $TESTS_FAILED  
**Success Rate**: $(( (TESTS_PASSED * 100) / TESTS_TOTAL ))%

## Test Results Summary

| Test Suite | Status |
|------------|--------|
| Health Checks | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| Service Metrics | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| Service Integration | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| API Functionality | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| WebSocket Connectivity | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| Data Validation | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| Performance Tests | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| Error Handling | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| Kubernetes Integration | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |
| Security Tests | $(if [[ $TESTS_FAILED -eq 0 ]]; then echo "✅ PASSED"; else echo "❌ FAILED"; fi) |

## System Information

- **Minute Service**: $MINUTE_SERVICE_URL
- **EOD Service**: $EOD_SERVICE_URL  
- **Analytics Service**: $ANALYTICS_SERVICE_URL
- **Kubernetes Namespace**: $NAMESPACE

## Recommendations

$(if [[ $TESTS_FAILED -gt 0 ]]; then
    echo "- ⚠️ Review failed tests and fix underlying issues"
    echo "- 🔍 Check service logs for error details"
    echo "- 📊 Monitor system performance after fixes"
else
    echo "- ✅ All tests passed - system is ready for production"
    echo "- 📈 Consider setting up continuous monitoring"
    echo "- 🔄 Schedule regular test runs"
fi)

## Test Artifacts

Test results and response files are stored in: \`${TEST_RESULTS_DIR}\`

EOF

    log_success "Test report generated: $report_file"
}

# Print final results
print_final_results() {
    echo ""
    echo "=============================================="
    echo "           Final Test Results"
    echo "=============================================="
    echo -e "Total Tests:  ${BLUE}$TESTS_TOTAL${NC}"
    echo -e "Passed:       ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed:       ${RED}$TESTS_FAILED${NC}"
    
    if [[ $TESTS_FAILED -eq 0 ]]; then
        echo -e "Result:       ${GREEN}✅ ALL TESTS PASSED${NC}"
        echo ""
        echo "🎉 The ATS system is working correctly!"
    else
        echo -e "Result:       ${RED}❌ SOME TESTS FAILED${NC}"
        echo ""
        echo "⚠️ Please review failed tests and fix issues before production deployment."
        echo ""
        echo "Common troubleshooting steps:"
        echo "1. Check pod logs: kubectl logs -l app=ats-analytics-service -n $NAMESPACE"
        echo "2. Verify secrets: kubectl get secrets -n $NAMESPACE"
        echo "3. Check service endpoints: kubectl get endpoints -n $NAMESPACE"
    fi
    
    echo ""
    echo "Test results saved to: ${TEST_RESULTS_DIR}"
}

# Main test execution
main() {
    print_banner
    setup_test_environment
    
    # Run all test suites
    test_health_checks
    test_service_metrics
    test_service_integration
    test_api_functionality
    test_websocket_connectivity
    test_data_validation
    test_performance
    test_error_handling
    test_kubernetes_integration
    test_security
    
    generate_test_report
    print_final_results
    
    # Exit with error code if tests failed
    if [[ $TESTS_FAILED -gt 0 ]]; then
        exit 1
    fi
}

# Script options
case "${1:-run}" in
    "run")
        main
        ;;
    "quick")
        print_banner
        setup_test_environment
        test_health_checks
        test_service_metrics
        print_final_results
        ;;
    "report")
        if [[ -f "${TEST_RESULTS_DIR}/test_report_${2:-latest}.md" ]]; then
            cat "${TEST_RESULTS_DIR}/test_report_${2:-latest}.md"
        else
            log_error "Test report not found"
            exit 1
        fi
        ;;
    *)
        echo "Usage: $0 [run|quick|report [timestamp]]"
        echo "  run: Execute full test suite (default)"
        echo "  quick: Execute only health checks and metrics tests"
        echo "  report: Display test report (optionally specify timestamp)"
        exit 1
        ;;
esac