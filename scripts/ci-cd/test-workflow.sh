#!/bin/bash
set -euo pipefail

# GitHub Actions Workflow Testing Script
echo "🧪 Testing GitHub Actions Workflow"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Test service discovery
test_service_discovery() {
    print_status "Testing service discovery..."
    
    if [[ -d "services" ]]; then
        SERVICES=$(find services/ -type d -maxdepth 1 -mindepth 1 -exec basename {} \; | sort)
        if [[ -n "$SERVICES" ]]; then
            print_success "✅ Found services:"
            echo "$SERVICES" | sed 's/^/  - /'
            
            # Convert to JSON array format for workflow
            SERVICES_JSON=$(echo "$SERVICES" | jq -R . | jq -s . | jq -c .)
            print_status "JSON format: $SERVICES_JSON"
        else
            print_warning "No services found in services/ directory"
        fi
    else
        print_error "services/ directory not found"
        return 1
    fi
}

# Test Dockerfile existence
test_dockerfiles() {
    print_status "Testing Dockerfile existence..."
    
    local missing_dockerfiles=()
    
    if [[ -d "services" ]]; then
        while IFS= read -r -d '' service_dir; do
            service=$(basename "$service_dir")
            dockerfile="$service_dir/Dockerfile"
            
            if [[ -f "$dockerfile" ]]; then
                print_success "✅ $service: Dockerfile found"
            else
                print_error "❌ $service: Dockerfile missing"
                missing_dockerfiles+=("$service")
            fi
        done < <(find services/ -type d -maxdepth 1 -mindepth 1 -print0)
        
        if [[ ${#missing_dockerfiles[@]} -eq 0 ]]; then
            print_success "All services have Dockerfiles"
        else
            print_error "Missing Dockerfiles for: ${missing_dockerfiles[*]}"
            return 1
        fi
    else
        print_error "services/ directory not found"
        return 1
    fi
}

# Test Kubernetes manifests
test_k8s_manifests() {
    print_status "Testing Kubernetes manifests..."
    
    local missing_manifests=()
    
    if [[ -d "k8s" ]]; then
        # Check for deployment files
        if [[ -d "services" ]]; then
            while IFS= read -r -d '' service_dir; do
                service=$(basename "$service_dir")
                manifest="k8s/$service/deployment.yaml"
                
                if [[ -f "$manifest" ]]; then
                    print_success "✅ $service: deployment.yaml found"
                    
                    # Validate YAML syntax
                    if command -v yamllint > /dev/null 2>&1; then
                        if yamllint "$manifest" >/dev/null 2>&1; then
                            print_success "  └─ YAML syntax valid"
                        else
                            print_warning "  └─ YAML syntax issues found"
                        fi
                    fi
                else
                    print_warning "⚠️ $service: deployment.yaml missing"
                    missing_manifests+=("$service")
                fi
            done < <(find services/ -type d -maxdepth 1 -mindepth 1 -print0)
        fi
        
        # Check for other important manifests
        important_manifests=(
            "k8s/permanent-access/nodeport-services.yaml"
            "k8s/permanent-access/ingress-controllers.yaml"
            "k8s/permanent-access/loadbalancer-services.yaml"
        )
        
        for manifest in "${important_manifests[@]}"; do
            if [[ -f "$manifest" ]]; then
                print_success "✅ Found: $manifest"
            else
                print_status "ℹ️ Optional: $manifest (not found)"
            fi
        done
        
    else
        print_error "k8s/ directory not found"
        return 1
    fi
}

# Test Python environment
test_python_environment() {
    print_status "Testing Python environment..."
    
    # Check Python version
    if command -v python3 > /dev/null 2>&1; then
        PYTHON_VERSION=$(python3 --version)
        print_success "✅ $PYTHON_VERSION"
    else
        print_error "❌ Python 3 not found"
        return 1
    fi
    
    # Check requirements.txt
    if [[ -f "requirements.txt" ]]; then
        print_success "✅ requirements.txt found"
        
        # Test installation (in a virtual environment)
        if command -v python3 > /dev/null 2>&1; then
            print_status "Testing dependency installation..."
            
            # Create temporary virtual environment
            TEMP_VENV=$(mktemp -d)
            python3 -m venv "$TEMP_VENV" >/dev/null 2>&1
            
            # Test installation
            if "$TEMP_VENV/bin/pip" install -r requirements.txt >/dev/null 2>&1; then
                print_success "✅ Dependencies install successfully"
            else
                print_warning "⚠️ Some dependencies failed to install"
            fi
            
            # Cleanup
            rm -rf "$TEMP_VENV"
        fi
    else
        print_error "❌ requirements.txt not found"
        return 1
    fi
    
    # Check for pyproject.toml
    if [[ -f "pyproject.toml" ]]; then
        print_success "✅ pyproject.toml found"
    else
        print_status "ℹ️ pyproject.toml not found (optional)"
    fi
}

# Test pytest configuration
test_pytest_config() {
    print_status "Testing pytest configuration..."
    
    if [[ -f "pytest.ini" ]]; then
        print_success "✅ pytest.ini found"
        
        # Validate pytest configuration
        if command -v python3 > /dev/null 2>&1; then
            if python3 -c "import configparser; c=configparser.ConfigParser(); c.read('pytest.ini')" 2>/dev/null; then
                print_success "✅ pytest.ini syntax valid"
            else
                print_warning "⚠️ pytest.ini syntax issues"
            fi
        fi
    else
        print_status "ℹ️ pytest.ini not found (using defaults)"
    fi
    
    # Check for tests directory
    if [[ -d "tests" ]]; then
        TEST_COUNT=$(find tests/ -name "*.py" -type f | wc -l)
        print_success "✅ Found $TEST_COUNT test files"
        
        # Check for integration tests
        if [[ -d "tests/integration" ]]; then
            INTEGRATION_COUNT=$(find tests/integration/ -name "*.py" -type f | wc -l)
            print_success "✅ Found $INTEGRATION_COUNT integration test files"
        else
            print_status "ℹ️ No integration tests directory found"
        fi
        
        # Check for conftest.py
        if [[ -f "tests/conftest.py" ]]; then
            print_success "✅ tests/conftest.py found"
        else
            print_status "ℹ️ tests/conftest.py not found (optional)"
        fi
    else
        print_error "❌ tests/ directory not found"
        return 1
    fi
}

# Test GitHub Actions workflow syntax
test_workflow_syntax() {
    print_status "Testing GitHub Actions workflow syntax..."
    
    local workflows=()
    
    if [[ -d ".github/workflows" ]]; then
        # Find all workflow files
        while IFS= read -r -d '' workflow; do
            workflows+=("$workflow")
        done < <(find .github/workflows/ -name "*.yaml" -o -name "*.yml" -print0)
        
        if [[ ${#workflows[@]} -eq 0 ]]; then
            print_error "❌ No workflow files found"
            return 1
        fi
        
        for workflow in "${workflows[@]}"; do
            workflow_name=$(basename "$workflow")
            print_status "Checking $workflow_name..."
            
            # Basic YAML syntax check
            if command -v yamllint > /dev/null 2>&1; then
                if yamllint "$workflow" >/dev/null 2>&1; then
                    print_success "  ✅ YAML syntax valid"
                else
                    print_error "  ❌ YAML syntax errors found"
                    yamllint "$workflow" | head -5
                fi
            elif command -v python3 > /dev/null 2>&1; then
                if python3 -c "import yaml; yaml.safe_load(open('$workflow'))" 2>/dev/null; then
                    print_success "  ✅ YAML syntax valid"
                else
                    print_error "  ❌ YAML syntax invalid"
                fi
            else
                print_warning "  ⚠️ No YAML validator available"
            fi
            
            # Check for required sections
            if grep -q "^name:" "$workflow"; then
                print_success "  ✅ Has name"
            else
                print_warning "  ⚠️ Missing name"
            fi
            
            if grep -q "^on:" "$workflow"; then
                print_success "  ✅ Has triggers"
            else
                print_error "  ❌ Missing triggers"
            fi
            
            if grep -q "^jobs:" "$workflow"; then
                print_success "  ✅ Has jobs"
            else
                print_error "  ❌ Missing jobs"
            fi
        done
    else
        print_error "❌ .github/workflows directory not found"
        return 1
    fi
}

# Test permanent access integration
test_permanent_access() {
    print_status "Testing permanent access integration..."
    
    local scripts=(
        "scripts/setup/start-permanent-access.sh"
        "scripts/setup/stop-permanent-access.sh"
        "scripts/setup/start-analytics-external.sh"
        "scripts/setup/setup-permanent-access.sh"
    )
    
    local found_scripts=0
    
    for script in "${scripts[@]}"; do
        if [[ -f "$script" && -x "$script" ]]; then
            print_success "✅ Found: $script (executable)"
            ((found_scripts++))
        elif [[ -f "$script" ]]; then
            print_warning "⚠️ Found: $script (not executable)"
            ((found_scripts++))
        else
            print_status "ℹ️ Not found: $script"
        fi
    done
    
    if [[ $found_scripts -gt 0 ]]; then
        print_success "✅ Permanent access system available ($found_scripts/4 scripts)"
    else
        print_warning "⚠️ No permanent access scripts found"
    fi
    
    # Check for documentation
    if [[ -f "docs/permanent-access-guide.md" ]]; then
        print_success "✅ Permanent access documentation found"
    else
        print_status "ℹ️ No permanent access documentation found"
    fi
}

# Run a comprehensive test
run_comprehensive_test() {
    print_status "Running comprehensive workflow test..."
    
    local test_results=()
    
    # Run all tests
    if test_service_discovery; then
        test_results+=("service_discovery:✅")
    else
        test_results+=("service_discovery:❌")
    fi
    
    echo ""
    if test_dockerfiles; then
        test_results+=("dockerfiles:✅")
    else
        test_results+=("dockerfiles:❌")
    fi
    
    echo ""
    if test_k8s_manifests; then
        test_results+=("k8s_manifests:✅")
    else
        test_results+=("k8s_manifests:❌")
    fi
    
    echo ""
    if test_python_environment; then
        test_results+=("python_env:✅")
    else
        test_results+=("python_env:❌")
    fi
    
    echo ""
    if test_pytest_config; then
        test_results+=("pytest_config:✅")
    else
        test_results+=("pytest_config:❌")
    fi
    
    echo ""
    if test_workflow_syntax; then
        test_results+=("workflow_syntax:✅")
    else
        test_results+=("workflow_syntax:❌")
    fi
    
    echo ""
    if test_permanent_access; then
        test_results+=("permanent_access:✅")
    else
        test_results+=("permanent_access:❌")
    fi
    
    # Generate summary
    echo ""
    echo "==================="
    print_status "📊 Test Summary"
    echo "==================="
    
    local passed=0
    local failed=0
    
    for result in "${test_results[@]}"; do
        test_name=${result%:*}
        status=${result#*:}
        
        if [[ "$status" == "✅" ]]; then
            echo -e "  ${GREEN}✅${NC} ${test_name//_/ }"
            ((passed++))
        else
            echo -e "  ${RED}❌${NC} ${test_name//_/ }"
            ((failed++))
        fi
    done
    
    echo ""
    if [[ $failed -eq 0 ]]; then
        print_success "🎉 All tests passed! ($passed/$((passed + failed)))"
        echo ""
        print_status "✅ Your workflow is ready for GitHub Actions!"
    else
        print_warning "⚠️ Some tests failed: $failed/$((passed + failed))"
        echo ""
        print_status "❗ Please fix the failing tests before deploying to GitHub Actions"
    fi
    
    # Generate recommendations
    generate_recommendations
}

# Generate recommendations based on test results
generate_recommendations() {
    print_status "📋 Recommendations:"
    echo ""
    
    print_status "🔧 To fix common issues:"
    echo "  1. Install missing dependencies:"
    echo "     pip install pytest pytest-cov black flake8 mypy"
    echo ""
    echo "  2. Make scripts executable:"
    echo "     chmod +x scripts/setup/*.sh"
    echo ""
    echo "  3. Validate YAML files:"
    echo "     pip install yamllint && yamllint .github/workflows/*.yaml"
    echo ""
    
    print_status "🚀 To deploy the improved workflow:"
    echo "  1. Run the migration script:"
    echo "     ./scripts/ci-cd/migrate-workflow.sh"
    echo ""
    echo "  2. Configure GitHub secrets:"
    echo "     - SLACK_WEBHOOK_URL (required)"
    echo "     - GITOPS_TOKEN (optional)"
    echo "     - ARGOCD_TOKEN (optional)"
    echo ""
    echo "  3. Test with a small change:"
    echo "     git checkout -b test/workflow"
    echo "     echo '# Test' >> README.md"
    echo "     git add README.md && git commit -m 'test: workflow'"
    echo "     git push origin test/workflow"
    echo ""
}

# Main execution
main() {
    local command="${1:-test}"
    
    case "$command" in
        "test"|"all")
            run_comprehensive_test
            ;;
        "services")
            test_service_discovery
            ;;
        "docker")
            test_dockerfiles
            ;;
        "k8s")
            test_k8s_manifests
            ;;
        "python")
            test_python_environment
            ;;
        "pytest")
            test_pytest_config
            ;;
        "workflow")
            test_workflow_syntax
            ;;
        "access")
            test_permanent_access
            ;;
        "help")
            echo "Usage: $0 [command]"
            echo ""
            echo "Commands:"
            echo "  test, all    - Run all tests (default)"
            echo "  services     - Test service discovery"
            echo "  docker       - Test Dockerfile existence"
            echo "  k8s          - Test Kubernetes manifests"
            echo "  python       - Test Python environment"
            echo "  pytest       - Test pytest configuration"
            echo "  workflow     - Test workflow syntax"
            echo "  access       - Test permanent access integration"
            echo "  help         - Show this help"
            ;;
        *)
            print_error "Unknown command: $command"
            echo "Run '$0 help' for usage information"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"