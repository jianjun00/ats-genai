#!/bin/bash
set -euo pipefail

# ATS System Setup Script
# This script prepares the complete ATS deployment environment

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

log_header() {
    echo -e "${PURPLE}[HEADER]${NC} $1"
}

# Print banner
print_banner() {
    echo -e "${CYAN}"
    cat << "EOF"
    ╔══════════════════════════════════════════════════════════════════════════════╗
    ║                        ATS System Deployment Setup                          ║
    ║                                                                              ║
    ║   🚀 Complete 3-Service Architecture with CI/CD Pipeline                   ║
    ║   📊 Real-time Data Collection & Portfolio Analytics                       ║
    ║   🔄 Automated Deployment with Argo CD & GitHub Actions                    ║
    ║   🛡️  Comprehensive Validation & Rollback Strategy                         ║
    ╚══════════════════════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
}

# Make scripts executable
setup_script_permissions() {
    log_header "Setting up script permissions..."
    
    local scripts=(
        "scripts/deploy/deploy-ats-system.sh"
        "scripts/test/test-ats-system.sh"
        "setup-ats-deployment.sh"
    )
    
    for script in "${scripts[@]}"; do
        if [[ -f "${SCRIPT_DIR}/${script}" ]]; then
            chmod +x "${SCRIPT_DIR}/${script}"
            log_success "Made executable: ${script}"
        else
            log_warning "Script not found: ${script}"
        fi
    done
}

# Validate project structure
validate_project_structure() {
    log_header "Validating project structure..."
    
    local required_dirs=(
        "services/minute-service"
        "services/eod-service"  
        "services/analytics-service"
        "k8s/minute-service"
        "k8s/eod-service"
        "k8s/analytics-service"
        "k8s/argocd"
        "k8s/validation"
        ".github/workflows"
        "scripts/deploy"
        "scripts/test"
        "docs"
    )
    
    local missing_dirs=()
    for dir in "${required_dirs[@]}"; do
        if [[ ! -d "${SCRIPT_DIR}/${dir}" ]]; then
            missing_dirs+=("$dir")
        fi
    done
    
    if [[ ${#missing_dirs[@]} -eq 0 ]]; then
        log_success "All required directories exist"
    else
        log_error "Missing directories:"
        for dir in "${missing_dirs[@]}"; do
            echo "  - $dir"
        done
        return 1
    fi
    
    local required_files=(
        "services/minute-service/minute_price_service.py"
        "services/minute-service/Dockerfile"
        "services/eod-service/enhanced_eod_service.py"
        "services/eod-service/Dockerfile"
        "services/analytics-service/unified_analytics_app.py"
        "services/analytics-service/Dockerfile"
        "k8s/argocd/argo-applications.yaml"
        "k8s/validation/deployment-validation.yaml"
        ".github/workflows/ats-ci-cd.yaml"
        "docs/ats_cicd_deployment_guide.md"
    )
    
    local missing_files=()
    for file in "${required_files[@]}"; do
        if [[ ! -f "${SCRIPT_DIR}/${file}" ]]; then
            missing_files+=("$file")
        fi
    done
    
    if [[ ${#missing_files[@]} -eq 0 ]]; then
        log_success "All required files exist"
    else
        log_error "Missing files:"
        for file in "${missing_files[@]}"; do
            echo "  - $file"
        done
        return 1
    fi
}

# Check prerequisites
check_prerequisites() {
    log_header "Checking prerequisites..."
    
    local required_commands=(
        "kubectl:Kubernetes CLI"
        "docker:Docker container runtime"
        "git:Git version control"
        "curl:HTTP client"
        "jq:JSON processor"
    )
    
    local missing_commands=()
    for cmd_info in "${required_commands[@]}"; do
        local cmd=$(echo "$cmd_info" | cut -d: -f1)
        local desc=$(echo "$cmd_info" | cut -d: -f2)
        
        if ! command -v "$cmd" &> /dev/null; then
            missing_commands+=("$cmd ($desc)")
        else
            log_success "$cmd is available"
        fi
    done
    
    if [[ ${#missing_commands[@]} -gt 0 ]]; then
        log_error "Missing required commands:"
        for cmd in "${missing_commands[@]}"; do
            echo "  - $cmd"
        done
        return 1
    fi
    
    # Check optional commands
    local optional_commands=(
        "websocat:WebSocket testing (install: cargo install websocat)"
        "argocd:Argo CD CLI (install: brew install argocd)"
    )
    
    for cmd_info in "${optional_commands[@]}"; do
        local cmd=$(echo "$cmd_info" | cut -d: -f1)
        local install_info=$(echo "$cmd_info" | cut -d: -f2)
        
        if command -v "$cmd" &> /dev/null; then
            log_success "$cmd is available"
        else
            log_warning "$cmd not found - $install_info"
        fi
    done
}

# Display system architecture
show_system_architecture() {
    log_header "System Architecture Overview"
    
    cat << "EOF"

    ┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
    │   Minute Service    │    │    EOD Service      │    │  Analytics Service  │
    │     (Port 8081)     │    │    (Port 8082)      │    │    (Port 8080)      │
    │                     │    │                     │    │                     │
    │ • Real-time data    │    │ • Daily data        │    │ • Portfolio metrics │
    │ • 1-min intervals   │    │ • Enhanced features │    │ • Real-time monitor │
    │ • Multi-vendor      │    │ • Quality scoring   │    │ • WebSocket support │
    │ • Rate limiting     │    │ • Gap detection     │    │ • System health     │
    └─────────────────────┘    └─────────────────────┘    └─────────────────────┘
              │                          │                          │
              └──────────────────────────┼──────────────────────────┘
                                         │
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                            Shared Infrastructure                            │
    │                                                                             │
    │  PostgreSQL/TimescaleDB  │  Redis Cache  │  Kubernetes  │  Monitoring     │
    │  • Price data storage    │  • Analytics  │  • Auto-scale│  • Prometheus   │
    │  • Time-series optimized │  • Session    │  • Health     │  • Grafana      │
    │  • Multi-vendor tables   │  • Metadata   │  • Rolling    │  • Alerting     │
    └─────────────────────────────────────────────────────────────────────────────┘

EOF
}

# Display deployment pipeline
show_deployment_pipeline() {
    log_header "CI/CD Pipeline Overview"
    
    cat << "EOF"

    GitHub Push (main) 
           │
           ▼
    ┌─────────────────┐
    │   CI Pipeline   │    • Code Quality (Black, Flake8, MyPy)
    │   (GitHub)      │    • Security Scan (Bandit, Safety)
    │                 │    • Unit & Integration Tests
    │                 │    • Container Security Scan
    └─────────┬───────┘    • SBOM Generation
              │
              ▼
    ┌─────────────────┐
    │  Build & Push   │    • Multi-arch Docker images
    │  (Docker)       │    • Vulnerability scanning
    │                 │    • Registry push (GHCR)
    └─────────┬───────┘
              │
              ▼
    ┌─────────────────┐
    │  Update Manifests│   • Automated K8s manifest updates
    │  (GitOps)       │    • Create deployment PR
    │                 │    • Validation checks
    └─────────┬───────┘
              │
              ▼
    ┌─────────────────┐
    │  Argo CD Sync   │    • Canary Deployment Strategy
    │  (Kubernetes)   │    • 10% → 25% → 50% → 100%
    │                 │    • Comprehensive Validation
    │                 │    • Automatic Rollback
    └─────────────────┘

EOF
}

# Display validation strategy
show_validation_strategy() {
    log_header "Comprehensive Validation Strategy"
    
    cat << "EOF"

    Pre-Deployment (CI)          Canary Deployment (CD)         Runtime Monitoring
    ┌─────────────────┐         ┌─────────────────┐             ┌─────────────────┐
    │ • Code Quality  │  ────▶  │ • Health Checks │  ────▶     │ • Live Metrics  │
    │ • Security Scan │         │ • Performance   │             │ • Data Quality  │
    │ • Unit Tests    │         │ • Integration   │             │ • User Experience│
    │ • Integration   │         │ • Load Testing  │             │ • System Health │
    └─────────────────┘         └─────────────────┘             └─────────────────┘
           │                            │                              │
           │                            ▼                              │
           │                   ┌─────────────────┐                    │
           └──────────────────▶│ Automated       │◀───────────────────┘
                               │ Rollback        │
                               │ • Failure > 5%  │
                               │ • Latency > 1s  │
                               │ • Memory > 90%  │
                               │ • Data Loss     │
                               └─────────────────┘

    Validation Stages:
    1. 🚀 Deploy 10% traffic (5 min validation)
    2. 📈 Scale to 25% (10 min validation)  
    3. 📈 Scale to 50% (15 min validation)
    4. ✅ Full promotion (100% traffic)

EOF
}

# Show next steps
show_next_steps() {
    log_header "Next Steps - Deployment Guide"
    
    echo ""
    echo "🚀 Ready to deploy! Follow these steps:"
    echo ""
    echo "1️⃣  Set up secrets and configuration:"
    echo "   • Update API keys in Kubernetes secrets"
    echo "   • Configure database connection"
    echo "   • Set up monitoring endpoints"
    echo ""
    echo "2️⃣  Deploy the system:"
    echo "   ${CYAN}./scripts/deploy/deploy-ats-system.sh${NC}"
    echo ""
    echo "3️⃣  Test the deployment:"
    echo "   ${CYAN}./scripts/test/test-ats-system.sh${NC}"
    echo ""
    echo "4️⃣  Set up CI/CD (optional):"
    echo "   • Install Argo CD in your cluster"
    echo "   • Configure GitHub repository secrets"
    echo "   • Apply Argo applications: kubectl apply -f k8s/argocd/"
    echo ""
    echo "5️⃣  Access the services:"
    echo "   • Port forward: kubectl port-forward -n ats-dev svc/ats-analytics-service 8080:8080"
    echo "   • Dashboard: http://localhost:8080/dashboard"
    echo "   • API docs: http://localhost:8080/docs"
    echo ""
    echo "📖 For detailed instructions, see:"
    echo "   ${CYAN}docs/ats_cicd_deployment_guide.md${NC}"
    echo ""
}

# Show quick commands reference
show_quick_commands() {
    log_header "Quick Commands Reference"
    
    echo ""
    echo "🔧 Deployment Commands:"
    echo "   Deploy system:     ./scripts/deploy/deploy-ats-system.sh"
    echo "   Test system:       ./scripts/test/test-ats-system.sh"
    echo "   Cleanup system:    ./scripts/deploy/deploy-ats-system.sh cleanup"
    echo "   Check status:      ./scripts/deploy/deploy-ats-system.sh status"
    echo ""
    echo "📊 Monitoring Commands:"
    echo "   View pods:         kubectl get pods -n ats-dev"
    echo "   View services:     kubectl get services -n ats-dev"
    echo "   Check logs:        kubectl logs -f -l app=ats-analytics-service -n ats-dev"
    echo "   Port forward:      kubectl port-forward -n ats-dev svc/ats-analytics-service 8080:8080"
    echo ""
    echo "🔍 Debugging Commands:"
    echo "   Shell into pod:    kubectl exec -it deployment/ats-analytics-service -n ats-dev -- /bin/bash"
    echo "   Check events:      kubectl get events -n ats-dev --sort-by='.lastTimestamp'"
    echo "   Describe pod:      kubectl describe pod <pod-name> -n ats-dev"
    echo ""
    echo "🚀 CI/CD Commands:"
    echo "   Check Argo apps:   argocd app list"
    echo "   Sync application:  argocd app sync ats-analytics-service"
    echo "   View rollout:      kubectl get rollouts -n ats-dev"
    echo ""
}

# Main setup function
main() {
    print_banner
    
    # Run setup steps
    setup_script_permissions
    validate_project_structure
    check_prerequisites
    
    # Display information
    show_system_architecture
    show_deployment_pipeline
    show_validation_strategy
    show_next_steps
    show_quick_commands
    
    log_success "🎉 ATS System setup completed successfully!"
    log_info "The deployment environment is ready."
    
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}                            Setup Complete!                                   ${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "Your ATS 3-Service System is ready for deployment with:"
    echo "• ✅ Minute-level data collection service"
    echo "• ✅ Enhanced EOD data service"  
    echo "• ✅ Unified analytics platform"
    echo "• ✅ Comprehensive CI/CD pipeline"
    echo "• ✅ Automated validation & rollback"
    echo "• ✅ Production-ready monitoring"
    echo ""
    echo "🚀 Ready to deploy? Run: ${CYAN}./scripts/deploy/deploy-ats-system.sh${NC}"
    echo ""
}

# Handle script arguments
case "${1:-setup}" in
    "setup")
        main
        ;;
    "validate")
        validate_project_structure
        check_prerequisites
        ;;
    "permissions")
        setup_script_permissions
        ;;
    "info")
        show_system_architecture
        show_deployment_pipeline
        show_validation_strategy
        ;;
    "help")
        echo "Usage: $0 [setup|validate|permissions|info|help]"
        echo "  setup: Full setup process (default)"
        echo "  validate: Validate project structure and prerequisites"
        echo "  permissions: Set script permissions only"
        echo "  info: Show system architecture and pipeline info"
        echo "  help: Show this help message"
        ;;
    *)
        echo "Usage: $0 [setup|validate|permissions|info|help]"
        exit 1
        ;;
esac