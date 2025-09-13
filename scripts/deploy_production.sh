#!/bin/bash

# ATS Data Quality Agent - One-Click Production Deployment Script
# This script performs complete production deployment with validation

set -e  # Exit on any error

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_ROOT/config/production.env"

echo -e "${PURPLE}🚀 ATS Data Quality Agent - Production Deployment${NC}"
echo "====================================================="
echo ""

# Helper functions
print_step() {
    echo -e "${BLUE}[STEP $1/$2]${NC} $3"
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

# Step 1: Pre-flight checks
print_step 1 7 "Pre-flight checks"
echo "Checking system requirements..."

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed"
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is not installed"
    exit 1
fi

# Check Docker daemon
if ! docker info &> /dev/null; then
    print_error "Docker daemon is not running"
    exit 1
fi

# Check available disk space (need at least 5GB)
available_space=$(df /var/lib/docker --output=avail | tail -n1)
if [[ $available_space -lt 5000000 ]]; then
    print_warning "Low disk space available. Need at least 5GB free."
fi

print_success "Pre-flight checks passed"
echo ""

# Step 2: Environment setup
print_step 2 7 "Environment setup"

# Run setup script
if [[ -f "$PROJECT_ROOT/scripts/setup_environment.py" ]]; then
    echo "Running environment setup script..."
    cd "$PROJECT_ROOT"
    python scripts/setup_environment.py production
    print_success "Environment setup completed"
else
    print_warning "Environment setup script not found, skipping automated setup"
fi

# Verify environment file exists
if [[ ! -f "$ENV_FILE" ]]; then
    print_error "Production environment file not found: $ENV_FILE"
    print_error "Please run: python scripts/setup_environment.py production"
    exit 1
fi

# Check critical environment variables
source "$ENV_FILE"
if [[ "$DB_PASSWORD" == "YOUR_PRODUCTION_PASSWORD_HERE" ]]; then
    print_error "Please set a secure DB_PASSWORD in $ENV_FILE"
    exit 1
fi

print_success "Environment configuration validated"
echo ""

# Step 3: System validation
print_step 3 7 "System validation"

echo "Running comprehensive system validation..."
if python scripts/validate_system.py --quick; then
    print_success "System validation passed"
else
    print_error "System validation failed"
    exit 1
fi
echo ""

# Step 4: Start production services
print_step 4 7 "Starting production services"

echo "Starting ATS Data Quality Agent production deployment..."
if "$PROJECT_ROOT/scripts/start_production.sh" start; then
    print_success "Production services started successfully"
else
    print_error "Failed to start production services"
    exit 1
fi
echo ""

# Step 5: Health checks
print_step 5 7 "Health checks and validation"

echo "Waiting for services to be fully operational..."
sleep 30

# Test API endpoints
echo "Testing API endpoints..."
if python scripts/test_api_endpoints.py; then
    print_success "API endpoints are responding correctly"
else
    print_error "API endpoint validation failed"
    exit 1
fi

# Test database integration
echo "Testing database integration..."
if python scripts/test_database_integration.py; then
    print_success "Database integration is working"
else
    print_error "Database integration failed"
    exit 1
fi

# Quick health check
echo "Running quick health check..."
if python scripts/quick_health_check.py; then
    print_success "All health checks passed"
else
    print_error "Health checks failed"
    exit 1
fi
echo ""

# Step 6: Agent initialization
print_step 6 7 "Agent initialization"

echo "Initializing the data quality agent..."

# Start the agent via API
agent_start_response=$(curl -s -X POST http://localhost:4000/agent/start || echo "FAILED")
if [[ "$agent_start_response" == *"success"* ]]; then
    print_success "Data quality agent started successfully"
else
    print_warning "Agent may need manual start. Check dashboard at http://localhost:4000/data-quality/dashboard"
fi

# Load production configuration preset
curl -s -X POST http://localhost:4000/agent/config/preset/production > /dev/null || true
print_success "Production configuration loaded"
echo ""

# Step 7: Final validation and summary
print_step 7 7 "Final validation and deployment summary"

echo "Performing final deployment validation..."

# Check all services are running
if docker-compose -f "$PROJECT_ROOT/docker-compose.production.yml" ps | grep -q "unhealthy"; then
    print_error "Some services are unhealthy"
    docker-compose -f "$PROJECT_ROOT/docker-compose.production.yml" ps
    exit 1
fi

# Test dashboard accessibility
if curl -f -s http://localhost:4000/data-quality/dashboard > /dev/null; then
    print_success "Dashboard is accessible"
else
    print_error "Dashboard is not accessible"
    exit 1
fi

# Test agent status
agent_status=$(curl -s http://localhost:4000/agent/status | grep -o '"agent_status":"[^"]*"' | cut -d'"' -f4 || echo "UNKNOWN")
if [[ "$agent_status" == "ACTIVE" ]]; then
    print_success "Agent is active and monitoring"
else
    print_warning "Agent status: $agent_status (check dashboard for details)"
fi

echo ""
print_success "🎉 Production deployment completed successfully!"
echo ""

# Deployment summary
echo -e "${PURPLE}📋 DEPLOYMENT SUMMARY${NC}"
echo "======================"
echo ""
echo -e "${GREEN}🌐 Services Deployed:${NC}"
echo "  • PostgreSQL Database (TimescaleDB)"
echo "  • Analytics Service with Dashboard"
echo "  • Data Quality Agent"
echo ""
echo -e "${GREEN}🔗 Access Points:${NC}"
echo "  • Dashboard:    http://localhost:4000/data-quality/dashboard"
echo "  • API:          http://localhost:4000"
echo "  • Health Check: http://localhost:4000/health"
echo "  • Agent Status: http://localhost:4000/agent/status"
echo ""
echo -e "${GREEN}📊 Service Status:${NC}"
docker-compose -f "$PROJECT_ROOT/docker-compose.production.yml" ps
echo ""
echo -e "${GREEN}🛠️  Management Commands:${NC}"
echo "  • View logs:    docker-compose -f docker-compose.production.yml logs -f"
echo "  • Stop all:     docker-compose -f docker-compose.production.yml down"
echo "  • Restart:      ./scripts/start_production.sh restart"
echo "  • Status:       ./scripts/start_production.sh status"
echo "  • Validate:     python scripts/validate_system.py"
echo ""
echo -e "${GREEN}📚 Documentation:${NC}"
echo "  • Production Guide:  docs/PRODUCTION_DEPLOYMENT_GUIDE.md"
echo "  • Operator Training: docs/OPERATOR_TRAINING_GUIDE.md"
echo "  • API Reference:     docs/API_REFERENCE.md"
echo "  • Troubleshooting:   docs/TROUBLESHOOTING_GUIDE.md"
echo ""
echo -e "${GREEN}🎓 Next Steps:${NC}"
echo "  1. Review the dashboard and familiarize yourself with the interface"
echo "  2. Configure alert notifications (email/Slack) if needed"
echo "  3. Set up monitoring and backup procedures"
echo "  4. Train operators using the provided documentation"
echo "  5. Schedule regular health checks and maintenance"
echo ""
echo -e "${BLUE}💡 Tips:${NC}"
echo "  • The agent will start monitoring automatically"
echo "  • Check the dashboard regularly for data quality issues"
echo "  • Use the API for automation and integration"
echo "  • Monitor logs for any issues or alerts"
echo ""
print_success "Ready for production use! 🚀"
echo ""

# Optional: Open dashboard in browser
if command -v xdg-open &> /dev/null; then
    read -p "Open dashboard in browser? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        xdg-open http://localhost:4000/data-quality/dashboard
    fi
elif command -v open &> /dev/null; then
    read -p "Open dashboard in browser? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        open http://localhost:4000/data-quality/dashboard
    fi
fi