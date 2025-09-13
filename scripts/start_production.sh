#!/bin/bash

# ATS Data Quality Agent - Production Startup Script
# This script handles the complete production deployment process

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.production.yml"
ENV_FILE="$PROJECT_ROOT/config/production.env"

echo -e "${BLUE}🚀 ATS Data Quality Agent - Production Startup${NC}"
echo "================================================="

# Function to print status messages
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

# Function to check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi
    
    print_success "Prerequisites check passed"
}

# Function to validate configuration
validate_configuration() {
    print_status "Validating configuration..."
    
    if [[ ! -f "$ENV_FILE" ]]; then
        print_warning "Production environment file not found: $ENV_FILE"
        print_status "Creating from template..."
        
        if [[ -f "$PROJECT_ROOT/config/production.env.template" ]]; then
            cp "$PROJECT_ROOT/config/production.env.template" "$ENV_FILE"
            print_warning "Please edit $ENV_FILE with your actual values before continuing."
            print_warning "Required changes:"
            echo "  - DB_PASSWORD: Set secure database password"
            echo "  - POLYGON_API_KEY: Set your Polygon API key"
            echo "  - TIINGO_API_KEY: Set your Tiingo API key"
            echo "  - EODHD_API_KEY: Set your EODHD API key"
            echo "  - EMAIL_* settings: Configure SMTP settings"
            echo "  - SLACK_WEBHOOK_URL: Configure Slack notifications"
            echo ""
            read -p "Press Enter after updating the configuration file..."
        else
            print_error "Template file not found. Please create $ENV_FILE manually."
            exit 1
        fi
    fi
    
    # Source environment file
    set -a  # Automatically export all variables
    source "$ENV_FILE"
    set +a
    
    # Check critical variables
    if [[ "$DB_PASSWORD" == "YOUR_PRODUCTION_PASSWORD_HERE" ]]; then
        print_error "Please set a secure DB_PASSWORD in $ENV_FILE"
        exit 1
    fi
    
    print_success "Configuration validation passed"
}

# Function to setup Docker network
setup_network() {
    print_status "Setting up Docker network..."
    
    if ! docker network ls | grep -q "ats-network"; then
        print_status "Creating ats-network..."
        docker network create ats-network
        print_success "Docker network created"
    else
        print_status "Docker network already exists"
    fi
}

# Function to create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    
    # Create log directories
    mkdir -p "$PROJECT_ROOT/logs"/{agent,alerts,system}
    mkdir -p /mnt/d/ats-backup
    
    # Set permissions
    chmod 755 "$PROJECT_ROOT/logs"
    chmod 755 /mnt/d/ats-backup
    
    print_success "Directories created"
}

# Function to initialize database
initialize_database() {
    print_status "Initializing database..."
    
    # Create init script if it doesn't exist
    INIT_SCRIPT="$PROJECT_ROOT/config/init-db.sql"
    if [[ ! -f "$INIT_SCRIPT" ]]; then
        cat > "$INIT_SCRIPT" << 'EOF'
-- ATS Data Quality Agent Database Initialization

-- Create extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create agent tables
CREATE TABLE IF NOT EXISTS agent_issues (
    issue_id VARCHAR(50) PRIMARY KEY,
    severity VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL,
    issue_type VARCHAR(50) NOT NULL,
    symbol VARCHAR(10),
    date DATE,
    description TEXT,
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    resolved_at TIMESTAMP WITH TIME ZONE,
    vendor VARCHAR(20),
    metadata JSONB
);

CREATE TABLE IF NOT EXISTS agent_workflows (
    workflow_id VARCHAR(50) PRIMARY KEY,
    issue_id VARCHAR(50) REFERENCES agent_issues(issue_id),
    state VARCHAR(20) NOT NULL,
    tool_name VARCHAR(50) NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    progress INTEGER DEFAULT 0,
    execution_log JSONB
);

CREATE TABLE IF NOT EXISTS agent_alerts (
    alert_id VARCHAR(50) PRIMARY KEY,
    severity VARCHAR(10) NOT NULL,
    type VARCHAR(20) NOT NULL,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMP WITH TIME ZONE,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP WITH TIME ZONE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_agent_issues_severity_status ON agent_issues(severity, status);
CREATE INDEX IF NOT EXISTS idx_agent_issues_detected_at ON agent_issues(detected_at);
CREATE INDEX IF NOT EXISTS idx_agent_workflows_state ON agent_workflows(state);
CREATE INDEX IF NOT EXISTS idx_agent_alerts_severity_resolved ON agent_alerts(severity, resolved);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
EOF
        print_success "Database initialization script created"
    fi
}

# Function to pull latest images
pull_images() {
    print_status "Pulling latest Docker images..."
    
    docker-compose -f "$COMPOSE_FILE" pull
    
    print_success "Images pulled successfully"
}

# Function to start services
start_services() {
    print_status "Starting production services..."
    
    # Load environment variables
    export $(grep -v '^#' "$ENV_FILE" | xargs)
    
    # Start services
    docker-compose -f "$COMPOSE_FILE" up -d
    
    print_success "Services started"
}

# Function to wait for services to be healthy
wait_for_services() {
    print_status "Waiting for services to be healthy..."
    
    local max_wait=300  # 5 minutes
    local wait_time=0
    local interval=10
    
    while [[ $wait_time -lt $max_wait ]]; do
        if docker-compose -f "$COMPOSE_FILE" ps | grep -q "unhealthy"; then
            print_status "Services starting... (${wait_time}s elapsed)"
            sleep $interval
            wait_time=$((wait_time + interval))
        else
            print_success "All services are healthy"
            return 0
        fi
    done
    
    print_error "Services failed to become healthy within $max_wait seconds"
    print_status "Service status:"
    docker-compose -f "$COMPOSE_FILE" ps
    exit 1
}

# Function to validate deployment
validate_deployment() {
    print_status "Validating deployment..."
    
    # Test API endpoints
    local api_url="http://localhost:4000"
    
    # Test health endpoint
    if curl -f -s "$api_url/health" > /dev/null; then
        print_success "Health endpoint is responding"
    else
        print_error "Health endpoint is not responding"
        return 1
    fi
    
    # Test agent status endpoint
    if curl -f -s "$api_url/agent/status" > /dev/null; then
        print_success "Agent status endpoint is responding"
    else
        print_error "Agent status endpoint is not responding"
        return 1
    fi
    
    # Test dashboard
    if curl -f -s "$api_url/data-quality/dashboard" > /dev/null; then
        print_success "Dashboard is accessible"
    else
        print_error "Dashboard is not accessible"
        return 1
    fi
    
    print_success "Deployment validation passed"
}

# Function to display service information
display_service_info() {
    print_success "🎉 Production deployment completed successfully!"
    echo ""
    echo "Service Information:"
    echo "==================="
    echo "📊 Dashboard:     http://localhost:4000/data-quality/dashboard"
    echo "🔗 API Base URL:  http://localhost:4000"
    echo "🏥 Health Check:  http://localhost:4000/health"
    echo "📈 Agent Status:  http://localhost:4000/agent/status"
    echo ""
    echo "Service Status:"
    docker-compose -f "$COMPOSE_FILE" ps
    echo ""
    echo "Log Monitoring:"
    echo "==============="
    echo "📋 All logs:      docker-compose -f $COMPOSE_FILE logs -f"
    echo "📊 Analytics:     docker logs ats-prod-analytics -f"
    echo "🤖 Agent:        docker logs ats-prod-agent -f"
    echo "🗄️  Database:     docker logs ats-prod-postgres -f"
    echo ""
    echo "Management Commands:"
    echo "==================="
    echo "🛑 Stop:          docker-compose -f $COMPOSE_FILE down"
    echo "🔄 Restart:       docker-compose -f $COMPOSE_FILE restart"
    echo "📊 Status:        docker-compose -f $COMPOSE_FILE ps"
    echo "🧪 Validate:      python scripts/validate_system.py"
}

# Function to handle cleanup on failure
cleanup_on_failure() {
    print_error "Deployment failed. Cleaning up..."
    docker-compose -f "$COMPOSE_FILE" down
    print_status "Cleanup completed"
}

# Main execution flow
main() {
    # Set trap for cleanup on failure
    trap cleanup_on_failure ERR
    
    # Change to project directory
    cd "$PROJECT_ROOT"
    
    # Execute deployment steps
    check_prerequisites
    validate_configuration
    setup_network
    create_directories
    initialize_database
    pull_images
    start_services
    wait_for_services
    validate_deployment
    display_service_info
    
    print_success "Production deployment completed successfully! 🎉"
}

# Parse command line arguments
case "${1:-start}" in
    "start")
        main
        ;;
    "stop")
        print_status "Stopping production services..."
        docker-compose -f "$COMPOSE_FILE" down
        print_success "Services stopped"
        ;;
    "restart")
        print_status "Restarting production services..."
        docker-compose -f "$COMPOSE_FILE" down
        docker-compose -f "$COMPOSE_FILE" up -d
        wait_for_services
        print_success "Services restarted"
        ;;
    "status")
        print_status "Service status:"
        docker-compose -f "$COMPOSE_FILE" ps
        ;;
    "logs")
        docker-compose -f "$COMPOSE_FILE" logs -f
        ;;
    "validate")
        validate_deployment
        ;;
    "help")
        echo "Usage: $0 [start|stop|restart|status|logs|validate|help]"
        echo ""
        echo "Commands:"
        echo "  start      Start production services (default)"
        echo "  stop       Stop production services"
        echo "  restart    Restart production services"
        echo "  status     Show service status"
        echo "  logs       Follow service logs"
        echo "  validate   Validate deployment"
        echo "  help       Show this help message"
        ;;
    *)
        print_error "Unknown command: $1"
        echo "Use '$0 help' for available commands"
        exit 1
        ;;
esac