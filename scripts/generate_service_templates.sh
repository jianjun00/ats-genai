#!/bin/bash

# Service Template Generator
# Generates complete service architecture for any domain
# Usage: ./generate_service_templates.sh <DomainName>

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEMPLATES_DIR="$PROJECT_ROOT/src/domains/core/services/templates"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
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

# Function to show usage
show_usage() {
    echo "Service Template Generator"
    echo ""
    echo "Usage: $0 <DomainName>"
    echo ""
    echo "Examples:"
    echo "  $0 MarketData    # Generate Market Data service"
    echo "  $0 Analytics     # Generate Analytics service" 
    echo "  $0 Trading       # Generate Trading service"
    echo "  $0 News          # Generate News service"
    echo ""
    echo "This script generates:"
    echo "  - Service interface with comprehensive operations"
    echo "  - Service implementation with business logic structure"
    echo "  - HTTP API router with REST endpoints"
    echo "  - Service container with dependency injection"
    echo "  - Test templates for unit and integration testing"
    echo "  - Complete directory structure"
}

# Function to validate domain name
validate_domain_name() {
    local domain="$1"
    
    # Check if domain name is provided
    if [[ -z "$domain" ]]; then
        print_error "Domain name is required"
        show_usage
        exit 1
    fi
    
    # Check if domain name is valid (starts with capital letter, alphanumeric)
    if [[ ! "$domain" =~ ^[A-Z][A-Za-z0-9]*$ ]]; then
        print_error "Domain name must start with a capital letter and contain only alphanumeric characters"
        print_error "Examples: MarketData, Analytics, Trading, News"
        exit 1
    fi
    
    print_success "Domain name '$domain' is valid"
}

# Function to create directory structure
create_directory_structure() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    print_status "Creating directory structure for $domain service..."
    
    # Create main service directories
    mkdir -p "$PROJECT_ROOT/src/domains/$domain_lower/services/interfaces"
    mkdir -p "$PROJECT_ROOT/src/domains/$domain_lower/services/impl" 
    mkdir -p "$PROJECT_ROOT/src/domains/$domain_lower/services/config"
    
    # Create test directories
    mkdir -p "$PROJECT_ROOT/tests/domains/$domain_lower/services"
    mkdir -p "$PROJECT_ROOT/tests/integration"
    
    # Create API directory (if it doesn't exist)
    mkdir -p "$PROJECT_ROOT/src/services/web_services/api"
    
    print_success "Directory structure created"
}

# Function to generate service interface
generate_service_interface() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    local template_file="$TEMPLATES_DIR/service_interface_template.py"
    local output_file="$PROJECT_ROOT/src/domains/$domain_lower/services/interfaces/${domain_lower}_service_interface.py"
    
    print_status "Generating service interface..."
    
    if [[ ! -f "$template_file" ]]; then
        print_error "Template file not found: $template_file"
        exit 1
    fi
    
    # Replace {DOMAIN} placeholder with actual domain name
    sed "s/{DOMAIN}/$domain/g" "$template_file" > "$output_file"
    
    print_success "Service interface generated: $output_file"
}

# Function to generate service implementation
generate_service_implementation() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    local template_file="$TEMPLATES_DIR/service_implementation_template.py"
    local output_file="$PROJECT_ROOT/src/domains/$domain_lower/services/impl/${domain_lower}_service_impl.py"
    
    print_status "Generating service implementation..."
    
    if [[ ! -f "$template_file" ]]; then
        print_error "Template file not found: $template_file"
        exit 1
    fi
    
    # Replace {DOMAIN} placeholder with actual domain name
    sed "s/{DOMAIN}/$domain/g" "$template_file" > "$output_file"
    
    print_success "Service implementation generated: $output_file"
}

# Function to generate API router
generate_api_router() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    local template_file="$TEMPLATES_DIR/api_router_template.py"
    local output_file="$PROJECT_ROOT/src/services/web_services/api/${domain_lower}_api.py"
    
    print_status "Generating API router..."
    
    if [[ ! -f "$template_file" ]]; then
        print_error "Template file not found: $template_file"
        exit 1
    fi
    
    # Replace {DOMAIN} placeholder with actual domain name
    sed "s/{DOMAIN}/$domain/g" "$template_file" > "$output_file"
    
    print_success "API router generated: $output_file"
}

# Function to generate service container
generate_service_container() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    local template_file="$TEMPLATES_DIR/service_container_template.py"
    local output_file="$PROJECT_ROOT/src/domains/$domain_lower/services/config/service_container.py"
    
    print_status "Generating service container..."
    
    if [[ ! -f "$template_file" ]]; then
        print_error "Template file not found: $template_file"
        exit 1
    fi
    
    # Replace {DOMAIN} placeholder with actual domain name
    sed "s/{DOMAIN}/$domain/g" "$template_file" > "$output_file"
    
    print_success "Service container generated: $output_file"
}

# Function to generate __init__.py files
generate_init_files() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    print_status "Generating __init__.py files..."
    
    # Service interfaces __init__.py
    cat > "$PROJECT_ROOT/src/domains/$domain_lower/services/interfaces/__init__.py" << EOF
# $domain service interfaces
EOF
    
    # Service implementations __init__.py
    cat > "$PROJECT_ROOT/src/domains/$domain_lower/services/impl/__init__.py" << EOF
# $domain service implementations
EOF
    
    # Service config __init__.py
    cat > "$PROJECT_ROOT/src/domains/$domain_lower/services/config/__init__.py" << EOF
# $domain service configuration
EOF
    
    # Main services __init__.py
    cat > "$PROJECT_ROOT/src/domains/$domain_lower/services/__init__.py" << EOF
# $domain services
EOF
    
    # Domain __init__.py
    mkdir -p "$PROJECT_ROOT/src/domains/$domain_lower"
    cat > "$PROJECT_ROOT/src/domains/$domain_lower/__init__.py" << EOF
# $domain domain
EOF
    
    # Tests __init__.py
    cat > "$PROJECT_ROOT/tests/domains/$domain_lower/__init__.py" << EOF
# $domain tests
EOF
    
    cat > "$PROJECT_ROOT/tests/domains/$domain_lower/services/__init__.py" << EOF  
# $domain service tests
EOF
    
    print_success "__init__.py files generated"
}

# Function to generate test templates
generate_test_templates() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    print_status "Generating test templates..."
    
    # Service implementation test template
    cat > "$PROJECT_ROOT/tests/domains/$domain_lower/services/test_${domain_lower}_service_impl.py" << EOF
"""
Tests for ${domain}ServiceImpl

Tests the business logic layer with mocked DAOs to verify:
1. Service interface contract compliance
2. Business rule enforcement  
3. Error handling
4. DTO conversions
5. Transaction coordination
"""

import pytest
from unittest.mock import Mock, AsyncMock
from datetime import date, datetime

from domains.${domain_lower}.services.impl.${domain_lower}_service_impl import ${domain}ServiceImpl
from domains.${domain_lower}.services.interfaces.${domain_lower}_service_interface import (
    ${domain}DTO,
    ${domain}SearchCriteria,
    ${domain}OperationResult
)


class Test${domain}ServiceImpl:
    """Test suite for ${domain}ServiceImpl business logic"""
    
    @pytest.fixture
    def mock_${domain_lower}_dao(self):
        """Mock primary DAO"""
        dao = Mock()
        # TODO: Add mock methods based on your DAO interface
        # dao.create = AsyncMock()
        # dao.get_by_id = AsyncMock()
        # dao.update = AsyncMock()
        # dao.delete = AsyncMock()
        # dao.search = AsyncMock()
        return dao
    
    @pytest.fixture
    def service(self, mock_${domain_lower}_dao):
        """Create ${domain}ServiceImpl with mocked dependencies"""
        return ${domain}ServiceImpl(
            # TODO: Pass your actual DAO dependencies
            # ${domain_lower}_dao=mock_${domain_lower}_dao,
        )
    
    @pytest.fixture
    def sample_dto(self):
        """Sample DTO for testing"""
        return ${domain}DTO(
            # TODO: Add sample data based on your domain
            # symbol="AAPL",
            # name="Apple Inc.",
        )
    
    # Test create operation
    @pytest.mark.asyncio
    async def test_create_${domain_lower}_success(self, service, mock_${domain_lower}_dao, sample_dto):
        """Test successful entity creation"""
        # Setup mocks
        # mock_${domain_lower}_dao.get_by_symbol.return_value = None  # No existing entity
        # mock_${domain_lower}_dao.create.return_value = 123
        
        # Execute
        # result = await service.create_${domain_lower}(sample_dto)
        
        # Verify
        # assert result.success is True
        # assert result.entity_id == 123
        
        # TODO: Implement actual test logic
        pass
    
    @pytest.mark.asyncio
    async def test_create_${domain_lower}_validation_error(self, service):
        """Test creation with validation error"""
        # TODO: Implement validation error test
        pass
    
    # Test get operation
    @pytest.mark.asyncio
    async def test_get_${domain_lower}_by_id_success(self, service, mock_${domain_lower}_dao):
        """Test successful entity retrieval"""
        # TODO: Implement get test
        pass
    
    @pytest.mark.asyncio
    async def test_get_${domain_lower}_by_id_not_found(self, service, mock_${domain_lower}_dao):
        """Test entity not found"""
        # TODO: Implement not found test
        pass
    
    # Test update operation
    @pytest.mark.asyncio
    async def test_update_${domain_lower}_success(self, service, mock_${domain_lower}_dao, sample_dto):
        """Test successful entity update"""
        # TODO: Implement update test
        pass
    
    # Test delete operation  
    @pytest.mark.asyncio
    async def test_delete_${domain_lower}_success(self, service, mock_${domain_lower}_dao):
        """Test successful entity deletion"""
        # TODO: Implement delete test
        pass
    
    # Test search operations
    @pytest.mark.asyncio
    async def test_list_${domain_lower}s_with_criteria(self, service, mock_${domain_lower}_dao):
        """Test listing with search criteria"""
        # TODO: Implement search test
        pass
    
    # Test batch operations
    @pytest.mark.asyncio
    async def test_create_${domain_lower}s_batch_success(self, service, mock_${domain_lower}_dao):
        """Test successful batch creation"""
        # TODO: Implement batch test
        pass
    
    # Test error handling
    @pytest.mark.asyncio
    async def test_dao_exception_handling(self, service, mock_${domain_lower}_dao):
        """Test proper exception handling when DAO throws exception"""
        # TODO: Implement error handling test
        pass
EOF

    # API integration test template
    cat > "$PROJECT_ROOT/tests/integration/test_${domain_lower}_api_integration.py" << EOF
"""
Integration tests for ${domain} API

Tests the complete HTTP API layer with service integration.
These tests verify:
1. HTTP request/response handling
2. Service layer integration  
3. Error handling and status codes
4. DTO conversion between HTTP and service layers
5. End-to-end functionality
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch
from fastapi.testclient import TestClient
from fastapi import FastAPI

from services.web_services.api.${domain_lower}_api import ${domain_lower}_router
from domains.${domain_lower}.services.interfaces.${domain_lower}_service_interface import (
    ${domain}DTO,
    ${domain}OperationResult
)


class Test${domain}APIIntegration:
    """Integration test suite for ${domain} API"""
    
    @pytest.fixture
    def mock_service(self):
        """Mock ${domain}ServiceInterface for testing"""
        service = Mock()
        
        # Setup async methods
        service.create_${domain_lower} = AsyncMock()
        service.get_${domain_lower}_by_id = AsyncMock()
        service.list_${domain_lower}s = AsyncMock()
        service.update_${domain_lower} = AsyncMock()
        service.delete_${domain_lower} = AsyncMock()
        service.health_check = AsyncMock()
        
        return service
    
    @pytest.fixture
    def client(self, mock_service):
        """Create FastAPI test client with mocked service"""
        app = FastAPI()
        app.include_router(${domain_lower}_router)
        
        # Patch the dependency injection
        with patch('services.web_services.api.${domain_lower}_api.get_service', 
                  return_value=mock_service):
            yield TestClient(app)
    
    @pytest.fixture
    def sample_request_data(self):
        """Sample request data for testing"""
        return {
            # TODO: Add sample request data based on your domain
            # "symbol": "AAPL",
            # "name": "Apple Inc.",
        }
    
    # Test create endpoint
    def test_create_${domain_lower}_success(self, client, mock_service, sample_request_data):
        """Test successful entity creation via API"""
        # Setup mock
        mock_service.create_${domain_lower}.return_value = ${domain}OperationResult(
            success=True,
            entity_id=123,
            created_count=1
        )
        
        # Execute
        response = client.post("/api/v1/${domain_lower}/", json=sample_request_data)
        
        # Verify HTTP response
        assert response.status_code == 201
        data = response.json()
        assert data["success"] is True
        assert data["entity_id"] == 123
        
        # Verify service called correctly
        mock_service.create_${domain_lower}.assert_called_once()
    
    def test_create_${domain_lower}_validation_error(self, client, mock_service):
        """Test creation with validation error"""
        # Setup mock
        mock_service.create_${domain_lower}.return_value = ${domain}OperationResult(
            success=False,
            error_message="Validation failed"
        )
        
        # Execute with invalid data
        response = client.post("/api/v1/${domain_lower}/", json={})
        
        # Verify HTTP response
        assert response.status_code == 400
    
    # Test get endpoint
    def test_get_${domain_lower}_success(self, client, mock_service):
        """Test successful entity retrieval"""
        # Setup mock
        mock_service.get_${domain_lower}_by_id.return_value = ${domain}DTO(
            id=123,
            # TODO: Add sample DTO data
        )
        
        # Execute
        response = client.get("/api/v1/${domain_lower}/123")
        
        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 123
    
    def test_get_${domain_lower}_not_found(self, client, mock_service):
        """Test entity not found"""
        # Setup mock
        mock_service.get_${domain_lower}_by_id.return_value = None
        
        # Execute
        response = client.get("/api/v1/${domain_lower}/999")
        
        # Verify HTTP response
        assert response.status_code == 404
    
    # Test list endpoint
    def test_list_${domain_lower}s_success(self, client, mock_service):
        """Test successful entity listing"""
        # Setup mock
        mock_service.list_${domain_lower}s.return_value = [
            ${domain}DTO(id=1),
            ${domain}DTO(id=2)
        ]
        mock_service.count_${domain_lower}s.return_value = 2
        
        # Execute
        response = client.get("/api/v1/${domain_lower}/?limit=10")
        
        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 2
        assert len(data["items"]) == 2
    
    # Test health check endpoint
    def test_health_check(self, client, mock_service):
        """Test health check endpoint"""
        # Setup mock
        mock_service.health_check.return_value = {"status": "healthy"}
        
        # Execute
        response = client.get("/api/v1/${domain_lower}/health")
        
        # Verify HTTP response
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
    
    # Test error scenarios
    def test_internal_server_error(self, client, mock_service):
        """Test API behavior on service exception"""
        # Setup mock to raise exception
        mock_service.get_${domain_lower}_by_id.side_effect = Exception("Database error")
        
        # Execute
        response = client.get("/api/v1/${domain_lower}/123")
        
        # Verify HTTP response
        assert response.status_code == 500
EOF

    print_success "Test templates generated"
}

# Function to generate documentation
generate_documentation() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    print_status "Generating documentation..."
    
    cat > "$PROJECT_ROOT/src/domains/$domain_lower/README.md" << EOF
# $domain Service

This service provides comprehensive $domain operations for the ATS platform.

## Architecture

The service follows the standard service-based architecture pattern:

\`\`\`
HTTP API Layer → Service Interface → Service Implementation → DAO Layer
\`\`\`

## Components

### Service Interface
- **File**: \`services/interfaces/${domain_lower}_service_interface.py\`
- **Purpose**: Defines the public contract for $domain operations
- **Operations**: CRUD, search, batch operations, and domain-specific functionality

### Service Implementation  
- **File**: \`services/impl/${domain_lower}_service_impl.py\`
- **Purpose**: Business logic implementation with DAO coordination
- **Features**: Validation, error handling, transaction management

### HTTP API
- **File**: \`../../services/web_services/api/${domain_lower}_api.py\`
- **Purpose**: REST endpoints for $domain operations
- **Base URL**: \`/api/v1/${domain_lower}\`

### Service Container
- **File**: \`services/config/service_container.py\`
- **Purpose**: Dependency injection and service lifecycle management
- **Features**: Environment-aware configuration, health checks

## Usage

### Basic Service Usage
\`\`\`python
from domains.${domain_lower}.services.config.service_container import get_${domain_lower}_service

# Get service instance
service = await get_${domain_lower}_service()

# Create entity
dto = ${domain}DTO(
    # TODO: Add your domain-specific fields
)
result = await service.create_${domain_lower}(dto)

# Get entity
entity = await service.get_${domain_lower}_by_id(result.entity_id)

# Search entities
criteria = ${domain}SearchCriteria(limit=100)
entities = await service.list_${domain_lower}s(criteria)
\`\`\`

### API Usage
\`\`\`bash
# Create entity
curl -X POST "http://localhost:8000/api/v1/${domain_lower}/" \\
  -H "Content-Type: application/json" \\
  -d '{
    // TODO: Add sample request data
  }'

# Get entity
curl "http://localhost:8000/api/v1/${domain_lower}/123"

# List entities
curl "http://localhost:8000/api/v1/${domain_lower}/?limit=10&offset=0"

# Health check
curl "http://localhost:8000/api/v1/${domain_lower}/health"
\`\`\`

## Development

### Running Tests
\`\`\`bash
# Service unit tests
pytest tests/domains/${domain_lower}/services/ -v

# API integration tests  
pytest tests/integration/test_${domain_lower}_api_integration.py -v

# All tests
pytest tests/domains/${domain_lower}/ tests/integration/test_${domain_lower}_* -v
\`\`\`

### Adding Custom Operations

1. **Add to Service Interface**:
   \`\`\`python
   @abstractmethod
   async def custom_${domain_lower}_operation(self, params) -> ${domain}OperationResult:
       """Custom domain-specific operation"""
       pass
   \`\`\`

2. **Implement in Service**:
   \`\`\`python
   async def custom_${domain_lower}_operation(self, params) -> ${domain}OperationResult:
       # Implement business logic
       return create_success_result(metadata={'operation': 'custom'})
   \`\`\`

3. **Add API Endpoint**:
   \`\`\`python
   @${domain_lower}_router.post("/custom-operation")
   async def custom_operation(params: CustomRequest, service = Depends(get_service)):
       result = await service.custom_${domain_lower}_operation(params)
       return operation_result_to_response(result)
   \`\`\`

## TODO: Customization Required

This service was generated from templates and requires customization:

### Service Interface
- [ ] Add domain-specific DTOs and fields
- [ ] Define custom operations for your domain
- [ ] Add proper validation rules

### Service Implementation  
- [ ] Implement actual DAO integration
- [ ] Add domain-specific business logic
- [ ] Configure proper error handling

### API Layer
- [ ] Add custom endpoints
- [ ] Implement request/response models
- [ ] Add domain-specific query parameters

### Service Container
- [ ] Configure actual DAO dependencies  
- [ ] Add vendor integrations if needed
- [ ] Set up proper health checks

### Testing
- [ ] Implement actual test cases
- [ ] Add domain-specific test data
- [ ] Create integration test scenarios

## Performance Considerations

- Use batch operations for bulk processing
- Implement caching for frequently accessed data
- Add proper pagination for large result sets
- Monitor service performance with health checks

## Error Handling

All operations return structured results:
- \`success: bool\` - Operation success status
- \`error_message: str\` - Detailed error information
- \`entity_id: int\` - Created/updated entity identifier
- \`created_count: int\` - Number of entities processed

## Health Monitoring

The service provides comprehensive health checks:
- Service health: \`GET /api/v1/${domain_lower}/health\`
- Container health: \`container.get_health_status()\`
- Database connectivity and performance monitoring
EOF

    print_success "Documentation generated: src/domains/$domain_lower/README.md"
}

# Function to generate summary report
generate_summary_report() {
    local domain="$1"
    local domain_lower=$(echo "$domain" | tr '[:upper:]' '[:lower:]')
    
    echo ""
    echo "========================================="
    echo "🎉 $domain Service Generation Complete!"
    echo "========================================="
    echo ""
    
    print_success "Generated Files:"
    echo "  📁 Service Architecture:"
    echo "    • src/domains/$domain_lower/services/interfaces/${domain_lower}_service_interface.py"
    echo "    • src/domains/$domain_lower/services/impl/${domain_lower}_service_impl.py"  
    echo "    • src/domains/$domain_lower/services/config/service_container.py"
    echo ""
    echo "  🌐 HTTP API:"
    echo "    • src/services/web_services/api/${domain_lower}_api.py"
    echo ""
    echo "  🧪 Tests:"
    echo "    • tests/domains/$domain_lower/services/test_${domain_lower}_service_impl.py"
    echo "    • tests/integration/test_${domain_lower}_api_integration.py"
    echo ""
    echo "  📚 Documentation:"
    echo "    • src/domains/$domain_lower/README.md"
    echo ""
    
    print_warning "Next Steps:"
    echo "  1. 📝 Customize the service interface with your domain-specific DTOs and operations"
    echo "  2. 🔧 Implement actual DAO integration in the service implementation"  
    echo "  3. 🌐 Add custom API endpoints for domain-specific operations"
    echo "  4. ⚙️  Configure DAO dependencies in the service container"
    echo "  5. 🧪 Implement comprehensive tests with real test data"
    echo "  6. ✅ Run tests to validate your implementation"
    echo ""
    
    print_status "Validation Commands:"
    echo "  # Test service logic:"
    echo "  pytest tests/domains/$domain_lower/services/ -v"
    echo ""
    echo "  # Test API endpoints:"
    echo "  pytest tests/integration/test_${domain_lower}_api_integration.py -v"
    echo ""
    echo "  # Check service health:"
    echo "  python -c \""
    echo "    import asyncio"
    echo "    from domains.$domain_lower.services.config.service_container import get_${domain_lower}_service"
    echo "    async def test():"
    echo "        service = await get_${domain_lower}_service()"
    echo "        health = await service.health_check()"
    echo "        print(f'Service health: {health}')"
    echo "    asyncio.run(test())\""
    echo ""
    
    print_success "🚀 Your $domain service is ready for customization!"
    echo ""
}

# Main execution
main() {
    local domain="$1"
    
    # Show header
    echo ""
    echo "🏗️  Service Template Generator"
    echo "================================="
    echo ""
    
    # Validate input
    validate_domain_name "$domain"
    
    # Check if templates directory exists
    if [[ ! -d "$TEMPLATES_DIR" ]]; then
        print_error "Templates directory not found: $TEMPLATES_DIR"
        exit 1
    fi
    
    # Generate service components
    create_directory_structure "$domain"
    generate_init_files "$domain"
    generate_service_interface "$domain"
    generate_service_implementation "$domain"
    generate_api_router "$domain"
    generate_service_container "$domain"
    generate_test_templates "$domain"
    generate_documentation "$domain"
    
    # Show summary
    generate_summary_report "$domain"
}

# Run main function with all arguments
main "$@"