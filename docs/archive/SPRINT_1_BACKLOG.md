# Sprint 1 Backlog - Foundation & Authentication
**Duration**: Weeks 1-2  
**Sprint Goal**: Establish core authentication system and database foundation for Portfolio GPT MVP

## Epic 1: Subscription & Authentication System
**Epic Owner**: Backend Team  
**Priority**: Critical  
**Story Points**: 21

### User Story 1.1: API Key Generation & Management
**Story Points**: 5  
**Assignee**: Backend Engineer  
**Priority**: Critical

**User Story:**
As a system administrator  
I want to generate and manage API keys for different subscription tiers  
So that I can control access to the recommendation system

**Acceptance Criteria:**
- [ ] Create API key generation endpoint `/api/v1/auth/generate-key`
- [ ] Support two tiers: `free` and `premium` with different permissions
- [ ] API keys are 32-character alphanumeric strings
- [ ] Store API keys securely with bcrypt hashing
- [ ] Include creation date, tier, and status (active/inactive)
- [ ] Admin endpoint to list/revoke API keys

**Technical Notes:**
- Extend existing FastAPI app in `src/main.py`
- New module: `src/auth/api_key_manager.py`
- Database table: `api_keys` with proper indexing
- Environment variable for admin secret key

**Dependencies:**
- Database schema setup (Story 1.4)

**Test Scenarios:**
- Generate keys for both tiers
- Duplicate key generation handling
- Key revocation and status updates

---

### User Story 1.2: API Authentication Middleware
**Story Points**: 8  
**Assignee**: Backend Engineer  
**Priority**: Critical

**User Story:**
As a developer  
I want API requests to be authenticated via API keys  
So that only authorized users can access recommendations

**Acceptance Criteria:**
- [ ] Authentication middleware validates API keys on protected endpoints
- [ ] Extract API key from `Authorization: Bearer <key>` header
- [ ] Return 401 for invalid/missing keys, 403 for inactive keys
- [ ] Inject user context (tier, permissions) into request
- [ ] Log authentication attempts for security monitoring
- [ ] Rate limiting based on tier: Free (24/day), Premium (unlimited)

**Technical Notes:**
- FastAPI dependency injection for authentication
- Redis for rate limiting (or in-memory for MVP)
- Custom exception handlers for auth errors
- Middleware integration in existing FastAPI app

**Dependencies:**
- API Key Generation (Story 1.1)
- Rate limiting infrastructure (Story 1.3)

**Test Scenarios:**
- Valid key authentication
- Invalid/expired key rejection
- Rate limiting enforcement
- Header format validation

---

### User Story 1.3: Rate Limiting & Usage Tracking
**Story Points**: 5  
**Assignee**: Backend Engineer  
**Priority**: High

**User Story:**
As a product manager  
I want to enforce rate limits and track API usage  
So that I can manage costs and ensure fair usage

**Acceptance Criteria:**
- [ ] Free tier limited to 24 requests per day
- [ ] Premium tier has no rate limits
- [ ] Usage tracking stored per API key with timestamps
- [ ] Rate limit headers returned: `X-RateLimit-Limit`, `X-RateLimit-Remaining`
- [ ] 429 status code when rate limit exceeded
- [ ] Daily usage reset at midnight UTC

**Technical Notes:**
- Redis for distributed rate limiting (or in-memory cache for MVP)
- Background job for daily usage reset
- Usage analytics table for reporting
- Configurable rate limits via environment variables

**Dependencies:**
- API Authentication Middleware (Story 1.2)

**Test Scenarios:**
- Rate limit enforcement for free tier
- Unlimited access for premium tier
- Usage counter accuracy
- Rate limit reset functionality

---

### User Story 1.4: Database Schema for Authentication
**Story Points**: 3  
**Assignee**: Data Engineer  
**Priority**: Critical

**User Story:**
As a backend developer  
I want database tables for authentication and usage tracking  
So that I can store user data and API keys securely

**Acceptance Criteria:**
- [ ] Create `api_keys` table with proper schema
- [ ] Create `api_usage` table for tracking requests
- [ ] Add database migration script
- [ ] Indexes on frequently queried columns
- [ ] Foreign key constraints for data integrity
- [ ] Environment-aware table naming (dev/intg/prod prefixes)

**Technical Notes:**
- Extend existing migration system in `src/db/migrations/`
- New migration: `028_create_auth_tables.sql`
- Use existing environment-aware naming from `src/config/environment.py`
- Include proper data types and constraints

**Database Schema:**
```sql
-- API Keys table
CREATE TABLE {env}_api_keys (
    id SERIAL PRIMARY KEY,
    key_hash VARCHAR(255) UNIQUE NOT NULL,
    tier VARCHAR(20) NOT NULL CHECK (tier IN ('free', 'premium')),
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,
    description TEXT
);

-- API Usage tracking
CREATE TABLE {env}_api_usage (
    id SERIAL PRIMARY KEY,
    api_key_id INTEGER REFERENCES {env}_api_keys(id),
    endpoint VARCHAR(255) NOT NULL,
    request_count INTEGER DEFAULT 1,
    date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(api_key_id, endpoint, date)
);
```

**Dependencies:**
- None (foundational)

**Test Scenarios:**
- Migration runs successfully in all environments
- Table creation with proper constraints
- Index performance on key lookups

---

## Epic 2: Core Infrastructure Setup
**Epic Owner**: DevOps Team  
**Priority**: High  
**Story Points**: 13

### User Story 2.1: Extend FastAPI for Recommendations
**Story Points**: 5  
**Assignee**: Backend Engineer  
**Priority**: High

**User Story:**
As a developer  
I want the FastAPI application extended for recommendation endpoints  
So that the frontend can retrieve stock recommendations

**Acceptance Criteria:**
- [ ] New router module: `src/recommendations/api.py`
- [ ] Endpoint: `GET /api/v1/recommendations/{symbol}` (protected)
- [ ] Endpoint: `GET /api/v1/recommendations` with query params (protected)
- [ ] Response includes: symbol, recommendation, confidence, forecast, timestamp
- [ ] Error handling for invalid symbols or missing data
- [ ] OpenAPI documentation updated

**Technical Notes:**
- Follow existing FastAPI patterns in `src/main.py`
- Integration with authentication middleware
- Response models using Pydantic
- Proper HTTP status codes

**Dependencies:**
- Authentication middleware (Story 1.2)

**Test Scenarios:**
- Authenticated requests return recommendations
- Unauthenticated requests rejected
- Invalid symbol handling
- API documentation accuracy

---

### User Story 2.2: Database Schema for Recommendations
**Story Points**: 5  
**Assignee**: Data Engineer  
**Priority**: High

**User Story:**
As a backend developer  
I want database tables to store forecasts and recommendations  
So that the API can serve historical and current predictions

**Acceptance Criteria:**
- [ ] Create `forecasts` table for model predictions
- [ ] Create `recommendations` table for buy/hold/sell decisions
- [ ] Include confidence scores and timestamp tracking
- [ ] Efficient queries for latest recommendations per symbol
- [ ] Partitioning strategy for time-series data optimization

**Technical Notes:**
- New migration: `029_create_recommendation_tables.sql`
- TimescaleDB optimization for time-series data
- Indexes for symbol and timestamp queries
- Environment-aware table naming

**Database Schema:**
```sql
-- Forecasts table
CREATE TABLE {env}_forecasts (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    forecast_date TIMESTAMP NOT NULL,
    horizon_days INTEGER NOT NULL,
    predicted_price DECIMAL(10,2) NOT NULL,
    confidence_score DECIMAL(5,2) NOT NULL,
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Recommendations table  
CREATE TABLE {env}_recommendations (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    recommendation VARCHAR(10) NOT NULL CHECK (recommendation IN ('buy', 'hold', 'sell')),
    confidence_score DECIMAL(5,2) NOT NULL,
    reasoning TEXT,
    valid_until TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**Dependencies:**
- None (foundational)

---

### User Story 2.3: Mock Data Service for Development
**Story Points**: 3  
**Assignee**: Backend Engineer  
**Priority**: Medium

**User Story:**
As a developer  
I want mock recommendation data for testing  
So that I can develop and test the API without waiting for the ML model

**Acceptance Criteria:**
- [ ] Mock service generates realistic recommendation data
- [ ] Support for 50 predefined stock symbols
- [ ] Random but consistent recommendations with confidence scores
- [ ] Database seeding script for development environment
- [ ] Toggle between mock and real data via environment variable

**Technical Notes:**
- New module: `src/recommendations/mock_service.py`
- Seed data script: `scripts/database/seed_mock_recommendations.py`
- Environment variable: `USE_MOCK_DATA=true/false`

**Dependencies:**
- Database schema (Story 2.2)

---

## Sprint 1 Success Criteria

### Definition of Done
- [ ] All user stories meet acceptance criteria
- [ ] Code reviewed and approved by 2+ team members
- [ ] Unit tests written with >80% coverage for new code
- [ ] Integration tests pass in dev environment
- [ ] API documentation updated and accurate
- [ ] Database migrations tested in all environments
- [ ] Security review completed for authentication system

### Sprint 1 Demo Goals
- [ ] Generate API keys for both tiers
- [ ] Authenticate API requests successfully
- [ ] Rate limiting enforced correctly
- [ ] Mock recommendations served via API
- [ ] Database schema deployed to dev environment

### Key Performance Indicators
- **Velocity**: Complete 21+ story points
- **Quality**: Zero critical bugs in authentication system
- **Performance**: API response time <200ms for recommendation endpoints
- **Coverage**: >80% test coverage for authentication module

## Risk Mitigation

### High-Risk Items
1. **Authentication Security**: Code review by security-focused team member
2. **Database Performance**: Load testing with expected API call volume
3. **Rate Limiting Accuracy**: Edge case testing around tier limits

### Contingency Plans
- **Authentication delays**: Simplified API key system without advanced features
- **Database issues**: Use SQLite for development, PostgreSQL for staging/prod
- **Integration problems**: Manual testing if automated tests fail

## Team Assignments

### Backend Engineer (Primary)
- Stories 1.1, 1.2, 1.3, 2.1, 2.3
- Focus on authentication system and API development

### Data Engineer (Primary)  
- Stories 1.4, 2.2
- Focus on database schema and migration system

### DevOps Engineer (Support)
- Environment setup and deployment pipeline
- Testing infrastructure and CI/CD integration

This Sprint 1 backlog establishes the foundation for the entire Portfolio GPT MVP with clear ownership, dependencies, and success criteria.