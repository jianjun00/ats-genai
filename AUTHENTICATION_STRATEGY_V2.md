# Portfolio GPT Authentication Strategy v2.0
**Updated**: Google OAuth2 + User-based API Keys

## Why Google Authentication?

### User Experience Benefits
- **One-click signup**: Users already have Google accounts
- **Trusted provider**: Users comfortable with Google security
- **No password management**: Eliminates password reset flows
- **Profile information**: Get name, email, profile picture automatically

### Product Benefits
- **Faster onboarding**: Reduce signup friction from minutes to seconds
- **Higher conversion**: OAuth typically sees 30-50% higher conversion rates
- **User identification**: Link subscriptions to real users, not anonymous API keys
- **Support capabilities**: Can provide user-specific support

### Technical Benefits
- **Security**: Leverage Google's security infrastructure
- **Compliance**: Google handles GDPR, privacy regulations
- **Scalability**: No user credential management on our side
- **Integration**: Easy integration with Google Cloud services

## Revised Authentication Architecture

### Flow Overview
```
1. User visits dashboard → "Sign in with Google"
2. Google OAuth2 flow → User grants permission
3. Backend receives Google token → Validates with Google
4. Create/update user account → Generate user-specific API key
5. Return dashboard session + API key for integrations
```

### Components

#### 1. User Account System
- **Users table**: Store Google user info and subscription status
- **Sessions table**: Web dashboard authentication
- **API Keys table**: User-generated keys for API access

#### 2. Authentication Methods
- **Web Dashboard**: Google OAuth2 with session cookies
- **API Access**: User-generated API keys (linked to user account)
- **Admin Access**: Separate admin system (can keep simple API keys)

#### 3. Subscription Management
- **Free Tier**: Automatic for all Google sign-ups
- **Premium Tier**: Stripe integration linked to Google user
- **Usage Tracking**: Per-user instead of per-key

## Updated Database Schema

### Users Table
```sql
CREATE TABLE {env}_users (
    id SERIAL PRIMARY KEY,
    google_id VARCHAR(255) UNIQUE NOT NULL,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255),
    picture_url TEXT,
    subscription_tier VARCHAR(20) DEFAULT 'free' CHECK (tier IN ('free', 'premium')),
    subscription_status VARCHAR(20) DEFAULT 'active',
    stripe_customer_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_login_at TIMESTAMP
);
```

### Sessions Table (for dashboard)
```sql
CREATE TABLE {env}_user_sessions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES {env}_users(id) ON DELETE CASCADE,
    session_token VARCHAR(255) UNIQUE NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP DEFAULT NOW()
);
```

### Updated API Keys Table
```sql
CREATE TABLE {env}_api_keys (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES {env}_users(id) ON DELETE CASCADE,
    key_hash VARCHAR(255) UNIQUE NOT NULL,
    key_prefix VARCHAR(8) NOT NULL,
    name VARCHAR(100) NOT NULL, -- User-provided name
    description TEXT,
    permissions JSONB DEFAULT '{"read": true}', -- Future: granular permissions
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP,
    expires_at TIMESTAMP
);
```

## User Experience Flow

### New User Journey
1. **Landing page** → "Get AI Stock Recommendations"
2. **Sign in with Google** → OAuth2 consent screen
3. **Dashboard** → Welcome, here are your first 5 free recommendations
4. **API key generation** → "Integrate with your apps" section
5. **Upgrade prompt** → When hitting free tier limits

### Returning User Journey
1. **Dashboard login** → Automatic Google sign-in
2. **View recommendations** → Latest hourly updates
3. **Manage API keys** → Create/revoke keys with names
4. **Subscription management** → Upgrade/downgrade tiers

## Implementation Plan

### Phase 1: Google OAuth2 Integration (Week 1)
- [ ] Google Cloud Console setup (OAuth2 app)
- [ ] Backend Google token validation
- [ ] User account creation/update
- [ ] Dashboard session management

### Phase 2: User-based API Keys (Week 1-2)
- [ ] User-scoped API key generation
- [ ] API key management interface
- [ ] Usage tracking per user (not per key)
- [ ] Rate limiting based on user subscription

### Phase 3: Dashboard Integration (Week 2)
- [ ] React OAuth2 integration
- [ ] User profile management
- [ ] API key management UI
- [ ] Subscription status display

### Phase 4: Subscription System (Future)
- [ ] Stripe integration for premium subscriptions
- [ ] Billing management interface
- [ ] Usage-based billing options

## Technical Implementation

### Environment Variables Needed
```bash
# Google OAuth2
GOOGLE_CLIENT_ID=your-google-client-id
GOOGLE_CLIENT_SECRET=your-google-client-secret
GOOGLE_REDIRECT_URI=http://localhost:3000/auth/callback

# Session management
SESSION_SECRET_KEY=your-session-secret
SESSION_EXPIRE_HOURS=24

# API Configuration
API_BASE_URL=http://localhost:8000
FRONTEND_URL=http://localhost:3000
```

### Updated API Endpoints

#### Authentication Endpoints
```
GET  /api/v1/auth/google/login    # Redirect to Google OAuth2
GET  /api/v1/auth/google/callback # Handle OAuth2 callback
POST /api/v1/auth/logout          # Clear session
GET  /api/v1/auth/me              # Get current user info
```

#### API Key Management
```
GET    /api/v1/user/api-keys      # List user's API keys
POST   /api/v1/user/api-keys      # Create new API key
DELETE /api/v1/user/api-keys/{id} # Revoke API key
GET    /api/v1/user/usage         # Get usage statistics
```

#### Recommendations (Protected)
```
GET /api/v1/recommendations       # All user's recommendations
GET /api/v1/recommendations/{symbol} # Specific stock
```

## Security Considerations

### Google OAuth2 Security
- **Validate tokens**: Always verify Google tokens server-side
- **Secure storage**: Store minimal user data, reference by Google ID
- **Token refresh**: Handle expired tokens gracefully
- **Scope limitation**: Request minimal Google permissions

### API Key Security
- **User-scoped**: Keys belong to authenticated users
- **Named keys**: Users can identify and manage multiple keys
- **Granular permissions**: Future: read-only, write, admin scopes
- **Automatic expiration**: Optional expiration dates

### Session Security
- **Secure cookies**: httpOnly, secure, sameSite attributes
- **Session rotation**: Rotate tokens on sensitive operations
- **Logout everywhere**: Ability to revoke all sessions

## Migration from Current System

### Backward Compatibility
- Keep existing API key system for admin/internal use
- Gradually migrate to user-based system
- Support both authentication methods during transition

### Data Migration
- Convert existing API keys to admin-only keys
- No user data to migrate (starting fresh)
- Preserve usage analytics structure

This approach provides a much better user experience while maintaining the technical robustness needed for a B2C SaaS product.