# 🔑 ATS API Reference

**Complete API keys, vendor monitoring, and endpoint reference for the ATS platform.**

---

## 🚨 API Key Management - Single Source of Truth

### CRITICAL: All API keys MUST use these exact values from .env.test (git commit 5168e8e83)

**✅ VERIFIED WORKING API KEYS (Last validated: 2025-09-11):**
```bash
# These are the ONLY valid API keys - use these exact values everywhere
POLYGON_API_KEY="wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"      # ✅ VERIFIED WORKING
TIINGO_API_KEY="5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"    # ✅ VERIFIED WORKING  
EODHD_API_KEY="68aa0c7d2fe831.67386369"                   # ✅ VERIFIED WORKING
FMP_API_KEY="Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr"            # ✅ Available
ALPHA_VANTAGE_API_KEY="9GI0NZ3V4VNFX271"                  # ✅ Available
```

### API Key Validation - MANDATORY Before Service Deployment

**ALWAYS validate API keys before deploying services:**
```bash
# Validate all API keys (uses correct keys automatically)
python3 scripts/validate_api_keys.py

# Expected output: ✅ All API keys are valid!
```

### Service Integration - Centralized Management

**All services automatically use correct API keys via run_intg.py and run_dev.py:**

#### Integration Environment Services:
```bash
# All integration services use centralized API key management
python3 scripts/run_intg.py start --service realtime-minute-collector
python3 scripts/run_intg.py start --service analytics  
python3 scripts/run_intg.py start --service news-realtime

# API keys automatically configured with correct values
```

#### Development Environment Services:
```bash
# All dev services use centralized API key management  
python3 scripts/run_dev.py start --service analytics
python3 scripts/run_dev.py start --service postgres

# API keys automatically configured with correct values
```

### ZERO TOLERANCE FOR API KEY INCONSISTENCIES

**❌ FORBIDDEN - These patterns cause authentication failures:**
- Using old/wrong API key values (like `675b5a33b36f43.67825763`)
- Manual API key configuration in containers
- Different keys across different services
- Hard-coding keys in individual scripts
- Creating new containers with different keys

**✅ REQUIRED - Consistent API Key Management:**
- ALL services use run_intg.py and run_dev.py service definitions
- ALL services inherit the same centralized API key configuration
- ALL API keys default to verified working values from .env.test
- Environment variables can override defaults when needed

### Troubleshooting API Key Issues

#### Step 1: Validate Current Keys
```bash
# Test all API keys
python3 scripts/validate_api_keys.py

# Test specific vendor directly
curl -s "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2025-09-11/2025-09-11?adjusted=true&sort=asc&limit=1&apikey=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
```

#### Step 2: Check Service Configuration
```bash
# Verify service has correct API keys
docker inspect <service_name> | grep -A 10 "Env"

# Should show: POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD
# Should show: EODHD_API_KEY=68aa0c7d2fe831.67386369
# Should show: TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5
```

#### Step 3: Fix Authentication Failures
```bash
# If service shows 401/403 errors, restart with correct keys
docker stop <service_name>
docker rm <service_name>

# Use run_intg.py or run_dev.py to ensure correct configuration
python3 scripts/run_intg.py start --service <service_name>
```

### SERVICE-SPECIFIC API KEY REQUIREMENTS

| Service | Required Keys | Verification Command |
|---------|---------------|---------------------|
| **realtime-minute-collector** | POLYGON, TIINGO, EODHD | `docker logs ats-intg-realtime-minute-collector` |
| **news-realtime** | POLYGON, TIINGO, EODHD | `docker logs ats-intg-news-realtime` |
| **analytics** | All keys (fallback) | `curl http://localhost:4000/health` |

### API Key Emergency Recovery

**If ALL API keys fail and services are down:**

#### Step 1: Immediate Recovery
```bash
# Stop all failing services
python3 scripts/run_intg.py stop --service realtime-minute-collector
python3 scripts/run_intg.py stop --service news-realtime

# Validate keys are still working
python3 scripts/validate_api_keys.py
```

#### Step 2: Root Cause Analysis
```bash
# Check git history for .env.test changes
git log --oneline -p -- .env.test | head -20

# Verify correct keys from specific commit
git show 5168e8e83:.env.test | grep -E "(POLYGON|TIINGO|EODHD)_API_KEY"
```

#### Step 3: System Recovery
```bash
# Update run_intg.py with correct keys if needed
# Restart services with verified configuration
python3 scripts/run_intg.py start --service realtime-minute-collector

# Verify recovery
python3 scripts/run_intg.py status
```

### Monitoring API Key Health

#### Continuous Monitoring:
```bash
# Daily API key health check
python3 scripts/validate_api_keys.py > /tmp/api_key_health.log

# Check service logs for authentication errors
docker logs ats-intg-realtime-minute-collector --tail 50 | grep -E "(401|403|auth|key)"
```

#### Rate Limiting Monitoring:
```bash
# Check for rate limit errors (sign of successful authentication)
docker logs ats-intg-realtime-minute-collector | grep -E "(429|rate.*limit)"

# Monitor API call frequency
python3 scripts/run_intg.py query --query "SELECT vendor, COUNT(*) FROM intg_minute_bar_api_calls WHERE created_at > NOW() - INTERVAL '1 hour' GROUP BY vendor"
```

### Security Best Practices

#### ✅ SECURE API Key Management:
- Use environment variables to override defaults when needed
- Never commit new API keys to version control
- Use the validation script before any service deployment
- Monitor logs for authentication failures
- Rotate keys proactively if possible

#### ❌ SECURITY VIOLATIONS:
- Hard-coding different API keys in source code
- Using untested/invalid API keys in production
- Ignoring authentication failures without root cause analysis
- Creating manual workarounds that bypass centralized management

---

## 🌐 API Endpoints Reference

### ATS-DEV Environment (Development)
```bash
# Analytics Service
http://localhost:3000/health          # Health check endpoint
http://localhost:3000/eda            # EDA Dashboard interface
http://localhost:3000/api/           # Analytics API endpoints

# API Service  
http://localhost:8000/health          # API health check
http://localhost:8000/api/           # Main API endpoints

# Database
postgresql://postgres:dev_password@localhost:3432/dev_db
```

### ATS-INTG Environment (Integration Testing)
```bash
# Analytics Service
http://localhost:4000/health          # Health check endpoint
http://localhost:4000/eda            # EDA Dashboard interface
http://localhost:4000/api/           # Analytics API endpoints

# Monitoring & Metrics
http://localhost:4080/health          # Prometheus metrics health
http://localhost:4080/metrics        # Prometheus metrics endpoint
http://localhost:4002/               # Grafana dashboards (admin/admin)
http://localhost:4091/-/ready        # Prometheus server ready check

# Database
postgresql://postgres:intg_password@localhost:4432/intg_db
```

### How to Find Service Endpoints
```bash
# Check running services and their ports
python scripts/run_dev.py status                    # ATS-DEV services
docker ps | grep -E "(ats-dev|intg)"               # Container status with ports

# Get external access info for any service
./scripts/get_external_access.sh service-name      # Service endpoint discovery script

# Test endpoints are working
curl -f http://localhost:3000/health               # ATS-DEV analytics
curl -f http://localhost:4000/health               # ATS-INTG analytics
curl -f http://localhost:4080/health               # ATS-INTG metrics
```

### Common API Usage Patterns
```bash
# Health checks (always test these first)
curl -s http://localhost:3000/health | jq
curl -s http://localhost:4000/health | jq

# Analytics API example
curl -s http://localhost:3000/api/datasets | jq

# Metrics collection
curl -s http://localhost:4080/metrics | grep "ats_"

# Dashboard access (open in browser)
open http://localhost:3000/eda                     # EDA interface
open http://localhost:4002/                        # Grafana dashboards (admin/admin)

# Real-time minute bar monitoring
open http://localhost:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f/ats-vendor-monitoring-dashboard-fixed
```

---

## 📊 Vendor Monitoring & Dashboards

### Comprehensive Vendor API and Data Collection Monitoring

**🎯 Primary Dashboard**
```bash
# Professional Grafana dashboards (recommended)
http://localhost:4002/d/cb0f07fd-9f56-486e-8cd6-7c9893e63116/ats-vendor-monitoring-dashboard-postgresql  # Main vendor dashboard
http://localhost:4002                                                                                        # Grafana home (admin/admin)
```

### SigNoz Observability Platform:
```bash
🌐 URL: http://10.0.0.79:4000
📊 Real-time collector traces and metrics
🔧 Service performance monitoring
📈 Error tracking and alerting
🚀 OpenTelemetry integration
```

**Grafana Vendor Monitoring:**
```bash
🌐 URL: http://10.0.0.79:4002/d/f9afe708-9be9-4c39-b901-f5c43a0a479f/ats-vendor-monitoring-dashboard-fixed
📊 Login: admin/admin (change on first login)
🔧 Data Source: ATS-INTG-PostgreSQL (172.17.0.1:4432/intg_db) + Prometheus metrics
📈 Auto-refresh: 30s intervals
```

### Monitoring Capabilities
- **Minute Bar Collection per Vendor**: Real-time collection rates by vendor/symbol
- **API Calls per Vendor with Status Codes**: 200, 429, 500 response breakdown
- **Vendor Health Monitoring**: Success rates, response times, rate limits
- **Error Tracking**: Recent API failures with detailed error messages
- **Data Quality Metrics**: Collection success rates and data quality scores

### Backend Services
```bash
# Prometheus metrics (feeds Grafana)
http://localhost:8091/metrics                    # Vendor performance metrics

# Database tables for direct queries
intg_api_calls                                   # API call tracking
intg_minute_bar_collection_metrics               # Collection performance
intg_vendor_api_health                          # Periodic health summaries
```

### Prometheus Metrics Endpoints
```bash
# Main metrics server (feeds Grafana)
curl -s http://localhost:4080/metrics | grep "ats_daily_minute"

# Key minute bar metrics:
ats_daily_minute_backfill_instruments_processed    # Number of instruments processed
ats_daily_minute_backfill_total_minute_bars         # Total minute bars processed
ats_daily_minute_backfill_symbols_by_type{type="stock"}     # Stock symbols processed
ats_daily_minute_backfill_symbols_by_type{type="etf"}       # ETF symbols processed
ats_daily_minute_backfill_symbols_by_letter{letter="A"}     # Symbols by first letter

# Health check endpoint
curl -f http://localhost:4080/health
```

---

## 🔧 Market Data Vendor API Keys

### Market Data Vendor API Keys

**✅ AUTOMATED: Centralized API key management - no manual setup required.**

| Vendor | Environment Variable | Purpose | Rate Limits | Status |
|--------|---------------------|---------|-------------|---------|
| **EODHD** | `EODHD_API_KEY` | EOD prices, fundamentals, intraday | 20 calls/min | ✅ **Auto-configured** |
| **Polygon** | `POLYGON_API_KEY` | Stock prices, fundamentals, news | 5 calls/min | ✅ **Auto-configured** |
| **Tiingo** | `TIINGO_API_KEY` | Daily prices, fundamentals | 1000 calls/hr | ✅ **Auto-configured** |
| **FMP** | `FMP_API_KEY` | Fundamentals, earnings | 250 calls/day | 📋 Available |
| **Alpha Vantage** | `ALPHA_VANTAGE_API_KEY` | Economic indicators | 25 calls/day | 📋 Available |
| **FirstRate** | `FIRSTRATE_USER_ID` | Minute-level OHLCV (direct feed) | Premium | 📋 Available |

### API Key Usage (Automatic)
```bash
# ✅ VALIDATE keys before operations
python3 scripts/validate_api_keys.py

# ✅ NO SETUP NEEDED - Keys are managed automatically
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
python3 scripts/run_dev.py run --script scripts/tiingo_30_year_daily_backfill.py

# 🔧 Override with custom keys (optional)
export EODHD_API_KEY="your-premium-key"
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
```

---

## 🚨 Critical Anti-Patterns

### API Key Anti-Patterns:
- ❌ **DO NOT** use different API keys across services
- ❌ **DO NOT** hard-code API keys in source code
- ❌ **DO NOT** deploy services without validating keys first
- ❌ **DO NOT** ignore authentication failures
- ❌ **DO NOT** create manual API key configurations

### Endpoint Anti-Patterns:
- ❌ **DO NOT** assume endpoints work without testing
- ❌ **DO NOT** use port-forwarding for external access testing
- ❌ **DO NOT** skip health checks before using services
- ❌ **DO NOT** hardcode endpoint URLs

---

## 🎯 Success Criteria

**API Key management is correct when:**
- [ ] All API keys validated before service deployment
- [ ] Services use centralized API key configuration
- [ ] No authentication errors in service logs
- [ ] Rate limiting working as expected (429 errors when exceeded)
- [ ] All services can access required vendor APIs

**Endpoint management is correct when:**
- [ ] All health endpoints responding correctly
- [ ] External access tested (not just port-forwarding)
- [ ] Monitoring dashboards accessible
- [ ] Database connections working from all environments
- [ ] API responses contain expected data structures

---

**📋 For complete API key troubleshooting and emergency procedures, see CLAUDE.md - API Key Management section**
**📋 For operational monitoring procedures, see OPERATIONS.md**

*This API reference provides centralized API key management and endpoint monitoring for reliable ATS platform operations.*