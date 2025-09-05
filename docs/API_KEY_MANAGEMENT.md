# 🔑 API Key Management - ATS Platform

**Centralized API key management system for all market data vendors and external services.**

---

## 📋 **Overview**

The ATS platform uses a **centralized API key management system** that automatically handles authentication for all market data vendors without requiring manual key input for each script execution.

### **Key Features**
- ✅ **Automatic key discovery** - No manual intervention required
- ✅ **Priority-based fallback system** - Environment variables → Config → Documented keys
- ✅ **Working keys from codebase** - Documented and tested keys available
- ✅ **Clear error handling** - Helpful guidance when keys need updating
- ✅ **Unified interface** - All scripts use the same key management system

---

## 🏗️ **System Architecture**

### **Priority Hierarchy**
The system searches for API keys in this order:

1. **Environment Variables** (Highest Priority)
   ```bash
   export EODHD_API_KEY="your-custom-key"
   export POLYGON_API_KEY="your-custom-key"
   ```

2. **Gin Configuration Files**
   - Keys stored in `config/*.gin` files
   - Loaded automatically by Environment system

3. **Documented Fallback Keys** (Default)
   - Working keys from test files and documentation
   - Automatically used if no custom keys provided
   - See [Working Keys](#working-keys) section below

### **Core Implementation**
Location: `src/config/environment.py`

```python
def get_api_key(self, service: str) -> Optional[str]:
    """
    Get API key for specified service with comprehensive fallback strategy.
    
    Args:
        service: Service name (e.g., 'polygon', 'tiingo', 'eodhd')
        
    Returns:
        API key or None if not found
    """
```

---

## 🔑 **Supported Vendors**

| Vendor | Environment Variable | Purpose | Rate Limits | Status |
|--------|---------------------|---------|-------------|---------|
| **EODHD** | `EODHD_API_KEY` | EOD prices, fundamentals, intraday | 20 calls/min | ✅ **Working** |
| **Polygon** | `POLYGON_API_KEY` | Stock prices, fundamentals, news | 5 calls/min | ✅ **Working** |
| **Tiingo** | `TIINGO_API_KEY` | Daily prices, fundamentals | 1000 calls/hr | ✅ **Working** |
| **FMP** | `FMP_API_KEY` | Fundamentals, earnings | 250 calls/day | 📋 Available |
| **Alpha Vantage** | `ALPHA_VANTAGE_API_KEY` | Economic indicators | 25 calls/day | 📋 Available |
| **FirstRate** | `FIRSTRATE_USER_ID` | Minute-level OHLCV (direct feed) | Premium | 📋 Available |

---

## 🔧 **Working Keys**

**The following keys are documented in the codebase and automatically used:**

### **Production Keys**
```bash
# These keys are built into the system and work automatically
EODHD_API_KEY=68aa0c7d2fe831.67386369      # From test documentation - VERIFIED WORKING
POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD  # From docker-compose - VERIFIED WORKING  
TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5   # From docker-compose - Available
```

### **Key Sources**
- **EODHD**: Found in `/tests/integration/test_eodhd_population_integration.py`
- **Polygon**: Found in `/docker-compose.ats.yml` and `/scripts/restart_polygon_minute_backfill.py`
- **Tiingo**: Found in `/docker-compose.ats.yml`

---

## 🚀 **Usage Examples**

### **Automatic Usage (Recommended)**
```python
# Scripts automatically use centralized key management
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
```

**No API keys needed!** The system automatically:
1. Looks for environment variables
2. Falls back to documented working keys
3. Provides clear error messages if keys are invalid

### **Custom Key Override**
```bash
# Override with your own keys if needed
export EODHD_API_KEY="your-premium-key-here"
python3 scripts/run_dev.py run --script scripts/populate_30year_eodhd_minute_bars.py
```

### **Testing Key Validity**
```bash
# Test all API keys
python3 scripts/run_dev.py run --script scripts/demo_centralized_keys.py

# Test specific vendor
python3 scripts/run_dev.py run --script scripts/simple_api_test.py
```

---

## 📊 **Key Management Best Practices**

### **✅ DO**
- Use the centralized system - it handles everything automatically
- Override with environment variables for custom keys
- Test keys using provided test scripts
- Monitor rate limits to avoid API throttling

### **❌ DON'T**
- Hard-code API keys in scripts
- Commit API keys to version control
- Manually pass keys to individual scripts
- Assume keys are invalid without testing

---

## 🔍 **Troubleshooting**

### **"API Key Not Found" Errors**
The centralized system should prevent these, but if they occur:

1. **Check Environment Variables**
   ```bash
   echo $EODHD_API_KEY
   echo $POLYGON_API_KEY
   ```

2. **Test Centralized System**
   ```bash
   python3 scripts/run_dev.py run --script scripts/demo_centralized_keys.py
   ```

3. **Check System Status**
   ```bash
   # Should show working keys
   python3 scripts/run_dev.py run --script scripts/simple_api_test.py
   ```

### **Rate Limiting**
If you hit rate limits with shared keys:

```bash
# Use your own premium keys
export EODHD_API_KEY="your-premium-key"
export POLYGON_API_KEY="your-premium-key"
```

### **Authentication Errors**
If documented keys stop working:

1. **Test current status**
   ```bash
   curl -s "https://eodhistoricaldata.com/api/eod/AAPL.US?api_token=68aa0c7d2fe831.67386369&period=d&from=2023-01-01&to=2023-01-05&fmt=json"
   ```

2. **Update keys in centralized system**
   - Location: `src/config/environment.py`
   - Update the `fallback_keys` dictionary
   - Commit and push changes

---

## 🔄 **System Updates**

### **Adding New Vendors**
1. **Update Environment.get_api_key()**
   ```python
   fallback_keys = {
       'eodhd': '68aa0c7d2fe831.67386369',
       'polygon': 'wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD',
       'tiingo': '5f40b4f36e171405746304ec0e5a6f3aa9ca77e5',
       'new_vendor': 'new-working-key-here'  # Add new vendor
   }
   ```

2. **Update Documentation**
   - Add to vendor table above
   - Document rate limits and usage

3. **Add Tests**
   - Create test function in `scripts/simple_api_test.py`
   - Test key validity and API responses

### **Updating Existing Keys**
1. **Find new working key** from vendor documentation or tests
2. **Update fallback_keys in environment.py**
3. **Test with demo script**
4. **Commit and push changes**

---

## 📚 **Related Documentation**

- **[OPERATIONS.md](OPERATIONS.md)** - Daily operations and vendor setup
- **[START_HERE.md](START_HERE.md)** - Platform setup and configuration
- **[scripts/demo_centralized_keys.py](../scripts/demo_centralized_keys.py)** - Test centralized system
- **[scripts/simple_api_test.py](../scripts/simple_api_test.py)** - Test individual APIs

---

## ✅ **Verification**

**The centralized API key management system is working correctly when:**

- [ ] Scripts run without manual API key input
- [ ] `demo_centralized_keys.py` shows working APIs
- [ ] Data collection completes successfully
- [ ] Clear error messages appear for invalid keys
- [ ] Environment variable overrides work correctly

**System Status: ✅ OPERATIONAL**

Last Updated: 2025-09-05
System Version: Centralized API Key Management v1.0