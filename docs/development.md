# Development Guidelines

## Testing and Verification Approach

### Never Assume - Always Verify
- **DO NOT** claim that fixes "should work" or "would work" without actual testing
- **ALWAYS** verify functionality through manual testing or direct observation
- **REQUIRE** green GitHub Actions confirmation before claiming CI/CD fixes are complete

### Dependency Management Best Practices

#### When Fixing Missing Dependencies
1. **Identify the root cause** - trace the import chain to find where the missing dependency is used
2. **Add to requirements.txt** - include the specific version to ensure reproducibility
3. **Make imports optional** - wrap in try/catch blocks with graceful fallbacks when appropriate
4. **Test locally first** - verify the fix works in the local development environment
5. **Verify with actual CI/CD run** - wait for and confirm green GitHub Actions status

#### Comprehensive Dependency Testing
When fixing CI/CD dependency issues, test the complete scenario:
```bash
# Test all dependencies are importable
python -c "import module_name"

# Test the actual failing test case
pytest path/to/failing/test.py::TestClass::test_method -v

# Test the full import chain that caused the failure
python -c "import the.full.chain.that.failed"
```

#### GitHub Actions Dependency Resolution Pattern
Recent fixes have addressed:
- **PyTorch**: Made optional with graceful fallback to numpy/pandas
- **PyArrow**: Added `pyarrow==18.1.0` and made optional across modules
- **Protocol Buffers**: Added `protobuf==5.29.2` for Google protobuf support
- **ib_insync**: Added `ib_insync==0.9.86` for Interactive Brokers integration

### Verification Requirements
- **Local Testing**: All fixes must pass local test execution before pushing
- **CI/CD Monitoring**: Must wait for and observe green GitHub Actions status
- **Error Reproduction**: Reproduce the exact error scenario before claiming it's fixed
- **Comprehensive Coverage**: Test not just the immediate fix but the entire use case

### Documentation of Fixes
When documenting dependency fixes:
- ✅ "Fixed and verified locally with passing tests"
- ✅ "Confirmed working through GitHub Actions green status"
- ❌ "This should work now" (without verification)
- ❌ "The fix would resolve the issue" (without testing)

## Development Workflow

1. **Identify Issue** - Through actual error observation (logs, test failures, etc.)
2. **Root Cause Analysis** - Trace the complete chain causing the problem
3. **Implement Fix** - Make targeted changes with appropriate fallbacks
4. **Local Verification** - Test the fix in the same environment where issue occurred
5. **Deploy and Monitor** - Push changes and wait for CI/CD confirmation
6. **Document Results** - Record what was actually observed, not assumptions

This approach ensures reliable, verified solutions rather than assumptions that may fail in production environments.