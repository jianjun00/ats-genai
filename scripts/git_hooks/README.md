# Git Hooks for ATS Project

This directory contains git hooks to maintain code quality and enforce naming conventions.

## Installation

Run the install script to set up all hooks:

```bash
./scripts/git_hooks/install-hooks.sh
```

## Available Hooks

### Pre-commit Hook

The pre-commit hook prevents commits that violate project standards:

#### 1. Naming Pattern Validation
Blocks files with these patterns that indicate temporary or generic implementations:
- `unified` (e.g., `unified_analytics_service.py`)
- `simple` (e.g., `simple_data_loader.py`)
- `enhanced` (e.g., `enhanced_validator.py`)

**Why:** These names suggest:
- Temporary implementations that should be properly named
- Generic solutions that lack specific purpose
- Code that hasn't been properly integrated

**Better naming examples:**
- ✅ `portfolio_analytics_service.py` instead of ❌ `unified_analytics_service.py`
- ✅ `market_data_loader.py` instead of ❌ `simple_data_loader.py`
- ✅ `schema_validator.py` instead of ❌ `enhanced_validator.py`

#### 2. Python Content Validation
Blocks Python files containing these patterns outside of test files:
- `fake` (e.g., `generate_fake_data()`, `fake_response`)
- `mock` (e.g., `mock_data()`, `return mock_response()`)
- `synthetic` (e.g., `synthetic_data()`, `create_synthetic()`)
- `fallback` (e.g., `fallback_data()`, `fallback_response()`)

**Why:** Enforces the ATS platform's "REAL DATA ONLY" policy:
- Prevents hiding database connection problems and query failures
- Avoids masking data quality issues and real-world edge cases
- Eliminates false performance metrics from fast demo data
- Ensures authentication and network issues are detected
- Prevents production surprises when real data behaves differently

**Better approaches:**
- ✅ `if data.empty: raise ValueError("No real market data available")` instead of ❌ `if data.empty: data = generate_fake_data()`
- ✅ `except ConnectionError: logger.error("Database failed"); raise` instead of ❌ `except ConnectionError: return mock_response()`
- ✅ `data = load_real_data(); validate_data_exists(data)` instead of ❌ `data = load_real_data() or fallback_synthetic_data()`

**Test File Exemption:** Mock/fake data IS allowed in test files:
- `tests/*`, `*test*`, `test_*`, `*_test.py` files are exempt
- Use mocks freely for isolated unit testing

#### 3. Non-ASCII Filename Check
Prevents non-ASCII characters in filenames for cross-platform compatibility.

#### 4. Whitespace Error Check
Catches trailing whitespace and other whitespace issues.

## Bypassing Hooks

If you need to commit files that trigger the naming check (e.g., for legitimate reasons), you can bypass the hook:

```bash
git commit --no-verify -m "commit message"
```

## Testing the Hook

You can test the different validations:

**Test naming validation:**
```bash
echo "test" > unified_service.py
git add unified_service.py
git commit -m "test"  # Blocked: prohibited naming pattern
```

**Test content validation:**
```bash
echo "def load_data(): return generate_fake_data()" > data_loader.py
git add data_loader.py
git commit -m "test"  # Blocked: prohibited content pattern
```

**Test that test files are exempt:**
```bash
echo "def test_mock(): return mock_data()" > tests/test_example.py
git add tests/test_example.py
git commit -m "test"  # Allowed: test file can use mock data
```

## Manual Installation

If the install script doesn't work, you can manually copy the pre-commit hook:

```bash
cp scripts/git_hooks/pre-commit-naming-check.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Development

The hook logic is maintained in this directory and can be version controlled, unlike hooks in `.git/hooks/` which are not tracked by git.