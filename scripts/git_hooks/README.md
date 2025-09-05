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

#### 2. Non-ASCII Filename Check
Prevents non-ASCII characters in filenames for cross-platform compatibility.

#### 3. Whitespace Error Check
Catches trailing whitespace and other whitespace issues.

## Bypassing Hooks

If you need to commit files that trigger the naming check (e.g., for legitimate reasons), you can bypass the hook:

```bash
git commit --no-verify -m "commit message"
```

## Testing the Hook

You can test the hook by trying to add a file with a prohibited name:

```bash
echo "test" > test_unified_file.py
git add test_unified_file.py
git commit -m "test"  # This will be blocked
```

## Manual Installation

If the install script doesn't work, you can manually copy the pre-commit hook:

```bash
cp scripts/git_hooks/pre-commit-naming-check.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

## Development

The hook logic is maintained in this directory and can be version controlled, unlike hooks in `.git/hooks/` which are not tracked by git.