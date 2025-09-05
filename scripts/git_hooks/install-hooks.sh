#!/bin/bash
#
# Install git hooks for ATS project
# This script copies hook scripts from the tracked git_hooks directory
# to the actual .git/hooks directory where git will execute them.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
HOOKS_DIR="$PROJECT_ROOT/.git/hooks"

echo "🔧 Installing ATS git hooks..."

# Create the full pre-commit hook that combines all checks
cat > "$HOOKS_DIR/pre-commit" << 'EOF'
#!/bin/sh
#
# ATS Project Pre-commit Hook
#
# This hook runs multiple checks before allowing commits:
# 1. Naming pattern validation (unified, simple, enhanced)
# 2. Python content validation (fake, mock, synthetic, fallback)
# 3. Non-ASCII filename checks
# 4. Whitespace error checks
#

if git rev-parse --verify HEAD >/dev/null 2>&1
then
	against=HEAD
else
	# Initial commit: diff against an empty tree object
	against=$(git hash-object -t tree /dev/null)
fi

# Redirect output to stderr
exec 1>&2

# 1. Check for files with prohibited naming patterns
prohibited_patterns="unified|simple|enhanced"
problem_files=$(git diff --cached --name-only --diff-filter=A $against | grep -iE "$prohibited_patterns")

if [ -n "$problem_files" ]; then
	cat <<EOF
🚫 COMMIT BLOCKED: Files with prohibited naming patterns detected!

The following files contain words that indicate temporary or generic implementations:
$(echo "$problem_files" | sed 's/^/  - /')

❌ Prohibited patterns: unified, simple, enhanced

These naming patterns suggest:
- Temporary implementations that should be properly named
- Generic solutions that lack specific purpose
- Code that hasn't been properly integrated

🔧 To fix this:
1. Rename files to reflect their actual purpose and functionality
2. Use descriptive, specific names that indicate the file's role
3. Avoid generic adjectives in production code

Examples:
  ❌ unified_analytics_service.py  →  ✅ portfolio_analytics_service.py
  ❌ simple_data_loader.py       →  ✅ market_data_loader.py
  ❌ enhanced_validator.py       →  ✅ schema_validator.py

If this is intentional and the naming is appropriate, you can bypass this check with:
  git commit --no-verify

EOF
	exit 1
fi

# 2. Check Python files for prohibited content patterns
python_problem_files=""
content_prohibited_patterns="fake|mock|synthetic|fallback"

# Get all Python files being added or modified
python_files=$(git diff --cached --name-only --diff-filter=AM $against | grep '\.py$')

if [ -n "$python_files" ]; then
	for file in $python_files; do
		# Skip test files - mock data is allowed in tests
		case "$file" in
			*test*|*tests*|*/test_*|test_*)
				continue
				;;
		esac
		# Check file content for problematic patterns
		if git show :$file | grep -iE "$content_prohibited_patterns" >/dev/null 2>&1; then
			# Get specific problematic lines for better error reporting
			problematic_lines=$(git show :$file | grep -inE "$content_prohibited_patterns" | head -3)
			python_problem_files="$python_problem_files\n  📄 $file:\n$(echo "$problematic_lines" | sed 's/^/    /')"
		fi
	done
fi

if [ -n "$python_problem_files" ]; then
	cat <<EOF
🚫 COMMIT BLOCKED: Python files with prohibited content patterns detected!

The ATS platform follows a strict "REAL DATA ONLY" policy outside of unit tests.

Files with problematic patterns:
$(echo -e "$python_problem_files")

❌ Prohibited patterns in non-test Python files:
  • fake     - Fake data generation or usage
  • mock     - Mock data fallbacks (outside tests)
  • synthetic - Synthetic data creation
  • fallback - Fallback to demo/fake data when real data fails

🚨 Why this is dangerous:
  • Hides database connection problems and query failures
  • Masks data quality issues and real-world edge cases
  • Creates false performance metrics (demo data is always fast)
  • Prevents detection of authentication and network issues
  • Results in production surprises when real data behaves differently

✅ Correct approaches:
  • Fail fast when real data is unavailable
  • Show actual errors (connection failures, missing data, schema problems)
  • Use real market data from vendors (Polygon, Tiingo, FirstRate, EODHD)
  • Handle missing data explicitly with proper error messages

🔧 Examples of fixes:
  ❌ if data.empty: data = generate_fake_data()
  ✅ if data.empty: raise ValueError("No real market data available")

  ❌ except ConnectionError: return mock_response()
  ✅ except ConnectionError: logger.error("Database connection failed"); raise

  ❌ data = load_real_data() or fallback_synthetic_data()
  ✅ data = load_real_data(); validate_data_exists(data)

🧪 Note: Mock/fake data IS allowed in test files:
  • tests/*, *test*, test_*, *_test.py files are exempt
  • Use mocks freely for isolated unit testing

If this is legitimate usage (e.g., proper test file), you can bypass with:
  git commit --no-verify

EOF
	exit 1
fi

# 3. Check for non-ASCII filenames
allownonascii=$(git config --type=bool hooks.allownonascii)

if [ "$allownonascii" != "true" ] &&
	test $(git diff --cached --name-only --diff-filter=A -z $against |
	  LC_ALL=C tr -d '[ -~]\0' | wc -c) != 0
then
	cat <<\EOF
🚫 Error: Attempt to add a non-ASCII file name.

This can cause problems if you want to work with people on other platforms.

To be portable it is advisable to rename the file.

If you know what you are doing you can disable this check using:
  git config hooks.allownonascii true
EOF
	exit 1
fi

# 4. Check for whitespace errors
exec git diff-index --check --cached $against --
EOF

# Make the hook executable
chmod +x "$HOOKS_DIR/pre-commit"

echo "✅ Pre-commit hook installed successfully!"
echo "📍 Location: $HOOKS_DIR/pre-commit"
echo ""
echo "🔍 The hook will now check for:"
echo "  - Files with naming patterns: 'unified', 'simple', 'enhanced'"
echo "  - Non-ASCII filenames"
echo "  - Whitespace errors"
echo ""
echo "⚠️  To bypass the hook temporarily, use: git commit --no-verify"