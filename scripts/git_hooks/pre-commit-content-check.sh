#!/bin/sh
#
# Pre-commit hook to prevent Python files with problematic content patterns
# that violate the "real data only" principle of the ATS platform
#
# This script checks for patterns like:
# - fake data generation
# - mock data fallbacks
# - synthetic data usage
# - fallback to demo data
#

if git rev-parse --verify HEAD >/dev/null 2>&1
then
	against=HEAD
else
	# Initial commit: diff against an empty tree object
	against=$(git hash-object -t tree /dev/null)
fi

# Check Python files for prohibited content patterns
problem_files=""
prohibited_patterns="fake|mock|synthetic|fallback|demo"

# Get all Python files being added or modified
python_files=$(git diff --cached --name-only --diff-filter=AM $against | grep '\.py$')

if [ -n "$python_files" ]; then
	for file in $python_files; do
		# Skip test files - mock data is allowed in tests
		case "$file" in
			*test*|*tests*|*/test_*)
				continue
				;;
		esac
		# Check file content for problematic patterns
		if git show :$file | grep -iE "$prohibited_patterns" >/dev/null 2>&1; then
			# Get specific problematic lines for better error reporting
			problematic_lines=$(git show :$file | grep -inE "$prohibited_patterns" | head -3)
			problem_files="$problem_files\n  📄 $file:\n$(echo "$problematic_lines" | sed 's/^/    /')"
		fi
	done
fi

if [ -n "$problem_files" ]; then
	cat <<EOF >&2
🚫 COMMIT BLOCKED: Python files with prohibited content patterns detected!

The ATS platform follows a strict "REAL DATA ONLY" policy outside of unit tests.

Files with problematic patterns:
$(echo -e "$problem_files")

❌ Prohibited patterns in non-test Python files:
  • fake     - Fake data generation or usage
  • mock     - Mock data fallbacks (outside tests)
  • synthetic - Synthetic data creation
  • fallback - Fallback to demo/fake data when real data fails
  • demo     - Demo data usage or generation

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

  ❌ if no_data: return demo_dataset()
  ✅ if no_data: raise ValueError("No market data available for analysis")

🧪 Note: Mock/fake data IS allowed in test files:
  • tests/*, *test*, test_*, *_test.py files are exempt
  • Use mocks freely for isolated unit testing

If this is legitimate usage (e.g., proper test file), you can bypass with:
  git commit --no-verify

EOF
	exit 1
fi

exit 0