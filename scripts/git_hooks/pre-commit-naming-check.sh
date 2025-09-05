#!/bin/sh
#
# Pre-commit hook to prevent files with certain naming patterns
# that indicate temporary or generic implementations
#
# This script should be called from the main pre-commit hook or can be
# installed directly as a pre-commit hook.

if git rev-parse --verify HEAD >/dev/null 2>&1
then
	against=HEAD
else
	# Initial commit: diff against an empty tree object
	against=$(git hash-object -t tree /dev/null)
fi

# Check for files with prohibited naming patterns
prohibited_patterns="unified|simple|enhanced"
problem_files=$(git diff --cached --name-only --diff-filter=A $against | grep -iE "$prohibited_patterns")

if [ -n "$problem_files" ]; then
	cat <<EOF >&2
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

exit 0