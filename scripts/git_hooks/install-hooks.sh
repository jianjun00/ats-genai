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
# 2. Non-ASCII filename checks
# 3. Whitespace error checks
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

# 2. Check for non-ASCII filenames
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

# 3. Check for whitespace errors
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