#!/bin/bash
# Pre-push hook logic
# Shared across all team members

set -e

# Import hook utilities
REPO_ROOT=$(git rev-parse --show-toplevel)
source "$REPO_ROOT/scripts/git_hooks/hook-utils.sh"

# Read stdin for refs being pushed
while read local_ref local_sha remote_ref remote_sha; do
    # Skip if deleting a branch
    if [ "$local_sha" = "0000000000000000000000000000000000000000" ]; then
        continue
    fi
    
    # Extract branch name
    branch=$(echo "$remote_ref" | sed 's|refs/heads/||')
    
    echo_header "Pre-push Hook: Validating push to '$branch'"
    
    # Master/main branch protection
    if [[ "$branch" == "main" || "$branch" == "master" ]]; then
        echo_warning "Pushing to protected branch: $branch"
        
        if [ "$NO_VERIFY" = "1" ]; then
            echo_warning "NO_VERIFY=1 set, bypassing protection (DANGEROUS!)"
            continue
        fi
        
        # Run comprehensive tests for main/master
        run_comprehensive_tests_for_main "$branch"
        
    else
        echo_success "Feature branch '$branch' - basic validation"
        run_basic_tests
    fi
done

echo_success "Pre-push checks completed"
