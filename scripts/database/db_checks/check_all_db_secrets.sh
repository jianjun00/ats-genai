#!/bin/bash
# Script to check database connections for all environments using Kubernetes secrets

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Default values
ENVIRONMENTS=("dev" "intg" "prod" "test")
SPECIFIED_ENV=""

# Function to display usage
function show_usage {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -e, --env ENV     Check only the specified environment (dev, intg, prod, test)"
    echo "  -h, --help        Show this help message"
    echo ""
    echo "Example:"
    echo "  $0                Check all environments"
    echo "  $0 --env dev      Check only the dev environment"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--env)
            SPECIFIED_ENV="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# If a specific environment was specified, only check that one
if [ -n "$SPECIFIED_ENV" ]; then
    ENVIRONMENTS=("$SPECIFIED_ENV")
fi

echo "=== Database Connection Check ==="
echo "Checking database connections using Kubernetes secrets"
echo

# Check each environment
for env in "${ENVIRONMENTS[@]}"; do
    echo "=== Checking $env environment ==="
    namespace="ats-$env"
    secret_name="db-credentials-$env"
    
    echo "Namespace: $namespace"
    echo "Secret: $secret_name"
    
    # Check if the namespace exists
    if ! kubectl get namespace "$namespace" &>/dev/null; then
        echo "❌ Namespace $namespace does not exist"
        echo "Creating namespace..."
        kubectl create namespace "$namespace"
    fi
    
    # Check if the secret exists
    if ! kubectl get secret "$secret_name" -n "$namespace" &>/dev/null; then
        echo "❌ Secret $secret_name does not exist in namespace $namespace"
        echo "You need to create the secret first:"
        echo "  ./scripts/create_k8s_secrets.sh --env-file .env.$env --create-ns --apply"
        continue
    fi
    
    # Run the Python script to check the database connection
    echo "Running database connection check..."
    uv run python "$SCRIPT_DIR/check_k8s_db_secrets.py" --namespace "$namespace" --secret "$secret_name"
    
    echo
done

echo "=== Database Connection Check Complete ==="
