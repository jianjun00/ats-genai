#!/bin/bash
# Environment Configuration Validator

set -euo pipefail

ENV_FILE="${1:-.env}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "❌ Environment file $ENV_FILE not found"
    exit 1
fi

echo "🔍 Validating environment configuration: $ENV_FILE"

source "$ENV_FILE"

# Check required variables
required_vars=(
    "ENVIRONMENT"
    "DB_HOST" 
    "DB_PASSWORD"
    "POLYGON_API_KEY"
    "TIINGO_API_KEY"
    "EODHD_API_KEY"
)

errors=0

for var in "${required_vars[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        echo "❌ Missing required variable: $var"
        errors=$((errors + 1))
    else
        echo "✅ $var is set"
    fi
done

# Check API key formats
if [[ -n "${POLYGON_API_KEY:-}" && ${#POLYGON_API_KEY} -lt 20 ]]; then
    echo "⚠️  POLYGON_API_KEY appears to be too short"
fi

if [[ -n "${TIINGO_API_KEY:-}" && ${#TIINGO_API_KEY} -lt 30 ]]; then
    echo "⚠️  TIINGO_API_KEY appears to be too short" 
fi

if [[ $errors -eq 0 ]]; then
    echo "✅ Environment validation passed"
    exit 0
else
    echo "❌ Environment validation failed with $errors errors"
    exit 1
fi
