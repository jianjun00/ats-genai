#!/bin/bash
# Script to convert .env files to Kubernetes secrets and apply them

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
OUTPUT_DIR="$PROJECT_ROOT/k8s/secrets"
APPLY_SECRETS=false
CREATE_NAMESPACES=false
ENV_FILES=()

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Function to display usage
function show_usage {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -e, --env-file FILE    Path to .env file (can be specified multiple times)"
    echo "  -a, --all-envs         Process all .env.* files in the project root"
    echo "  -o, --output-dir DIR   Output directory for YAML files (default: k8s/secrets)"
    echo "  -A, --apply            Apply secrets to Kubernetes cluster after creation"
    echo "  -n, --create-ns        Create Kubernetes namespaces if they don't exist"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 --env-file .env.dev --env-file .env.prod --apply"
    echo "  $0 --all-envs"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--env-file)
            ENV_FILES+=("$2")
            shift 2
            ;;
        -a|--all-envs)
            # Find all .env.* files in the project root
            for file in "$PROJECT_ROOT"/.env.*; do
                # Skip .env.example and .env.template
                if [[ "$file" != *".env.example"* && "$file" != *".env.template"* ]]; then
                    ENV_FILES+=("$file")
                fi
            done
            shift
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -A|--apply)
            APPLY_SECRETS=true
            shift
            ;;
        -n|--create-ns)
            CREATE_NAMESPACES=true
            shift
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

# Check if any .env files were specified
if [ ${#ENV_FILES[@]} -eq 0 ]; then
    echo "Error: No .env files specified."
    show_usage
    exit 1
fi

# Process each .env file
for env_file in "${ENV_FILES[@]}"; do
    echo "Processing $env_file..."
    
    # Extract environment name from filename
    filename=$(basename "$env_file")
    if [[ "$filename" == .env.* ]]; then
        env_name=${filename#.env.}
    elif [[ "$filename" == ".env" ]]; then
        env_name="default"
    else
        env_name=$(basename "$env_file" .env)
    fi
    
    # Run the Python script to convert .env to Kubernetes secret
    uv run python "$SCRIPT_DIR/env_to_k8s_secrets.py" \
        --env-file "$env_file" \
        --output-dir "$OUTPUT_DIR" \
        --secret-name "db-credentials"
        
    echo "Created secret YAML for $env_name environment"
done

# Extract namespaces from generated YAML files
if [ "$APPLY_SECRETS" = true ] || [ "$CREATE_NAMESPACES" = true ]; then
    declare -A NAMESPACES
    
    for yaml_file in "$OUTPUT_DIR"/*.yaml; do
        # Extract namespace from YAML file
        namespace=$(grep -E "^[[:space:]]*namespace:" "$yaml_file" | awk '{print $2}')
        if [ -n "$namespace" ]; then
            NAMESPACES["$namespace"]="1"
        fi
    done
    
    # Create namespaces if they don't exist
    if [ "$CREATE_NAMESPACES" = true ]; then
        echo "Checking and creating namespaces if needed..."
        for namespace in "${!NAMESPACES[@]}"; do
            if ! kubectl get namespace "$namespace" &>/dev/null; then
                echo "Creating namespace: $namespace"
                kubectl create namespace "$namespace"
            else
                echo "Namespace $namespace already exists"
            fi
        done
    fi
    
    # Apply secrets to Kubernetes cluster if requested
    if [ "$APPLY_SECRETS" = true ]; then
        echo "Applying secrets to Kubernetes cluster..."
        for yaml_file in "$OUTPUT_DIR"/*.yaml; do
            echo "Applying $yaml_file..."
            kubectl apply -f "$yaml_file"
        done
        echo "All secrets applied successfully."
    fi
fi

echo "Done!"
