#!/bin/bash
# Force ArgoCD Sync Script
# Forces immediate ArgoCD synchronization

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ARGOCD_NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
APPLICATION_NAME="${APPLICATION_NAME:-ats-dev}"
TIMEOUT=300  # 5 minutes

print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        return 1
    fi
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

check_argocd_availability() {
    if ! kubectl get namespace "$ARGOCD_NAMESPACE" >/dev/null 2>&1; then
        echo -e "${RED}❌ ArgoCD namespace '$ARGOCD_NAMESPACE' not found${NC}"
        exit 1
    fi
    
    if ! kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" >/dev/null 2>&1; then
        echo -e "${RED}❌ ArgoCD application '$APPLICATION_NAME' not found${NC}"
        exit 1
    fi
    
    print_status 0 "ArgoCD application found: $APPLICATION_NAME"
}

get_current_status() {
    local SYNC_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
    local HEALTH_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}' 2>/dev/null || echo "Unknown")
    
    echo -e "${BLUE}Current Status:${NC}"
    echo -e "${BLUE}  Sync: $SYNC_STATUS${NC}"
    echo -e "${BLUE}  Health: $HEALTH_STATUS${NC}"
    echo ""
}

force_hard_refresh() {
    echo -e "${BLUE}🔄 Forcing hard refresh...${NC}"
    
    kubectl patch application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" --type='merge' \
        -p='{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}' >/dev/null 2>&1
    
    print_status $? "Hard refresh annotation applied"
    sleep 5
}

trigger_sync() {
    echo -e "${BLUE}⚡ Triggering immediate sync...${NC}"
    
    kubectl patch application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" --type='merge' \
        -p='{"operation":{"sync":{"revision":"HEAD"}}}' >/dev/null 2>&1
    
    print_status $? "Sync operation triggered"
}

wait_for_sync() {
    echo -e "${BLUE}⏱️  Waiting for sync completion...${NC}"
    
    local START_TIME=$(date +%s)
    local LAST_STATUS=""
    
    while true; do
        local CURRENT_TIME=$(date +%s)
        local ELAPSED=$((CURRENT_TIME - START_TIME))
        
        if [ $ELAPSED -gt $TIMEOUT ]; then
            echo ""
            echo -e "${RED}❌ Sync timeout after ${TIMEOUT}s${NC}"
            return 1
        fi
        
        local SYNC_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
            -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
        local OPERATION_PHASE=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
            -o jsonpath='{.status.operationState.phase}' 2>/dev/null || echo "")
        
        # Show progress
        if [ "$SYNC_STATUS" != "$LAST_STATUS" ]; then
            echo -e "${BLUE}    Status changed: $SYNC_STATUS${NC}"
            LAST_STATUS="$SYNC_STATUS"
        else
            printf "\r${BLUE}    Waiting... Sync: $SYNC_STATUS Phase: $OPERATION_PHASE Elapsed: ${ELAPSED}s${NC}"
        fi
        
        case "$SYNC_STATUS" in
            "Synced")
                echo ""
                print_status 0 "Sync completed successfully"
                return 0
                ;;
            "Failed"|"Error")
                echo ""
                print_status 1 "Sync failed"
                show_sync_error
                return 1
                ;;
            *)
                sleep 3
                continue
                ;;
        esac
    done
}

show_sync_error() {
    echo -e "${RED}Sync Error Details:${NC}"
    local ERROR_MSG=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
        -o jsonpath='{.status.conditions[*].message}' 2>/dev/null)
    
    if [ -n "$ERROR_MSG" ]; then
        echo "$ERROR_MSG" | head -5 | sed 's/^/  /'
    else
        echo "  No detailed error message available"
    fi
    
    echo ""
    echo -e "${BLUE}Troubleshooting Steps:${NC}"
    echo "1. Check YAML syntax: ./scripts/validate_deployment.sh k8s/**/*.yaml"
    echo "2. Check resource conflicts: python scripts/detect_k8s_conflicts.py k8s/"
    echo "3. Check ArgoCD logs: kubectl logs -n $ARGOCD_NAMESPACE -l app.kubernetes.io/name=argocd-server"
    echo "4. Manual sync via ArgoCD UI"
}

show_final_status() {
    echo ""
    echo -e "${BLUE}📊 Final Status:${NC}"
    get_current_status
    
    # Get last sync time
    local LAST_SYNC=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
        -o jsonpath='{.status.operationState.finishedAt}' 2>/dev/null)
    
    if [ -n "$LAST_SYNC" ]; then
        echo -e "${BLUE}Last Sync: $LAST_SYNC${NC}"
    fi
    
    # Show any resources that are out of sync
    local OUT_OF_SYNC=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
        -o jsonpath='{.status.resources[?(@.status=="OutOfSync")].name}' 2>/dev/null)
    
    if [ -n "$OUT_OF_SYNC" ]; then
        echo -e "${YELLOW}⚠️  Resources still out of sync:${NC}"
        echo "$OUT_OF_SYNC" | tr ' ' '\n' | sed 's/^/  - /'
    fi
}

main() {
    echo -e "${BLUE}🚀 Force ArgoCD Sync${NC}"
    echo -e "${BLUE}===================${NC}"
    echo ""
    
    # Check ArgoCD availability
    check_argocd_availability
    
    # Show current status
    get_current_status
    
    # Ask for confirmation unless --force flag is provided
    if [ "$1" != "--force" ]; then
        echo -e "${YELLOW}⚠️  This will force immediate sync of $APPLICATION_NAME${NC}"
        echo -e "${YELLOW}   This may cause rolling updates of services in the target environment.${NC}"
        echo ""
        read -p "Continue? (y/N): " -n 1 -r
        echo ""
        
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${BLUE}Sync cancelled by user${NC}"
            exit 0
        fi
    fi
    
    # Execute sync
    if force_hard_refresh && trigger_sync && wait_for_sync; then
        echo ""
        print_status 0 "ArgoCD sync completed successfully!"
        show_final_status
        
        echo ""
        echo -e "${GREEN}🎉 Application is now synced with Git repository${NC}"
        echo ""
        echo -e "${BLUE}Next steps:${NC}"
        echo "1. Monitor deployment: ./scripts/monitor_deployment.sh <service-name>"
        echo "2. Test your changes at the service endpoints"
        echo "3. Check logs: ./scripts/get_service_logs.sh <service-name>"
        
        return 0
    else
        echo ""
        print_status 1 "ArgoCD sync failed"
        show_final_status
        
        echo ""
        echo -e "${BLUE}Recovery options:${NC}"
        echo "1. Fix issues and retry: $0"
        echo "2. Check ArgoCD UI for detailed error information"
        echo "3. Manual rollback: ./scripts/rollback_deployment.sh <service-name>"
        
        return 1
    fi
}

# Show usage if help requested
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "Usage: $0 [--force]"
    echo ""
    echo "Forces immediate ArgoCD synchronization of the application."
    echo ""
    echo "Options:"
    echo "  --force     Skip confirmation prompt"
    echo "  -h, --help  Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  ARGOCD_NAMESPACE    ArgoCD namespace (default: argocd)"
    echo "  APPLICATION_NAME    ArgoCD application name (default: ats-dev)"
    echo ""
    exit 0
fi

# Run main function
main "$@"