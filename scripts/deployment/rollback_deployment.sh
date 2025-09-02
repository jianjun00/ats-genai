#!/bin/bash
# Rollback Deployment Script
# Provides multiple rollback strategies for quick recovery

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="${NAMESPACE:-ats-dev}"
ARGOCD_NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
APPLICATION_NAME="${APPLICATION_NAME:-ats-dev}"

# Usage
if [ $# -eq 0 ]; then
    echo -e "${RED}Usage: $0 <service-name> [strategy] [options]${NC}"
    echo ""
    echo "Strategies:"
    echo "  k8s       - Kubernetes rollout undo (fastest, ~30 seconds)"
    echo "  git       - Git revert + ArgoCD sync (safest, ~2 minutes)"
    echo "  argocd    - ArgoCD rollback to previous revision"
    echo "  manual    - Interactive selection of rollback target"
    echo ""
    echo "Options:"
    echo "  --immediate   Skip confirmation prompts"
    echo "  --revision N  Rollback to specific revision"
    echo ""
    echo "Examples:"
    echo "  $0 analytics-service                    # Interactive strategy selection"
    echo "  $0 analytics-service k8s                # Fast Kubernetes rollback"
    echo "  $0 analytics-service git --immediate    # Git rollback without prompts"
    echo "  $0 analytics-service k8s --revision 3   # Rollback to revision 3"
    echo ""
    echo "Available deployments:"
    kubectl get deployments -n "$NAMESPACE" --no-headers -o custom-columns="NAME:.metadata.name" 2>/dev/null | head -10 | sed 's/^/  /'
    exit 1
fi

SERVICE_NAME="$1"
STRATEGY="${2:-interactive}"
IMMEDIATE_MODE=false
TARGET_REVISION=""

# Parse options
shift 2 2>/dev/null || shift 1
while [[ $# -gt 0 ]]; do
    case $1 in
        --immediate)
            IMMEDIATE_MODE=true
            shift
            ;;
        --revision)
            TARGET_REVISION="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Functions
print_header() {
    echo -e "${PURPLE}🔄 Deployment Rollback - $SERVICE_NAME${NC}"
    echo -e "${PURPLE}======================================${NC}"
    echo ""
}

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

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

check_deployment_exists() {
    if ! kubectl get deployment "$SERVICE_NAME" -n "$NAMESPACE" >/dev/null 2>&1; then
        echo -e "${RED}❌ Deployment '$SERVICE_NAME' not found in namespace '$NAMESPACE'${NC}"
        echo ""
        echo "Available deployments:"
        kubectl get deployments -n "$NAMESPACE" --no-headers -o custom-columns="NAME:.metadata.name" 2>/dev/null | sed 's/^/  /'
        exit 1
    fi
}

show_current_status() {
    echo -e "${BLUE}📊 Current Deployment Status${NC}"
    echo ""
    
    local DEPLOYMENT_INFO=$(kubectl get deployment "$SERVICE_NAME" -n "$NAMESPACE" -o json 2>/dev/null)
    local REPLICAS=$(echo "$DEPLOYMENT_INFO" | jq -r '.spec.replicas')
    local READY_REPLICAS=$(echo "$DEPLOYMENT_INFO" | jq -r '.status.readyReplicas // 0')
    local IMAGE=$(echo "$DEPLOYMENT_INFO" | jq -r '.spec.template.spec.containers[0].image')
    
    echo -e "${BLUE}Deployment:${NC} $SERVICE_NAME"
    echo -e "${BLUE}Replicas:${NC} $READY_REPLICAS/$REPLICAS"
    echo -e "${BLUE}Image:${NC} $IMAGE"
    
    # Get rollout history
    echo ""
    echo -e "${BLUE}📋 Rollout History:${NC}"
    kubectl rollout history deployment/"$SERVICE_NAME" -n "$NAMESPACE" 2>/dev/null | tail -n +2 | tail -5
}

get_user_confirmation() {
    if [ "$IMMEDIATE_MODE" = true ]; then
        return 0
    fi
    
    echo ""
    echo -e "${YELLOW}⚠️  This will rollback the deployment '$SERVICE_NAME'${NC}"
    echo -e "${YELLOW}   Strategy: $1${NC}"
    echo -e "${YELLOW}   This affects the live $NAMESPACE environment${NC}"
    echo ""
    read -p "Proceed with rollback? (y/N): " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${BLUE}Rollback cancelled by user${NC}"
        exit 0
    fi
}

rollback_kubernetes() {
    echo -e "${BLUE}🔄 Kubernetes Rollout Undo${NC}"
    echo ""
    
    get_user_confirmation "Kubernetes rollout undo"
    
    # Perform rollback
    if [ -n "$TARGET_REVISION" ]; then
        echo -e "${BLUE}Rolling back to revision $TARGET_REVISION...${NC}"
        kubectl rollout undo deployment/"$SERVICE_NAME" -n "$NAMESPACE" --to-revision="$TARGET_REVISION"
    else
        echo -e "${BLUE}Rolling back to previous revision...${NC}"
        kubectl rollout undo deployment/"$SERVICE_NAME" -n "$NAMESPACE"
    fi
    
    print_status $? "Rollback command executed"
    
    # Wait for rollback completion
    echo -e "${BLUE}Waiting for rollback to complete...${NC}"
    kubectl rollout status deployment/"$SERVICE_NAME" -n "$NAMESPACE" --timeout=300s
    
    print_status $? "Rollback completed"
    
    # Show new status
    echo ""
    echo -e "${BLUE}📊 Post-Rollback Status:${NC}"
    kubectl get deployment "$SERVICE_NAME" -n "$NAMESPACE"
}

rollback_git() {
    echo -e "${BLUE}🔄 Git Revert + ArgoCD Sync${NC}"
    echo ""
    
    get_user_confirmation "Git revert with ArgoCD sync"
    
    # Check if we're in a Git repository
    if ! git status >/dev/null 2>&1; then
        echo -e "${RED}❌ Not in a Git repository${NC}"
        return 1
    fi
    
    # Show recent commits
    echo -e "${BLUE}📋 Recent commits:${NC}"
    git log --oneline -5
    echo ""
    
    # Determine what to revert
    local COMMIT_TO_REVERT=""
    if [ -n "$TARGET_REVISION" ]; then
        COMMIT_TO_REVERT="$TARGET_REVISION"
    else
        # Default to HEAD (most recent commit)
        COMMIT_TO_REVERT="HEAD"
        echo -e "${BLUE}Reverting most recent commit (HEAD)${NC}"
    fi
    
    # Perform Git revert
    echo -e "${BLUE}Reverting commit: $COMMIT_TO_REVERT${NC}"
    git revert --no-edit "$COMMIT_TO_REVERT"
    
    print_status $? "Git revert completed"
    
    # Push changes
    local BRANCH=$(git branch --show-current)
    echo -e "${BLUE}Pushing revert to branch: $BRANCH${NC}"
    git push origin "$BRANCH"
    
    print_status $? "Changes pushed to remote"
    
    # Trigger ArgoCD sync
    echo -e "${BLUE}Triggering ArgoCD sync...${NC}"
    ./scripts/force_argocd_sync.sh --force
    
    print_status $? "ArgoCD sync completed"
}

rollback_argocd() {
    echo -e "${BLUE}🔄 ArgoCD Rollback${NC}"
    echo ""
    
    get_user_confirmation "ArgoCD rollback"
    
    # Check if ArgoCD CLI is available
    if ! command -v argocd >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  ArgoCD CLI not available, using kubectl method${NC}"
        
        # Get previous sync revision
        local PREV_REVISION=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
            -o jsonpath='{.status.history[-2].revision}' 2>/dev/null)
        
        if [ -n "$PREV_REVISION" ]; then
            echo -e "${BLUE}Rolling back to revision: $PREV_REVISION${NC}"
            kubectl patch application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" --type='merge' \
                -p="{\"operation\":{\"sync\":{\"revision\":\"$PREV_REVISION\"}}}"
        else
            echo -e "${RED}❌ Cannot determine previous revision${NC}"
            return 1
        fi
    else
        # Use ArgoCD CLI for more sophisticated rollback
        echo -e "${BLUE}Using ArgoCD CLI for rollback...${NC}"
        argocd app rollback "$APPLICATION_NAME" "${TARGET_REVISION:-0}"
    fi
    
    print_status $? "ArgoCD rollback initiated"
    
    # Monitor rollback progress
    ./scripts/force_argocd_sync.sh --force
}

interactive_strategy_selection() {
    echo -e "${BLUE}🤔 Select Rollback Strategy${NC}"
    echo ""
    echo "Available strategies:"
    echo "1. Kubernetes rollout undo (fastest, ~30 seconds)"
    echo "2. Git revert + ArgoCD sync (safest, ~2 minutes)" 
    echo "3. ArgoCD rollback (GitOps compliant)"
    echo ""
    read -p "Enter your choice (1-3): " -n 1 -r
    echo ""
    
    case $REPLY in
        1)
            STRATEGY="k8s"
            ;;
        2)
            STRATEGY="git"
            ;;
        3)
            STRATEGY="argocd"
            ;;
        *)
            echo -e "${RED}Invalid choice. Defaulting to Kubernetes rollback.${NC}"
            STRATEGY="k8s"
            ;;
    esac
    
    echo -e "${BLUE}Selected strategy: $STRATEGY${NC}"
    echo ""
}

monitor_rollback_progress() {
    echo ""
    echo -e "${BLUE}📊 Monitoring rollback progress...${NC}"
    echo -e "${BLUE}Press Ctrl+C to stop monitoring (rollback will continue)${NC}"
    echo ""
    
    # Start monitoring in background if script exists
    if [ -f "./scripts/monitor_deployment.sh" ]; then
        timeout 60 ./scripts/monitor_deployment.sh "$SERVICE_NAME" || true
    else
        # Simple monitoring
        for i in {1..12}; do
            local STATUS=$(kubectl get deployment "$SERVICE_NAME" -n "$NAMESPACE" -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null)
            printf "\r${BLUE}Status: $STATUS${NC}"
            sleep 5
        done
        echo ""
    fi
}

show_rollback_summary() {
    echo ""
    echo -e "${BLUE}📊 Rollback Summary${NC}"
    echo -e "${BLUE}==================${NC}"
    
    # Get current deployment status
    local DEPLOYMENT_INFO=$(kubectl get deployment "$SERVICE_NAME" -n "$NAMESPACE" -o json 2>/dev/null)
    local REPLICAS=$(echo "$DEPLOYMENT_INFO" | jq -r '.spec.replicas')
    local READY_REPLICAS=$(echo "$DEPLOYMENT_INFO" | jq -r '.status.readyReplicas // 0')
    local NEW_IMAGE=$(echo "$DEPLOYMENT_INFO" | jq -r '.spec.template.spec.containers[0].image')
    
    echo -e "${BLUE}Service:${NC} $SERVICE_NAME"
    echo -e "${BLUE}Namespace:${NC} $NAMESPACE"
    echo -e "${BLUE}Strategy Used:${NC} $STRATEGY"
    echo -e "${BLUE}Current Replicas:${NC} $READY_REPLICAS/$REPLICAS"
    echo -e "${BLUE}Current Image:${NC} $NEW_IMAGE"
    
    if [ "$READY_REPLICAS" = "$REPLICAS" ]; then
        echo -e "${GREEN}✅ Rollback completed successfully${NC}"
    else
        echo -e "${YELLOW}⚠️  Rollback may still be in progress${NC}"
    fi
    
    # Show service endpoints
    echo ""
    echo -e "${BLUE}🔗 Service Endpoints:${NC}"
    kubectl get services -n "$NAMESPACE" -l app="$SERVICE_NAME" --no-headers 2>/dev/null | \
    while read -r SVC_NAME SVC_TYPE CLUSTER_IP EXTERNAL_IP PORT AGE; do
        case "$SVC_TYPE" in
            "NodePort")
                local NODEPORT=$(echo "$PORT" | grep -o '[0-9]*:[0-9]*' | cut -d: -f2)
                local NODE_IP=$(kubectl get nodes -o wide --no-headers | head -1 | awk '{print $7}')
                echo "  🌐 $SVC_NAME: http://$NODE_IP:$NODEPORT"
                ;;
            "LoadBalancer")
                if [ "$EXTERNAL_IP" != "<pending>" ] && [ "$EXTERNAL_IP" != "<none>" ]; then
                    echo "  🌐 $SVC_NAME: http://$EXTERNAL_IP"
                else
                    echo "  ⏳ $SVC_NAME: LoadBalancer pending"
                fi
                ;;
        esac
    done
}

main() {
    print_header
    
    # Check deployment exists
    check_deployment_exists
    
    # Show current status
    show_current_status
    
    # Select strategy if interactive
    if [ "$STRATEGY" = "interactive" ]; then
        interactive_strategy_selection
    fi
    
    # Execute rollback based on strategy
    case "$STRATEGY" in
        "k8s"|"kubernetes")
            rollback_kubernetes
            ;;
        "git")
            rollback_git
            ;;
        "argocd")
            rollback_argocd
            ;;
        *)
            echo -e "${RED}Unknown strategy: $STRATEGY${NC}"
            echo "Supported strategies: k8s, git, argocd"
            exit 1
            ;;
    esac
    
    # Monitor progress
    monitor_rollback_progress
    
    # Show summary
    show_rollback_summary
    
    echo ""
    echo -e "${GREEN}🎉 Rollback process completed!${NC}"
    echo ""
    echo -e "${BLUE}Next steps:${NC}"
    echo "1. Test the rolled-back service to confirm it's working"
    echo "2. Monitor for any issues: ./scripts/monitor_deployment.sh $SERVICE_NAME"
    echo "3. Check logs if needed: kubectl logs -n $NAMESPACE -l app=$SERVICE_NAME"
    echo "4. Plan and implement proper fix for the original issue"
}

# Run main function
main "$@"