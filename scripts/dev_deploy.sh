#!/bin/bash
# Development Deployment Script
# Deploys changes to ats-dev environment with safety checks and team coordination

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
LOCK_FILE="/tmp/ats-dev-deployment.lock"
MAX_WAIT_TIME=300  # 5 minutes

# Functions
print_header() {
    echo -e "${PURPLE}🚀 Development Deployment${NC}"
    echo -e "${PURPLE}========================${NC}"
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

create_deployment_lock() {
    BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")
    USER=$(whoami)
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    echo "Branch: $BRANCH | User: $USER | Started: $TIMESTAMP" > "$LOCK_FILE"
}

remove_deployment_lock() {
    rm -f "$LOCK_FILE"
}

check_git_status() {
    if ! git status >/dev/null 2>&1; then
        echo -e "${RED}❌ Not in a Git repository${NC}"
        exit 1
    fi
    
    BRANCH=$(git branch --show-current)
    if [ "$BRANCH" = "main" ]; then
        echo -e "${YELLOW}⚠️  You're deploying from the main branch.${NC}"
        echo -e "${YELLOW}   This will affect the production-like environment.${NC}"
        echo ""
        read -p "Continue? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${BLUE}Deployment cancelled by user${NC}"
            exit 0
        fi
    fi
    
    return 0
}

check_team_coordination() {
    if [ -f "$LOCK_FILE" ]; then
        LOCK_INFO=$(cat "$LOCK_FILE")
        echo -e "${RED}❌ Deployment already in progress:${NC}"
        echo -e "${YELLOW}   $LOCK_INFO${NC}"
        echo ""
        echo -e "${BLUE}Options:${NC}"
        echo "1. Wait for current deployment to finish"
        echo "2. Check with team member in #dev-deployments"
        echo "3. Force deployment (only if you're sure)"
        echo ""
        read -p "Force deployment anyway? (y/N): " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo -e "${BLUE}Deployment cancelled - respecting team coordination${NC}"
            exit 0
        fi
        echo -e "${YELLOW}⚠️  Forcing deployment despite existing lock${NC}"
    fi
}

announce_deployment() {
    BRANCH=$(git branch --show-current)
    USER=$(whoami)
    COMMIT=$(git rev-parse --short HEAD)
    
    echo -e "${BLUE}📢 Deployment Information${NC}"
    echo -e "${BLUE}Branch:${NC} $BRANCH"
    echo -e "${BLUE}User:${NC} $USER"  
    echo -e "${BLUE}Commit:${NC} $COMMIT"
    echo -e "${BLUE}Target:${NC} $NAMESPACE namespace"
    echo ""
    
    # If Slack webhook is configured, send notification
    if [ -n "$SLACK_WEBHOOK_URL" ]; then
        SLACK_MESSAGE="🚀 *Dev Deployment Started*\n*Branch:* $BRANCH\n*User:* $USER\n*Commit:* $COMMIT\n*Target:* $NAMESPACE"
        curl -s -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"$SLACK_MESSAGE\"}" \
            "$SLACK_WEBHOOK_URL" >/dev/null 2>&1 || true
    fi
}

wait_for_user_confirmation() {
    echo -e "${YELLOW}⚡ Ready to deploy to $NAMESPACE environment${NC}"
    echo ""
    echo -e "${BLUE}This will:${NC}"
    echo "1. Push current changes to remote branch"
    echo "2. Trigger ArgoCD sync to deploy changes"
    echo "3. Perform rolling update of affected services"
    echo ""
    echo -e "${YELLOW}⚠️  This affects the shared ats-dev environment${NC}"
    echo ""
    read -p "Proceed with deployment? (y/N): " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${BLUE}Deployment cancelled by user${NC}"
        exit 0
    fi
}

push_changes() {
    echo -e "${BLUE}📤 Pushing changes to remote repository...${NC}"
    
    BRANCH=$(git branch --show-current)
    
    # Check if branch exists on remote
    if git ls-remote --heads origin "$BRANCH" | grep -q "$BRANCH"; then
        git push origin "$BRANCH"
    else
        echo -e "${BLUE}Creating new remote branch: $BRANCH${NC}"
        git push -u origin "$BRANCH"
    fi
    
    print_status $? "Changes pushed to remote branch: $BRANCH"
}

force_argocd_sync() {
    echo -e "${BLUE}🔄 Triggering ArgoCD sync...${NC}"
    
    # Force hard refresh
    kubectl patch application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" --type='merge' \
        -p='{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}' >/dev/null 2>&1
    
    sleep 2
    
    # Trigger sync
    kubectl patch application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" --type='merge' \
        -p='{"operation":{"sync":{"revision":"HEAD"}}}' >/dev/null 2>&1
    
    print_status $? "ArgoCD sync triggered"
}

monitor_sync_progress() {
    echo -e "${BLUE}⏱️  Monitoring deployment progress...${NC}"
    echo ""
    
    START_TIME=$(date +%s)
    
    while true; do
        CURRENT_TIME=$(date +%s)
        ELAPSED=$((CURRENT_TIME - START_TIME))
        
        if [ $ELAPSED -gt $MAX_WAIT_TIME ]; then
            echo -e "${RED}❌ Deployment timeout after ${MAX_WAIT_TIME}s${NC}"
            print_rollback_instructions
            return 1
        fi
        
        # Get ArgoCD status
        SYNC_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
            -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "Unknown")
        HEALTH_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
            -o jsonpath='{.status.health.status}' 2>/dev/null || echo "Unknown")
        
        printf "\r${BLUE}Status: Sync=$SYNC_STATUS Health=$HEALTH_STATUS Elapsed=${ELAPSED}s${NC}"
        
        case "$SYNC_STATUS" in
            "Synced")
                echo ""
                print_status 0 "ArgoCD sync completed successfully"
                break
                ;;
            "Failed"|"Error")
                echo ""
                print_status 1 "ArgoCD sync failed"
                show_sync_error
                print_rollback_instructions
                return 1
                ;;
            *)
                sleep 5
                continue
                ;;
        esac
    done
    
    echo ""
    return 0
}

show_sync_error() {
    echo -e "${RED}Sync Error Details:${NC}"
    kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" \
        -o jsonpath='{.status.conditions[*].message}' 2>/dev/null | head -3 | sed 's/^/  /'
}

print_rollback_instructions() {
    echo ""
    echo -e "${YELLOW}🔄 Quick Rollback Options:${NC}"
    echo -e "${BLUE}Option 1 (Kubernetes):${NC} kubectl rollout undo deployment/<service-name> -n $NAMESPACE"
    echo -e "${BLUE}Option 2 (Script):${NC} ./scripts/rollback_deployment.sh <service-name>"
    echo -e "${BLUE}Option 3 (Git):${NC} git revert HEAD && git push origin \$(git branch --show-current)"
}

get_service_access_info() {
    echo -e "${BLUE}🔗 Service Access Information${NC}"
    
    # Get external node IP
    NODE_IP=$(kubectl get nodes -o wide --no-headers | head -1 | awk '{print $7}')
    if [ -z "$NODE_IP" ]; then
        NODE_IP=$(kubectl get nodes -o wide --no-headers | head -1 | awk '{print $6}')
    fi
    
    # Get NodePort services
    NODEPORT_SERVICES=$(kubectl get services -n "$NAMESPACE" -o custom-columns="NAME:.metadata.name,TYPE:.spec.type,PORTS:.spec.ports[*].nodePort" --no-headers | grep NodePort)
    
    if [ -n "$NODEPORT_SERVICES" ]; then
        echo -e "${BLUE}External Access URLs:${NC}"
        echo "$NODEPORT_SERVICES" | while read -r SERVICE_NAME SERVICE_TYPE NODEPORTS; do
            for PORT in $NODEPORTS; do
                if [ "$PORT" != "<none>" ]; then
                    echo "  • $SERVICE_NAME: http://$NODE_IP:$PORT"
                fi
            done
        done
    else
        echo -e "${YELLOW}No NodePort services found${NC}"
    fi
    
    echo ""
}

cleanup_deployment() {
    remove_deployment_lock
    
    if [ -n "$SLACK_WEBHOOK_URL" ]; then
        BRANCH=$(git branch --show-current)
        USER=$(whoami)
        STATUS_EMOJI=$([ $1 -eq 0 ] && echo "✅" || echo "❌")
        STATUS_TEXT=$([ $1 -eq 0 ] && echo "Success" || echo "Failed")
        
        SLACK_MESSAGE="$STATUS_EMOJI *Dev Deployment $STATUS_TEXT*\n*Branch:* $BRANCH\n*User:* $USER"
        curl -s -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"$SLACK_MESSAGE\"}" \
            "$SLACK_WEBHOOK_URL" >/dev/null 2>&1 || true
    fi
}

# Trap to ensure cleanup on exit
trap 'cleanup_deployment $?' EXIT

# Main execution
main() {
    print_header
    
    # Pre-flight checks
    print_info "Running pre-deployment checks..."
    check_git_status
    check_team_coordination
    
    # Create deployment lock
    create_deployment_lock
    
    # Announce deployment
    announce_deployment
    
    # Get user confirmation
    wait_for_user_confirmation
    
    # Execute deployment
    if push_changes && force_argocd_sync && monitor_sync_progress; then
        echo ""
        print_status 0 "Deployment completed successfully!"
        get_service_access_info
        
        echo -e "${GREEN}🎉 Your changes are now live in the $NAMESPACE environment${NC}"
        echo ""
        echo -e "${BLUE}Next steps:${NC}"
        echo "1. Test your changes using the URLs above"
        echo "2. Monitor for any issues: ./scripts/monitor_deployment.sh"
        echo "3. If issues found: ./scripts/rollback_deployment.sh <service-name>"
        echo ""
        
        return 0
    else
        echo ""
        print_status 1 "Deployment failed"
        echo ""
        return 1
    fi
}

# Show usage if help requested
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "Usage: $0"
    echo ""
    echo "Environment Variables:"
    echo "  NAMESPACE           Target Kubernetes namespace (default: ats-dev)"
    echo "  ARGOCD_NAMESPACE    ArgoCD namespace (default: argocd)"
    echo "  APPLICATION_NAME    ArgoCD application name (default: ats-dev)"
    echo "  SLACK_WEBHOOK_URL   Slack webhook for notifications (optional)"
    echo ""
    echo "Examples:"
    echo "  $0                  # Deploy current branch to ats-dev"
    echo "  NAMESPACE=ats-staging $0  # Deploy to staging environment"
    echo ""
    exit 0
fi

# Run main function
main "$@"