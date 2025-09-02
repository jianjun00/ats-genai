#!/bin/bash
# Deployment Status Overview Script
# Shows comprehensive status of all deployments and ArgoCD

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

NAMESPACE="${NAMESPACE:-ats-dev}"
ARGOCD_NAMESPACE="${ARGOCD_NAMESPACE:-argocd}"
APPLICATION_NAME="${APPLICATION_NAME:-ats-dev}"

print_header() {
    echo -e "${PURPLE}📊 Deployment Status Overview${NC}"
    echo -e "${PURPLE}=============================${NC}"
    echo -e "${BLUE}Namespace: $NAMESPACE${NC}"
    echo -e "${BLUE}Timestamp: $(date)${NC}"
    echo ""
}

show_argocd_status() {
    echo -e "${BLUE}🔄 ArgoCD Application Status${NC}"
    echo -e "${BLUE}---------------------------${NC}"
    
    if kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" >/dev/null 2>&1; then
        local SYNC_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.sync.status}')
        local HEALTH_STATUS=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.health.status}')
        local LAST_SYNC=$(kubectl get application "$APPLICATION_NAME" -n "$ARGOCD_NAMESPACE" -o jsonpath='{.status.operationState.finishedAt}')
        
        # Status icons
        local SYNC_ICON="${RED}❌${NC}"
        case "$SYNC_STATUS" in
            "Synced") SYNC_ICON="${GREEN}✅${NC}" ;;
            "OutOfSync") SYNC_ICON="${YELLOW}⚠️${NC}" ;;
        esac
        
        local HEALTH_ICON="${RED}❌${NC}"
        case "$HEALTH_STATUS" in
            "Healthy") HEALTH_ICON="${GREEN}✅${NC}" ;;
            "Progressing") HEALTH_ICON="${YELLOW}🔄${NC}" ;;
            "Degraded") HEALTH_ICON="${YELLOW}⚠️${NC}" ;;
        esac
        
        echo -e "  Sync Status: $SYNC_ICON $SYNC_STATUS"
        echo -e "  Health Status: $HEALTH_ICON $HEALTH_STATUS"
        echo -e "  Last Sync: ${LAST_SYNC:-Never}"
    else
        echo -e "  ${RED}❌ ArgoCD application '$APPLICATION_NAME' not found${NC}"
    fi
    echo ""
}

show_deployment_summary() {
    echo -e "${BLUE}📦 Deployment Summary${NC}"
    echo -e "${BLUE}--------------------${NC}"
    
    local TOTAL_DEPLOYMENTS=$(kubectl get deployments -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
    local HEALTHY_DEPLOYMENTS=$(kubectl get deployments -n "$NAMESPACE" -o json 2>/dev/null | jq '[.items[] | select(.status.readyReplicas == .spec.replicas)] | length')
    local UNHEALTHY_DEPLOYMENTS=$((TOTAL_DEPLOYMENTS - HEALTHY_DEPLOYMENTS))
    
    echo -e "  Total Deployments: $TOTAL_DEPLOYMENTS"
    echo -e "  ${GREEN}✅ Healthy: $HEALTHY_DEPLOYMENTS${NC}"
    
    if [ $UNHEALTHY_DEPLOYMENTS -gt 0 ]; then
        echo -e "  ${RED}❌ Unhealthy: $UNHEALTHY_DEPLOYMENTS${NC}"
    fi
    echo ""
}

show_deployment_details() {
    echo -e "${BLUE}📋 Deployment Details${NC}"
    echo -e "${BLUE}--------------------${NC}"
    
    # Header
    printf "%-30s %-8s %-12s %-20s %-10s\n" "NAME" "STATUS" "READY" "IMAGE" "AGE"
    printf "%-30s %-8s %-12s %-20s %-10s\n" "----" "------" "-----" "-----" "---"
    
    kubectl get deployments -n "$NAMESPACE" -o json 2>/dev/null | \
    jq -r '.items[] | "\(.metadata.name)|\(.spec.replicas)|\(.status.readyReplicas // 0)|\(.spec.template.spec.containers[0].image)|\(.metadata.creationTimestamp)"' | \
    while IFS='|' read -r NAME REPLICAS READY IMAGE CREATED; do
        # Calculate age
        local CREATED_TIMESTAMP=$(date -d "$CREATED" +%s 2>/dev/null || echo "0")
        local NOW=$(date +%s)
        local AGE_SECONDS=$((NOW - CREATED_TIMESTAMP))
        local AGE_DISPLAY="${AGE_SECONDS}s"
        
        if [ $AGE_SECONDS -gt 86400 ]; then
            AGE_DISPLAY="$((AGE_SECONDS / 86400))d"
        elif [ $AGE_SECONDS -gt 3600 ]; then
            AGE_DISPLAY="$((AGE_SECONDS / 3600))h"
        elif [ $AGE_SECONDS -gt 60 ]; then
            AGE_DISPLAY="$((AGE_SECONDS / 60))m"
        fi
        
        # Status and icon
        local STATUS_ICON="${RED}❌${NC}"
        local STATUS="Unhealthy"
        if [ "$READY" = "$REPLICAS" ]; then
            STATUS_ICON="${GREEN}✅${NC}"
            STATUS="Healthy"
        elif [ "$READY" -lt "$REPLICAS" ] && [ "$READY" -gt 0 ]; then
            STATUS_ICON="${YELLOW}🔄${NC}"
            STATUS="Updating"
        fi
        
        # Truncate image name
        local SHORT_IMAGE=$(echo "$IMAGE" | sed 's/.*\///' | cut -c1-20)
        
        printf "%s %-29s %-8s %-12s %-20s %-10s\n" "$STATUS_ICON" "$NAME" "$STATUS" "$READY/$REPLICAS" "$SHORT_IMAGE" "$AGE_DISPLAY"
    done
    echo ""
}

show_service_summary() {
    echo -e "${BLUE}🌐 Service Summary${NC}"
    echo -e "${BLUE}-----------------${NC}"
    
    local TOTAL_SERVICES=$(kubectl get services -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l)
    local NODEPORT_SERVICES=$(kubectl get services -n "$NAMESPACE" --no-headers -o custom-columns="TYPE:.spec.type" 2>/dev/null | grep -c "NodePort" || echo "0")
    local LOADBALANCER_SERVICES=$(kubectl get services -n "$NAMESPACE" --no-headers -o custom-columns="TYPE:.spec.type" 2>/dev/null | grep -c "LoadBalancer" || echo "0")
    local CLUSTERIP_SERVICES=$(kubectl get services -n "$NAMESPACE" --no-headers -o custom-columns="TYPE:.spec.type" 2>/dev/null | grep -c "ClusterIP" || echo "0")
    
    echo -e "  Total Services: $TOTAL_SERVICES"
    echo -e "  🌐 NodePort: $NODEPORT_SERVICES"
    echo -e "  ⚖️  LoadBalancer: $LOADBALANCER_SERVICES"  
    echo -e "  🔒 ClusterIP: $CLUSTERIP_SERVICES"
    echo ""
}

show_external_access() {
    echo -e "${BLUE}🔗 External Access Points${NC}"
    echo -e "${BLUE}------------------------${NC}"
    
    local NODE_IP=$(kubectl get nodes -o wide --no-headers | head -1 | awk '{print ($7 != "<none>") ? $7 : $6}')
    
    kubectl get services -n "$NAMESPACE" -o json 2>/dev/null | \
    jq -r '.items[] | select(.spec.type == "NodePort") | "\(.metadata.name)|\(.spec.ports[0].port)|\(.spec.ports[0].nodePort)"' | \
    while IFS='|' read -r SERVICE PORT NODEPORT; do
        echo -e "  🌐 $SERVICE: http://$NODE_IP:$NODEPORT"
    done
    
    kubectl get services -n "$NAMESPACE" -o json 2>/dev/null | \
    jq -r '.items[] | select(.spec.type == "LoadBalancer") | "\(.metadata.name)|\(.spec.ports[0].port)|\(.status.loadBalancer.ingress[0].ip // "pending")"' | \
    while IFS='|' read -r SERVICE PORT EXTERNAL_IP; do
        if [ "$EXTERNAL_IP" != "pending" ] && [ "$EXTERNAL_IP" != "null" ]; then
            echo -e "  🌐 $SERVICE: http://$EXTERNAL_IP:$PORT"
        else
            echo -e "  ⏳ $SERVICE: LoadBalancer IP pending"
        fi
    done
    echo ""
}

show_pod_issues() {
    echo -e "${BLUE}⚠️  Pod Issues${NC}"
    echo -e "${BLUE}-------------${NC}"
    
    local PROBLEM_PODS=$(kubectl get pods -n "$NAMESPACE" --no-headers -o custom-columns="NAME:.metadata.name,STATUS:.status.phase,READY:.status.containerStatuses[*].ready" 2>/dev/null | \
    grep -v "Running.*true\|Succeeded.*" | head -10)
    
    if [ -n "$PROBLEM_PODS" ]; then
        echo "$PROBLEM_PODS" | while read -r POD STATUS READY; do
            local ISSUE_ICON="${RED}❌${NC}"
            case "$STATUS" in
                "Pending") ISSUE_ICON="${YELLOW}⏳${NC}" ;;
                "CrashLoopBackOff") ISSUE_ICON="${RED}🔄${NC}" ;;
                "Error") ISSUE_ICON="${RED}💥${NC}" ;;
            esac
            echo -e "  $ISSUE_ICON $POD: $STATUS"
        done
    else
        echo -e "  ${GREEN}✅ No pod issues found${NC}"
    fi
    echo ""
}

show_resource_usage() {
    echo -e "${BLUE}📊 Resource Usage${NC}"
    echo -e "${BLUE}----------------${NC}"
    
    # Get resource requests and limits
    local CPU_REQUESTS=$(kubectl get pods -n "$NAMESPACE" -o json 2>/dev/null | \
    jq '[.items[].spec.containers[].resources.requests.cpu // "0"] | map(tonumber) | add')
    
    local MEMORY_REQUESTS=$(kubectl get pods -n "$NAMESPACE" -o json 2>/dev/null | \
    jq '[.items[].spec.containers[].resources.requests.memory // "0Gi"] | map(gsub("Gi|Mi"; "") | tonumber) | add')
    
    echo -e "  CPU Requests: ${CPU_REQUESTS:-0} cores"
    echo -e "  Memory Requests: ${MEMORY_REQUESTS:-0} MB"
    
    # Pod count by node (if multiple nodes)
    local NODE_COUNT=$(kubectl get nodes --no-headers | wc -l)
    if [ "$NODE_COUNT" -gt 1 ]; then
        echo -e "  Pod Distribution:"
        kubectl get pods -n "$NAMESPACE" -o wide --no-headers 2>/dev/null | awk '{print $7}' | sort | uniq -c | \
        while read -r COUNT NODE; do
            echo -e "    $NODE: $COUNT pods"
        done
    fi
    echo ""
}

show_recent_events() {
    echo -e "${BLUE}📋 Recent Events (Last 10)${NC}"
    echo -e "${BLUE}------------------------${NC}"
    
    kubectl get events -n "$NAMESPACE" --sort-by='.lastTimestamp' -o custom-columns="AGE:.firstTimestamp,TYPE:.type,OBJECT:.involvedObject.name,REASON:.reason,MESSAGE:.message" --no-headers 2>/dev/null | \
    tail -10 | \
    while read -r AGE TYPE OBJECT REASON MESSAGE; do
        local TYPE_ICON="${BLUE}•${NC}"
        case "$TYPE" in
            "Warning") TYPE_ICON="${YELLOW}⚠️${NC}" ;;
            "Error") TYPE_ICON="${RED}❌${NC}" ;;
            "Normal") TYPE_ICON="${GREEN}ℹ️${NC}" ;;
        esac
        
        # Truncate long messages
        local SHORT_MESSAGE=$(echo "$MESSAGE" | cut -c1-60)
        printf "  %s %-20s %-12s %s\n" "$TYPE_ICON" "$OBJECT" "$REASON" "$SHORT_MESSAGE"
    done
    echo ""
}

show_quick_actions() {
    echo -e "${BLUE}⚡ Quick Actions${NC}"
    echo -e "${BLUE}---------------${NC}"
    echo "  🚀 Deploy changes:     ./scripts/dev_deploy.sh"
    echo "  🔄 Force ArgoCD sync:  ./scripts/force_argocd_sync.sh"
    echo "  📊 Monitor service:    ./scripts/monitor_deployment.sh <service-name>"
    echo "  🔙 Rollback service:   ./scripts/rollback_deployment.sh <service-name>"
    echo "  🔍 Check safety:       ./scripts/pre_deploy_check.sh"
    echo "  🌐 Get access URLs:    ./scripts/get_external_access.sh all"
    echo ""
}

main() {
    print_header
    show_argocd_status
    show_deployment_summary
    show_deployment_details
    show_service_summary
    show_external_access
    show_pod_issues
    show_resource_usage
    show_recent_events
    show_quick_actions
}

# Show usage if help requested
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "Usage: $0"
    echo ""
    echo "Shows comprehensive status overview of:"
    echo "- ArgoCD application sync status"
    echo "- Deployment health and details"
    echo "- Service endpoints and external access"
    echo "- Pod issues and resource usage"
    echo "- Recent events and quick action commands"
    echo ""
    echo "Environment Variables:"
    echo "  NAMESPACE           Target namespace (default: ats-dev)"
    echo "  ARGOCD_NAMESPACE    ArgoCD namespace (default: argocd)"
    echo "  APPLICATION_NAME    ArgoCD application (default: ats-dev)"
    echo ""
    exit 0
fi

# Run main function
main "$@"