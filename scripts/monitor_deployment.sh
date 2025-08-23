#!/bin/bash
# Deployment Monitoring Script
# Monitors deployment progress and health

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
REFRESH_INTERVAL=5
MAX_MONITOR_TIME=600  # 10 minutes

# Usage
if [ $# -eq 0 ]; then
    echo -e "${RED}Usage: $0 <service-name> [deployment-name]${NC}"
    echo ""
    echo "Examples:"
    echo "  $0 analytics-service"
    echo "  $0 analytics-service ats-analytics-service"
    echo "  $0 all  # Monitor all deployments"
    echo ""
    echo "Available services:"
    kubectl get deployments -n "$NAMESPACE" --no-headers -o custom-columns="NAME:.metadata.name" 2>/dev/null | head -10 | sed 's/^/  /'
    exit 1
fi

SERVICE_NAME="$1"
DEPLOYMENT_NAME="${2:-$SERVICE_NAME}"

# Functions
print_header() {
    clear
    echo -e "${PURPLE}📊 Deployment Monitor - $SERVICE_NAME${NC}"
    echo -e "${PURPLE}$(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo -e "${PURPLE}================================${NC}"
    echo ""
}

get_deployment_status() {
    kubectl get deployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" -o json 2>/dev/null
}

get_pod_status() {
    kubectl get pods -l app="$SERVICE_NAME" -n "$NAMESPACE" -o json 2>/dev/null
}

display_deployment_info() {
    local DEPLOYMENT_JSON="$1"
    
    if [ -z "$DEPLOYMENT_JSON" ] || [ "$DEPLOYMENT_JSON" = "null" ]; then
        echo -e "${RED}❌ Deployment '$DEPLOYMENT_NAME' not found in namespace '$NAMESPACE'${NC}"
        return 1
    fi
    
    # Extract deployment information
    local REPLICAS=$(echo "$DEPLOYMENT_JSON" | jq -r '.spec.replicas // 0')
    local READY_REPLICAS=$(echo "$DEPLOYMENT_JSON" | jq -r '.status.readyReplicas // 0')
    local AVAILABLE_REPLICAS=$(echo "$DEPLOYMENT_JSON" | jq -r '.status.availableReplicas // 0')
    local UPDATED_REPLICAS=$(echo "$DEPLOYMENT_JSON" | jq -r '.status.updatedReplicas // 0')
    
    local IMAGE=$(echo "$DEPLOYMENT_JSON" | jq -r '.spec.template.spec.containers[0].image // "unknown"')
    local CREATION_TIME=$(echo "$DEPLOYMENT_JSON" | jq -r '.metadata.creationTimestamp')
    local STRATEGY=$(echo "$DEPLOYMENT_JSON" | jq -r '.spec.strategy.type // "RollingUpdate"')
    
    echo -e "${BLUE}📦 Deployment: $DEPLOYMENT_NAME${NC}"
    echo -e "${BLUE}Namespace:${NC} $NAMESPACE"
    echo -e "${BLUE}Strategy:${NC} $STRATEGY"
    echo -e "${BLUE}Image:${NC} $IMAGE"
    echo ""
    
    echo -e "${BLUE}📊 Replica Status:${NC}"
    echo -e "${BLUE}Desired:${NC} $REPLICAS"
    echo -e "${BLUE}Ready:${NC} $READY_REPLICAS"
    echo -e "${BLUE}Available:${NC} $AVAILABLE_REPLICAS"
    echo -e "${BLUE}Updated:${NC} $UPDATED_REPLICAS"
    
    # Status indicator
    if [ "$READY_REPLICAS" -eq "$REPLICAS" ] && [ "$AVAILABLE_REPLICAS" -eq "$REPLICAS" ]; then
        echo -e "${GREEN}✅ Deployment is healthy${NC}"
        return 0
    elif [ "$UPDATED_REPLICAS" -lt "$REPLICAS" ]; then
        echo -e "${YELLOW}🔄 Rolling update in progress${NC}"
        return 2
    else
        echo -e "${RED}❌ Deployment has issues${NC}"
        return 1
    fi
}

display_pod_info() {
    local PODS_JSON="$1"
    
    if [ -z "$PODS_JSON" ] || [ "$PODS_JSON" = "null" ]; then
        echo -e "${YELLOW}⚠️  No pods found for service '$SERVICE_NAME'${NC}"
        return
    fi
    
    echo ""
    echo -e "${BLUE}🐳 Pod Status:${NC}"
    
    # Extract pod information
    local POD_COUNT=$(echo "$PODS_JSON" | jq -r '.items | length')
    
    if [ "$POD_COUNT" -eq 0 ]; then
        echo -e "${YELLOW}⚠️  No pods running${NC}"
        return
    fi
    
    echo "$PODS_JSON" | jq -r '.items[] | "\(.metadata.name)|\(.status.phase)|\(.status.containerStatuses[0].ready // false)|\(.status.containerStatuses[0].restartCount // 0)|\(.metadata.creationTimestamp)"' | \
    while IFS='|' read -r POD_NAME POD_PHASE POD_READY RESTART_COUNT CREATION_TIME; do
        local STATUS_ICON=""
        case "$POD_PHASE" in
            "Running")
                if [ "$POD_READY" = "true" ]; then
                    STATUS_ICON="${GREEN}✅${NC}"
                else
                    STATUS_ICON="${YELLOW}🔄${NC}"
                fi
                ;;
            "Pending")
                STATUS_ICON="${YELLOW}⏳${NC}"
                ;;
            "Failed"|"Error")
                STATUS_ICON="${RED}❌${NC}"
                ;;
            *)
                STATUS_ICON="${BLUE}ℹ️${NC}"
                ;;
        esac
        
        local AGE=$(date -d "$CREATION_TIME" +%s 2>/dev/null || echo "0")
        local NOW=$(date +%s)
        local AGE_SECONDS=$((NOW - AGE))
        local AGE_DISPLAY="${AGE_SECONDS}s"
        
        if [ $AGE_SECONDS -gt 3600 ]; then
            AGE_DISPLAY="$((AGE_SECONDS / 3600))h"
        elif [ $AGE_SECONDS -gt 60 ]; then
            AGE_DISPLAY="$((AGE_SECONDS / 60))m"
        fi
        
        printf "  %s %-50s %s (restarts: %s, age: %s)\n" "$STATUS_ICON" "$POD_NAME" "$POD_PHASE" "$RESTART_COUNT" "$AGE_DISPLAY"
    done
}

display_events() {
    echo ""
    echo -e "${BLUE}📋 Recent Events:${NC}"
    
    kubectl get events -n "$NAMESPACE" \
        --field-selector involvedObject.name="$DEPLOYMENT_NAME" \
        --sort-by='.lastTimestamp' \
        --output=custom-columns="AGE:.firstTimestamp,TYPE:.type,REASON:.reason,MESSAGE:.message" \
        --no-headers 2>/dev/null | tail -5 | \
    while read -r AGE TYPE REASON MESSAGE; do
        local TYPE_ICON=""
        case "$TYPE" in
            "Normal")
                TYPE_ICON="${GREEN}ℹ️${NC}"
                ;;
            "Warning")
                TYPE_ICON="${YELLOW}⚠️${NC}"
                ;;
            "Error")
                TYPE_ICON="${RED}❌${NC}"
                ;;
            *)
                TYPE_ICON="${BLUE}•${NC}"
                ;;
        esac
        
        printf "  %s %s: %s\n" "$TYPE_ICON" "$REASON" "$MESSAGE"
    done
}

display_service_endpoints() {
    echo ""
    echo -e "${BLUE}🔗 Service Endpoints:${NC}"
    
    # Get services related to this deployment
    kubectl get services -n "$NAMESPACE" -l app="$SERVICE_NAME" -o json 2>/dev/null | \
    jq -r '.items[] | "\(.metadata.name)|\(.spec.type)|\(.spec.ports[].port // "")|\(.spec.ports[].nodePort // "")"' | \
    while IFS='|' read -r SVC_NAME SVC_TYPE PORT NODEPORT; do
        case "$SVC_TYPE" in
            "NodePort")
                NODE_IP=$(kubectl get nodes -o wide --no-headers | head -1 | awk '{print $7}')
                echo -e "  🌐 $SVC_NAME: http://$NODE_IP:$NODEPORT (NodePort)"
                ;;
            "LoadBalancer")
                EXTERNAL_IP=$(kubectl get service "$SVC_NAME" -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
                if [ -n "$EXTERNAL_IP" ]; then
                    echo -e "  🌐 $SVC_NAME: http://$EXTERNAL_IP:$PORT (LoadBalancer)"
                else
                    echo -e "  ⏳ $SVC_NAME: LoadBalancer IP pending"
                fi
                ;;
            "ClusterIP")
                echo -e "  🔒 $SVC_NAME: Internal only ($SVC_TYPE:$PORT)"
                ;;
        esac
    done
}

monitor_single_deployment() {
    local START_TIME=$(date +%s)
    local CONSECUTIVE_HEALTHY_CHECKS=0
    local REQUIRED_HEALTHY_CHECKS=3  # Need 3 consecutive healthy checks to consider stable
    
    while true; do
        local CURRENT_TIME=$(date +%s)
        local ELAPSED=$((CURRENT_TIME - START_TIME))
        
        # Check if we've exceeded max monitor time
        if [ $ELAPSED -gt $MAX_MONITOR_TIME ]; then
            echo -e "${YELLOW}⏰ Monitoring timeout after ${MAX_MONITOR_TIME}s${NC}"
            break
        fi
        
        print_header
        
        # Get current status
        local DEPLOYMENT_JSON=$(get_deployment_status)
        local PODS_JSON=$(get_pod_status)
        
        # Display information
        display_deployment_info "$DEPLOYMENT_JSON"
        local DEPLOYMENT_STATUS=$?
        
        display_pod_info "$PODS_JSON"
        display_events
        display_service_endpoints
        
        echo ""
        echo -e "${BLUE}📊 Monitoring: ${ELAPSED}s elapsed, refreshing every ${REFRESH_INTERVAL}s${NC}"
        echo -e "${BLUE}Press Ctrl+C to stop monitoring${NC}"
        
        # Check if deployment is stable
        if [ $DEPLOYMENT_STATUS -eq 0 ]; then
            CONSECUTIVE_HEALTHY_CHECKS=$((CONSECUTIVE_HEALTHY_CHECKS + 1))
            if [ $CONSECUTIVE_HEALTHY_CHECKS -ge $REQUIRED_HEALTHY_CHECKS ]; then
                echo ""
                echo -e "${GREEN}🎉 Deployment appears stable after $CONSECUTIVE_HEALTHY_CHECKS healthy checks${NC}"
                echo -e "${BLUE}You can safely stop monitoring (Ctrl+C) or continue watching${NC}"
            fi
        else
            CONSECUTIVE_HEALTHY_CHECKS=0
        fi
        
        sleep $REFRESH_INTERVAL
    done
}

monitor_all_deployments() {
    while true; do
        print_header
        
        echo -e "${BLUE}📦 All Deployments in $NAMESPACE:${NC}"
        echo ""
        
        kubectl get deployments -n "$NAMESPACE" -o custom-columns="NAME:.metadata.name,READY:.status.readyReplicas,UP-TO-DATE:.status.updatedReplicas,AVAILABLE:.status.availableReplicas,AGE:.metadata.creationTimestamp" --no-headers 2>/dev/null | \
        while read -r NAME READY UPDATED AVAILABLE AGE; do
            local STATUS_ICON="${GREEN}✅${NC}"
            if [ "$READY" = "<none>" ] || [ "$READY" = "0" ]; then
                STATUS_ICON="${RED}❌${NC}"
            elif [ "$UPDATED" != "$READY" ]; then
                STATUS_ICON="${YELLOW}🔄${NC}"
            fi
            
            printf "  %s %-30s Ready: %-3s Updated: %-3s Available: %-3s\n" \
                "$STATUS_ICON" "$NAME" "${READY:-0}" "${UPDATED:-0}" "${AVAILABLE:-0}"
        done
        
        echo ""
        echo -e "${BLUE}📊 Monitoring all deployments, refreshing every ${REFRESH_INTERVAL}s${NC}"
        echo -e "${BLUE}Press Ctrl+C to stop monitoring${NC}"
        
        sleep $REFRESH_INTERVAL
    done
}

# Main execution
case "$SERVICE_NAME" in
    "all")
        monitor_all_deployments
        ;;
    *)
        monitor_single_deployment
        ;;
esac