#!/bin/bash
# Get External Access Information Script
# Shows how to access services from outside the cluster

set -e

NAMESPACE="${NAMESPACE:-ats-dev}"
SERVICE_NAME="$1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

if [ -z "$SERVICE_NAME" ]; then
    echo -e "${RED}Usage: $0 <service-name-or-pattern>${NC}"
    echo ""
    echo "Examples:"
    echo "  $0 analytics-service"
    echo "  $0 all                    # Show all services"
    echo ""
    echo "Available services with external access:"
    kubectl get services -n "$NAMESPACE" -o custom-columns="NAME:.metadata.name,TYPE:.spec.type,PORTS:.spec.ports[*].nodePort" --no-headers | grep -E "(NodePort|LoadBalancer)" | head -10
    exit 1
fi

get_node_external_ip() {
    # Try external IP first, fallback to internal IP
    kubectl get nodes -o wide --no-headers | head -1 | awk '{print ($7 != "<none>") ? $7 : $6}'
}

show_service_access() {
    local SERVICE="$1"
    local SERVICE_INFO=$(kubectl get service "$SERVICE" -n "$NAMESPACE" -o json 2>/dev/null)
    
    if [ -z "$SERVICE_INFO" ]; then
        echo -e "${RED}❌ Service '$SERVICE' not found${NC}"
        return 1
    fi
    
    local SERVICE_TYPE=$(echo "$SERVICE_INFO" | jq -r '.spec.type')
    local PORTS=$(echo "$SERVICE_INFO" | jq -r '.spec.ports[]')
    
    echo -e "${BLUE}🔗 $SERVICE ($SERVICE_TYPE)${NC}"
    
    case "$SERVICE_TYPE" in
        "NodePort")
            local NODE_IP=$(get_node_external_ip)
            echo "$PORTS" | jq -r '. | "\(.port):\(.nodePort):\(.protocol)"' | while IFS=':' read -r PORT NODEPORT PROTOCOL; do
                if [ "$NODEPORT" != "null" ]; then
                    echo -e "${GREEN}  External: http://$NODE_IP:$NODEPORT${NC}"
                    echo -e "${BLUE}  Internal: http://$SERVICE:$PORT${NC}"
                fi
            done
            ;;
        "LoadBalancer")
            local EXTERNAL_IP=$(echo "$SERVICE_INFO" | jq -r '.status.loadBalancer.ingress[0].ip // .status.loadBalancer.ingress[0].hostname // "pending"')
            echo "$PORTS" | jq -r '.port' | while read -r PORT; do
                if [ "$EXTERNAL_IP" != "pending" ] && [ "$EXTERNAL_IP" != "null" ]; then
                    echo -e "${GREEN}  External: http://$EXTERNAL_IP:$PORT${NC}"
                else
                    echo -e "${BLUE}  External: LoadBalancer IP pending${NC}"
                fi
                echo -e "${BLUE}  Internal: http://$SERVICE:$PORT${NC}"
            done
            ;;
        "ClusterIP")
            echo "$PORTS" | jq -r '.port' | while read -r PORT; do
                echo -e "${BLUE}  Internal only: http://$SERVICE:$PORT${NC}"
                echo -e "${BLUE}  Port-forward: kubectl port-forward service/$SERVICE $PORT:$PORT -n $NAMESPACE${NC}"
            done
            ;;
    esac
    echo ""
}

if [ "$SERVICE_NAME" = "all" ]; then
    echo -e "${BLUE}🌐 External Access Information for all services${NC}"
    echo -e "${BLUE}=============================================${NC}"
    echo ""
    
    kubectl get services -n "$NAMESPACE" --no-headers -o custom-columns="NAME:.metadata.name" | while read -r SERVICE; do
        show_service_access "$SERVICE"
    done
else
    # Support pattern matching
    MATCHING_SERVICES=$(kubectl get services -n "$NAMESPACE" --no-headers -o custom-columns="NAME:.metadata.name" | grep "$SERVICE_NAME" || true)
    
    if [ -z "$MATCHING_SERVICES" ]; then
        echo -e "${RED}❌ No services found matching '$SERVICE_NAME'${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}🌐 External Access Information${NC}"
    echo -e "${BLUE}=============================${NC}"
    echo ""
    
    echo "$MATCHING_SERVICES" | while read -r SERVICE; do
        show_service_access "$SERVICE"
    done
fi