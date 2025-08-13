# Data Agent Kubernetes Deployment with ArgoCD

This directory contains Kubernetes manifests for deploying the Data Agent and its monitoring stack using ArgoCD for GitOps-based continuous deployment.

## Components

The deployment includes the following components:

1. **Data Agent**
   - Deployment with monitoring and health API enabled
   - Service exposing metrics and health endpoints

2. **Prometheus**
   - Deployment with auto-discovery of Kubernetes pods with Prometheus annotations
   - ConfigMap for Prometheus configuration
   - Service exposing the Prometheus web interface

3. **Grafana**
   - Deployment with pre-configured dashboards for Data Agent metrics
   - ConfigMaps for datasources, dashboard provisioning, and dashboard definitions
   - Service exposing the Grafana web interface

## Prerequisites

- Kubernetes cluster with ArgoCD installed
- Namespace `market-data` (will be created automatically by ArgoCD)
- Secrets for the Data Agent and Grafana (must be created separately)

## Secrets

Before deploying, you need to create the following secrets:

1. **Data Agent Secrets**

```bash
kubectl create secret generic data-agent-secrets \
  --namespace market-data \
  --from-literal=polygon-api-key=YOUR_POLYGON_API_KEY \
  --from-literal=tiingo-api-key=YOUR_TIINGO_API_KEY \
  --from-literal=openai-api-key=YOUR_OPENAI_API_KEY \
  --from-literal=database-url=YOUR_DATABASE_URL \
  --from-literal=slack-webhook-url=YOUR_SLACK_WEBHOOK_URL
```

2. **Grafana Secrets**

```bash
kubectl create secret generic grafana-secrets \
  --namespace market-data \
  --from-literal=admin-password=YOUR_ADMIN_PASSWORD
```

## Deployment with ArgoCD

1. Apply the ArgoCD Application manifest:

```bash
kubectl apply -f argocd-application.yaml -n argocd
```

2. ArgoCD will automatically sync and deploy all resources in this directory to the `market-data` namespace.

3. Monitor the deployment status in the ArgoCD UI or using the ArgoCD CLI:

```bash
argocd app get data-agent
```

## Accessing the Services

- **Data Agent Health API**: Available within the cluster at `http://data-agent:8080/health`
- **Data Agent Metrics**: Available within the cluster at `http://data-agent:8000/metrics`
- **Prometheus**: Available within the cluster at `http://prometheus:9090`
- **Grafana**: Available within the cluster at `http://grafana:3000`

To access these services externally, you can:

1. Create Ingress resources (recommended for production)
2. Use port-forwarding for temporary access:

```bash
# For Prometheus
kubectl port-forward svc/prometheus -n market-data 9090:9090

# For Grafana
kubectl port-forward svc/grafana -n market-data 3000:3000
```

## Customization

- **Environment Variables**: Modify the environment variables in `data-agent-deployment.yaml` to adjust the Data Agent configuration
- **Resource Limits**: Adjust the resource requests and limits in the deployment manifests based on your cluster's capacity
- **Grafana Dashboards**: Add or modify dashboards by updating the `grafana-dashboards` ConfigMap

## Troubleshooting

If you encounter issues with the deployment:

1. Check the ArgoCD application status:
   ```bash
   argocd app get data-agent
   ```

2. Check the pod logs:
   ```bash
   kubectl logs -l app=data-agent -n market-data
   kubectl logs -l app=prometheus -n market-data
   kubectl logs -l app=grafana -n market-data
   ```

3. Verify that the secrets exist and are correctly referenced in the deployments.

## CI/CD Integration

This setup can be integrated with your CI/CD pipeline:

1. On code changes, build and push a new Docker image for the Data Agent
2. Update the image tag in the `data-agent-deployment.yaml` file
3. Commit and push the changes to the repository
4. ArgoCD will automatically detect the changes and update the deployment
