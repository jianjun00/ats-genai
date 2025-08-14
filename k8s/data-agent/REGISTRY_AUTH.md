# Registry Authentication Guide for Data Agent Deployment

This guide explains how to properly set up authentication for pulling the Data Agent image from Google Container Registry (GCR) in your Kubernetes cluster.

## Prerequisites

- Google Cloud SDK (`gcloud`) installed
- `kubectl` configured to access your Kubernetes cluster
- Access to a Google Cloud project with Container Registry enabled

## Steps to Set Up GCR Authentication

### 1. Create a Service Account for Registry Access

```bash
# Create a service account for registry access
gcloud iam service-accounts create registry-access \
    --display-name="Registry Access Service Account"

# Grant the service account permission to pull from GCR
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
    --member="serviceAccount:registry-access@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"
```

### 2. Create and Download a JSON Key for the Service Account

```bash
# Create and download a JSON key
gcloud iam service-accounts keys create registry-key.json \
    --iam-account=registry-access@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

### 3. Create a Kubernetes Secret with the JSON Key

```bash
# Create a Docker registry secret using the JSON key
kubectl create secret docker-registry registry-credentials \
    --docker-server=gcr.io \
    --docker-username=_json_key \
    --docker-password="$(cat registry-key.json)" \
    --docker-email=your-email@example.com \
    --namespace=market-data
```

### 4. Update Deployment to Use the Secret

Ensure your deployment manifest includes the `imagePullSecrets` section:

```yaml
spec:
  imagePullSecrets:
  - name: registry-credentials
  containers:
  - name: data-agent
    image: gcr.io/YOUR_PROJECT_ID/data-agent:latest
    imagePullPolicy: Always
```

## Troubleshooting

If you encounter `ErrImagePull` or `ImagePullBackOff` errors:

1. Check that the secret exists in the correct namespace:
   ```bash
   kubectl get secret registry-credentials -n market-data
   ```

2. Verify the secret contains the correct data:
   ```bash
   kubectl get secret registry-credentials -n market-data -o yaml
   ```

3. Ensure the service account has the correct permissions in Google Cloud.

4. Check that the image path is correct and the image exists in the registry.

## Security Best Practices

- Rotate the service account key regularly
- Use the principle of least privilege when assigning roles
- Consider using Workload Identity (GKE) or IAM Roles for Service Accounts (EKS) for more secure authentication
