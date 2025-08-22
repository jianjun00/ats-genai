# Minikube Usage Guide for ATS-GenAI

This guide provides instructions for using Minikube with your existing Kubernetes job scripts in the ATS-GenAI project.

## Daily Operations

### Starting and Stopping Minikube

```bash
# Start Minikube
minikube start

# Check status
minikube status

# Stop Minikube when done
minikube stop
```

### Dashboard Access

```bash
# Open Kubernetes dashboard in your browser
minikube dashboard
```

## Working with Your Existing Scripts

### Instrument Polygon Jobs

Your existing job generator and runner scripts have been updated to work with Minikube. You can continue using them as before:

```bash
# Generate a test job
python scripts/instrument_polygon_job_generator.py --job-type test --tickers "AAPL,MSFT,GOOG" --output k8s/generated/test-job.yaml

# Generate and apply a job
python scripts/run_k8s_job.py --job-type test --tickers "AAPL,MSFT" --apply
```

### GitHub Actions Workflow

The GitHub Actions workflow (`.github/workflows/instrument-polygon-job.yaml`) will continue to work as before. It now uses the Flyte workflow to dynamically generate job YAML files. It uses kubectl context configuration that is independent of your local setup.

## Testing Your Setup

A test script has been created to verify your Minikube setup works correctly with your Kubernetes jobs:

```bash
# Run a simple test job
python scripts/test_minikube_job.py

# Test an instrument polygon job
python scripts/test_minikube_job.py --job-type polygon-test --tickers "AAPL,MSFT"
```

## Accessing Services

To access services running in Minikube:

```bash
# List all services
kubectl get services -n ats-dev

# Access a specific service in your browser
minikube service <service-name> -n ats-dev
```

## Working with Databases

If your application uses databases running in Kubernetes:

```bash
# Port-forward to access the database locally
kubectl port-forward service/timescaledb 5432:5432 -n ats-dev
```

## Resource Management

Monitor and manage Minikube resources:

```bash
# View resource usage
minikube dashboard

# Adjust resources (requires restart)
minikube stop
minikube config set memory 8192
minikube config set cpus 4
minikube start
```

## Troubleshooting

### Common Issues

1. **Pod stuck in Pending state**
   ```bash
   # Check events
   kubectl describe pod <pod-name> -n ats-dev
   
   # Check available resources
   kubectl describe nodes
   ```

2. **Network connectivity issues**
   ```bash
   # Check if DNS is working
   kubectl run test-dns --image=busybox:1.28 -n ats-dev --rm -it -- nslookup kubernetes.default
   ```

3. **Image pull errors**
   ```bash
   # Check if image exists
   minikube ssh -- docker pull <image-name>
   ```

### Reset Minikube

If you encounter persistent issues:

```bash
# Delete and recreate Minikube
minikube delete
./scripts/setup_minikube_k8s.sh
```

## Additional Resources

- [Minikube Documentation](https://minikube.sigs.k8s.io/docs/)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- Migration guide: `docs/MINIKUBE_MIGRATION.md`
- Comparison between Kind and Minikube: `docs/KIND_VS_MINIKUBE.md`
