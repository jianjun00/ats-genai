# Migrating from Docker Desktop Kubernetes to Minikube

This document provides guidance on migrating from Docker Desktop Kubernetes to Minikube for the ATS-GenAI project.

## Migration Summary

The ATS-GenAI project has been updated to use Minikube instead of Docker Desktop for local Kubernetes development. This change was necessary because Docker Desktop is no longer working as expected.

## What Has Been Done

1. **Minikube Setup Script**: Created a setup script at `scripts/setup_minikube_k8s.sh` that:
   - Installs Minikube if not already installed
   - Configures Minikube with appropriate resources (2 CPUs, 4GB RAM)
   - Enables useful addons (dashboard, metrics-server, ingress)
   - Creates the `ats-dev` namespace
   - Sets up kubectl context

2. **Script Updates**: Updated Kubernetes-related scripts to work with Minikube:
   - Changed references from `docker-desktop` context to `minikube`
   - Updated Kubernetes config paths
   - Modified Kubernetes status checks

3. **Alternative Kind Setup**: Also provided a Kind (Kubernetes IN Docker) setup script at `scripts/setup_kind_k8s.sh` as an alternative option.

## How to Use Minikube

### Initial Setup

```bash
# Make the script executable
chmod +x scripts/setup_minikube_k8s.sh

# Run the setup script
./scripts/setup_minikube_k8s.sh
```

### Daily Usage

```bash
# Start Minikube (if not already running)
minikube start

# Check status
minikube status

# Access dashboard
minikube dashboard

# Stop Minikube when done
minikube stop
```

### Using with Existing Scripts

The existing Kubernetes job scripts (`scripts/run_k8s_job.py`, `scripts/instrument_polygon_job_generator.py`) have been updated to work with Minikube. You can continue using them as before:

```bash
# Example: Generate and apply a test job
python scripts/run_k8s_job.py --job-type test --tickers "AAPL,MSFT" --apply
```

## Differences from Docker Desktop

1. **Service Access**: To access services in Minikube, use:
   ```bash
   minikube service <service-name> --namespace ats-dev
   ```

2. **Resource Usage**: Minikube runs in a VM or container and has explicitly allocated resources (2 CPUs, 4GB RAM by default).

3. **Persistence**: Minikube's state is preserved between restarts, but will be lost if you delete the cluster.

## Troubleshooting

If you encounter issues:

1. **Check Minikube Status**:
   ```bash
   minikube status
   ```

2. **View Logs**:
   ```bash
   minikube logs
   ```

3. **Restart Minikube**:
   ```bash
   minikube stop
   minikube start
   ```

4. **Reset Minikube** (as a last resort):
   ```bash
   minikube delete
   ./scripts/setup_minikube_k8s.sh
   ```

## Additional Resources

- [Minikube Documentation](https://minikube.sigs.k8s.io/docs/)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- Comparison between Kind and Minikube: `docs/KIND_VS_MINIKUBE.md`
