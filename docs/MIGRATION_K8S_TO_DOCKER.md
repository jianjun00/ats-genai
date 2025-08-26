# 🚀 Migration Guide: Kubernetes → Docker Development

**Migration from K8s-first to Docker+GPU-first development approach**

---

## 📋 Why We Migrated

### ✅ **Benefits of New Docker Approach:**
- **🎮 GPU Support**: Native NVIDIA GPU integration for ML/AI workloads
- **⚡ Faster Setup**: No complex K8s cluster configuration
- **🐳 Simpler Infrastructure**: Docker containers vs K8s complexity  
- **💻 Local Development**: Everything runs on localhost
- **🔧 Easier Debugging**: Direct container access and logs

### ❌ **Issues with K8s Approach:**
- **GPU Integration**: Complex device plugin setup for NVIDIA GPUs
- **Development Friction**: Heavy infrastructure for development workflow
- **WSL2 Limitations**: Minikube GPU passthrough challenges
- **Over-Engineering**: K8s complexity for single-developer tasks

---

## 🔄 Command Migration Map

### Database Operations
```bash
# OLD: K8s approach
kubectl exec -n ats-dev deployment/postgres -- psql -U postgres -d dev_db -c "SELECT 1"

# NEW: Docker approach  
python scripts/run_dev.py query --query "SELECT 1"
```

### Running Scripts/Jobs
```bash
# OLD: K8s jobs
python scripts/run_dev.py deploy --file k8s/price-unification-job.yaml
python scripts/run_dev.py logs --job job-name

# NEW: Docker execution
python scripts/run_dev.py run --script scripts/data_generation/create_sample_data.py
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu  # With GPU
```

### Service Management
```bash
# OLD: K8s services
kubectl get pods -n ats-dev
kubectl port-forward svc/postgres -n ats-dev 5433:5432

# NEW: Docker services
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py start --service analytics  
python scripts/run_dev.py status
python scripts/run_dev.py logs --service analytics
```

### Testing
```bash
# OLD: Manual PYTHONPATH
PYTHONPATH=src pytest tests/integration/ -v

# NEW: Automated Docker testing
python scripts/run_dev.py test
python scripts/run_dev.py test --test tests/integration/
```

---

## 🛠️ Migration Steps

### 1. **Update Your Workflow**
Replace all `kubectl` and manual `PYTHONPATH` commands:

```bash
# Setup development environment
python scripts/run_dev.py setup

# Check what's running
python scripts/run_dev.py status

# Run your scripts
python scripts/run_dev.py run --script your_script.py
```

### 2. **GPU Workloads**
For ML/AI development, add `--gpu` flag:

```bash
# ML training with GPU support
python scripts/run_dev.py run --script scripts/training/train_model.py --gpu

# Data processing with GPU
python scripts/run_dev.py run --script scripts/data_analysis/gpu_analysis.py --gpu
```

### 3. **Service Dependencies**
Start required services before running scripts:

```bash
# Start database
python scripts/run_dev.py start --service postgres

# Start analytics service
python scripts/run_dev.py start --service analytics

# Run your script that needs these services
python scripts/run_dev.py run --script your_script.py
```

### 4. **Update Documentation References**
- Replace K8s commands with Docker equivalents
- Update team runbooks and guides
- Remove references to `kubectl` for development

---

## 🔍 What Changed Under the Hood

### **Database Connections**
- **Before**: Port-forwarded K8s PostgreSQL on `localhost:5433`
- **After**: Direct PostgreSQL container on `localhost:5432`
- **Auto-detection**: `run_dev` automatically finds available connection

### **Script Execution**
- **Before**: K8s Job YAML files + container scheduling
- **After**: Direct Docker container execution
- **Benefits**: Faster startup, GPU access, simpler debugging

### **Service Management**
- **Before**: K8s deployments, services, ingress
- **After**: Docker containers with port mapping
- **Benefits**: Localhost access, simplified networking

### **Testing**
- **Before**: Manual `PYTHONPATH` management
- **After**: Automated Docker test execution
- **Benefits**: Consistent environment, no path issues

---

## 🚨 Important Notes

### **Production Deployment**
- **Development**: Docker containers (new approach)
- **Production**: Cloud Kubernetes (GKE/EKS/AKS) - unchanged
- **CI/CD**: GitHub Actions - still builds containers

### **GPU Access**
- **Development**: `--gpu` flag for Docker containers
- **Production**: GPU node pools in cloud K8s

### **Database**
- **Development**: Local PostgreSQL container
- **Production**: Cloud database services

---

## 🎯 Quick Reference

### **Most Common Commands**
```bash
# Daily workflow
python scripts/run_dev.py setup      # Start dev environment
python scripts/run_dev.py status     # Check what's running
python scripts/run_dev.py test       # Run tests

# Database work
python scripts/run_dev.py query --query "SELECT COUNT(*) FROM table"

# Script execution
python scripts/run_dev.py run --script path/to/script.py
python scripts/run_dev.py run --script ml_script.py --gpu  # With GPU

# Service management  
python scripts/run_dev.py start --service postgres
python scripts/run_dev.py logs --service analytics
python scripts/run_dev.py stop --service analytics
```

### **Troubleshooting**
- **Database issues**: `python scripts/run_dev.py start --service postgres`
- **Service not starting**: Check Docker Desktop is running
- **GPU not working**: Verify Docker Desktop GPU support
- **Tests failing**: `python scripts/run_dev.py setup` first

---

**🎉 Migration Complete! You're now using the modern Docker+GPU development approach.**