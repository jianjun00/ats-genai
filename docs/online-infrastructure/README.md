# ☁️ Online Infrastructure

**Kubernetes, CI/CD, Monitoring, and Platform Operations**

The Online Infrastructure component provides the foundational platform that hosts, orchestrates, and monitors all other ATS components. It handles container orchestration, automated deployments, security, monitoring, and incident response.

---

## 🎯 Component Overview

### **Core Capabilities**
- **Kubernetes Orchestration**: Multi-environment container management (dev/intg/prod)
- **CI/CD Automation**: GitHub Actions, ArgoCD GitOps, automated testing
- **Monitoring Stack**: Prometheus, Grafana, AlertManager, comprehensive dashboards
- **Security Management**: Authentication, authorization, secrets, network policies
- **Infrastructure as Code**: Kubernetes manifests, Helm charts, Terraform
- **Incident Response**: Automated alerting, runbooks, escalation procedures

### **Key Technologies**
- **Kubernetes**: Container orchestration platform
- **ArgoCD**: GitOps continuous deployment
- **GitHub Actions**: CI/CD pipeline automation
- **Prometheus/Grafana**: Monitoring and observability
- **Helm**: Kubernetes package management
- **Docker**: Container runtime and image management

---

## 📚 Documentation Structure

### **🏗️ Architecture & Design**
- **[SYSTEM_DESIGN.md](SYSTEM_DESIGN.md)** - Infrastructure architecture, network design, security patterns
- Kubernetes cluster design and networking
- CI/CD pipeline architecture
- Security and compliance frameworks

### **⚙️ Operations & Deployment**
- **[OPERATIONS.md](OPERATIONS.md)** - DevOps procedures, monitoring, incident response
- Infrastructure monitoring and alerting
- Backup and disaster recovery procedures
- Security operations and incident response

### **📋 Product & Planning**
- **[prd/](prd/)** - Product Requirements Documents
- **[drd/](drd/)** - Detailed Requirements Documents
- Infrastructure roadmap and capacity planning
- Platform feature specifications

---

## 🚀 Quick Start

### Cluster Operations
```bash
# Check cluster health
kubectl cluster-info
kubectl get nodes -o wide
kubectl top nodes

# Verify core services
kubectl get pods -n kube-system
kubectl get pods -n argocd
kubectl get pods -n monitoring
```

### Service Deployment
```bash
# Deploy using GitOps workflow
./scripts/dev_deploy.sh

# Monitor deployment progress
./scripts/monitor_deployment.sh service-name

# Check external access
./scripts/get_external_access.sh all
```

### Monitoring Access
```bash
# Access monitoring dashboards
kubectl port-forward service/grafana 3000:3000 -n monitoring
kubectl port-forward service/prometheus 9090:9090 -n monitoring
kubectl port-forward service/alertmanager 9093:9093 -n monitoring

# ArgoCD UI access
kubectl port-forward service/argocd-server 8080:80 -n argocd
```

---

## 🏗️ Infrastructure Architecture

### **Multi-Environment Setup**
```
┌─────────────────────────────────────────────────────────────────┐
│                    KUBERNETES CLUSTER                          │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│   Development   │  │   Integration   │  │   Production    │
│   (ats-dev)     │  │   (ats-intg)    │  │   (ats-prod)    │
│                 │  │                 │  │                 │
│ • Rapid Iteration│ • Weekly Testing  │ • Live Customer   │
│ • Auto-deploy   │  │ • QA Validation │  │ • Manual Deploy │
│ • Real-time logs│  │ • Performance   │  │ • High Security │
│ • Debug tools   │  │   Testing       │  │ • Compliance    │
└─────────────────┘  └─────────────────┘  └─────────────────┘
          │                    │                    │
          └────────────────────┼────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SHARED INFRASTRUCTURE                       │
│                                                                 │
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐    │
│ │ ArgoCD GitOps   │ │ Monitoring      │ │ Security        │    │
│ │                 │ │                 │ │                 │    │
│ │ • Auto Sync     │ │ • Prometheus    │ │ • RBAC          │    │
│ │ • Health Checks │ │ • Grafana       │ │ • Network Pol   │    │
│ │ • Rollback      │ │ • AlertManager  │ │ • Secrets Mgmt  │    │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘    │
│                                                                 │
│ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐    │
│ │ CI/CD Pipeline  │ │ Container Reg   │ │ Backup/Storage  │    │
│ │                 │ │                 │ │                 │    │
│ │ • GitHub Actions│ │ • Docker Hub    │ │ • Persistent    │    │
│ │ • Build/Test    │ │ • Image Scan    │ │   Volumes       │    │
│ │ • Security Scan │ │ • Multi-arch    │ │ • Snapshot      │    │
│ └─────────────────┘ └─────────────────┘ └─────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### **Network Architecture**
- **Ingress Controller**: NGINX for external traffic routing
- **Service Mesh**: Istio for inter-service communication (planned)
- **Load Balancing**: Kubernetes native load balancing
- **External Access**: NodePort services for development, LoadBalancer for production

---

## 🔄 CI/CD Pipeline Architecture

### **GitHub Actions Workflow**
```yaml
# Automated CI/CD pipeline stages
Pipeline Flow:
  
  1. Code Push/PR
      ↓
  2. Security Scanning (Trivy, CodeQL)
      ↓  
  3. Unit Tests (pytest, coverage)
      ↓
  4. Integration Tests (K8s cluster)
      ↓
  5. Build Docker Images (multi-arch)
      ↓
  6. Push to Registry (GitHub CR)
      ↓
  7. Update K8s Manifests
      ↓
  8. ArgoCD Auto-Sync
      ↓
  9. Health Checks & Verification
      ↓
  10. Slack Notifications
```

### **Environment Promotion Strategy**
| Environment | Trigger | Approval | Testing |
|-------------|---------|----------|---------|
| **dev** | Every commit to main | None | Unit + Integration |
| **intg** | Weekly schedule | Auto | System + Performance |
| **prod** | Manual trigger | Required | Full regression |

---

## 📊 Monitoring & Observability

### **Infrastructure Metrics**
- **Cluster Health**: Node utilization, pod distribution, resource availability
- **Application Health**: Service uptime, response times, error rates
- **Resource Usage**: CPU, memory, storage, network utilization
- **Security Events**: Authentication failures, suspicious activities

### **Key Dashboards**
- **Cluster Overview**: Node status, namespace utilization, resource consumption
- **Application Health**: Service status, API performance, business metrics
- **CI/CD Pipeline**: Build success rates, deployment frequency, lead time
- **Security Dashboard**: Authentication events, security policy violations

---

## 🔒 Security & Compliance

### **Security Framework**
- **RBAC**: Role-based access control for all cluster resources
- **Network Policies**: Microsegmentation between services
- **Secrets Management**: Kubernetes secrets with rotation policies
- **Container Security**: Image scanning, runtime security monitoring
- **Audit Logging**: Comprehensive logging of all cluster activities

### **Compliance Requirements**
- **SOC 2 Type II**: Security controls and monitoring
- **FINRA Compliance**: Financial industry regulations
- **Data Protection**: GDPR/CCPA compliance for user data
- **Audit Trail**: Complete activity logging and retention

---

## 🔗 Related Components

- **[🔧 Backend Platform](../backend-platform/)** - Hosts application services
- **[📊 Data Infrastructure](../data-infrastructure/)** - Orchestrates data processing jobs
- **[🤖 ML Platform](../ml-platform/)** - Manages training and inference workloads

---

## 📊 Key Metrics & SLAs

- **Cluster Availability**: 99.9% uptime
- **Deployment Success Rate**: > 98%
- **Mean Time to Recovery**: < 15 minutes
- **Security Incident Response**: < 5 minutes detection, < 30 minutes response
- **Resource Efficiency**: > 70% cluster utilization

---

## 👥 Team Ownership

- **Primary Team**: DevOps, Platform Engineering
- **Secondary Teams**: Security, Site Reliability Engineering
- **Key Contacts**: DevOps Lead, Platform Architect

---

*For infrastructure automation workflows, see the [📖 main documentation hub](../README.md)*