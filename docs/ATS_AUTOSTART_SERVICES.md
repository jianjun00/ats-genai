# 🚀 ATS Autostart Services Documentation

**Complete reference for all automatically started services when WSL boots up.**

---

## 📋 **Overview**

The ATS platform automatically starts **8 services** across **2 environments** (DEV + INTG) with complete monitoring stack when WSL restarts. All services use Docker containers with `--restart unless-stopped` policy for maximum resilience.

### **Autostart Trigger**
- **Script**: `/home/jianjun/ats-genai-admin/scripts/ats_autostart.sh`
- **SystemD Service**: `ats-autostart.service` 
- **Trigger**: WSL startup → SystemD → Docker containers auto-restart
- **Log File**: `/mnt/d/ats-logs/autostart.log`

---

## 🏗️ **Complete Service Configuration**

### **🔵 ATS-DEV Environment**

#### **1. ATS-DEV PostgreSQL Database**
```bash
Container Name: ats-dev-postgres
Docker Image:   postgres:13
Network:        ats-network
External Port:  localhost:3432 → 5432 (container)
Command:        docker-entrypoint.sh postgres
Restart Policy: unless-stopped

Environment Variables:
- POSTGRES_DB=dev_db
- POSTGRES_USER=postgres  
- POSTGRES_PASSWORD=dev_password
- PGDATA=/var/lib/postgresql/data

Volume Mounts:
- postgres-data-new:/var/lib/postgresql/data    # Main database storage
- /mnt/d/ats-backup/dev:/backup                 # Backup directory

Health Check:
- Command: pg_isready -U postgres -d dev_db
- Interval: 10s, Timeout: 5s, Retries: 5
```

#### **2. ATS-DEV Analytics Service**
```bash
Container Name: ats-dev-analytics
Docker Image:   dragonflyer762/ats-genai:latest
Network:        ats-network
External Port:  localhost:3000 → 3000 (container)
Command:        python3 src/services/analytics_service.py
Restart Policy: unless-stopped
Working Dir:    /workspace

Environment Variables:
- ENVIRONMENT=dev
- DB_HOST=ats-dev-postgres
- DB_PORT=5432
- DB_USER=postgres
- DB_PASSWORD=dev_password
- DB_NAME=dev_db
- PYTHONPATH=/workspace/src

Volume Mounts:
- /home/jianjun/ats-genai-admin:/workspace       # Source code
- /mnt/d/ats-data:/data                         # Training data & minute bars
- /mnt/d/ats-backup:/backup                     # Database backups
- /mnt/d/ats-logs:/logs                         # Service logs
```

#### **3. DEV Grafana Dashboard**
```bash
Container Name: ats-grafana
Docker Image:   grafana/grafana:latest
Network:        ats-network
External Port:  localhost:3001 → 3000 (container)
Command:        /run.sh
Restart Policy: unless-stopped

Environment Variables:
- GF_SECURITY_ADMIN_PASSWORD=admin123
- GF_USERS_ALLOW_SIGN_UP=false

Volume Mounts:
- /mnt/d/ats-data/grafana:/var/lib/grafana      # Dashboard configs & data
```

---

### **🟠 ATS-INTG Environment**

#### **4. ATS-INTG PostgreSQL Database**
```bash
Container Name: ats-intg-postgres
Docker Image:   postgres:13
Network:        ats-intg-network
External Port:  localhost:4432 → 5432 (container)
Command:        docker-entrypoint.sh postgres
Restart Policy: unless-stopped

Environment Variables:
- POSTGRES_DB=intg_db
- POSTGRES_USER=postgres
- POSTGRES_PASSWORD=intg_password
- PGDATA=/var/lib/postgresql/data

Volume Mounts:
- postgres-intg-data:/var/lib/postgresql/data    # Main database storage
- /mnt/d/ats-backup/intg:/backup                 # Backup directory

Health Check:
- Command: pg_isready -U postgres -d intg_db
- Interval: 10s, Timeout: 5s, Retries: 5
```

#### **5. ATS-INTG Analytics Service**
```bash
Container Name: ats-intg-analytics
Docker Image:   dragonflyer762/ats-genai:latest
Network:        ats-intg-network
External Port:  localhost:4000 → 3000 (container)
Command:        python3 src/services/analytics_service.py
Restart Policy: unless-stopped
Working Dir:    /workspace

Environment Variables:
- ENVIRONMENT=intg
- DB_HOST=ats-intg-postgres
- DB_PORT=5432
- DB_USER=postgres
- DB_PASSWORD=intg_password
- DB_NAME=intg_db
- PYTHONPATH=/workspace/src

Volume Mounts:
- /home/jianjun/ats-genai-admin:/workspace       # Source code
- /mnt/d/ats-data:/data                         # Training data & minute bars
- /mnt/d/ats-backup:/backup                     # Database backups
- /mnt/d/ats-logs:/logs                         # Service logs
```

#### **6. INTG Grafana Dashboard**
```bash
Container Name: ats-grafana-intg
Docker Image:   grafana/grafana:10.0.0
Network:        ats-intg-network
External Port:  localhost:4002 → 3000 (container)
Command:        /run.sh
Restart Policy: unless-stopped

Environment Variables:
- GF_SECURITY_ADMIN_PASSWORD=admin123
- GF_USERS_ALLOW_SIGN_UP=false

Volume Mounts:
- /mnt/d/ats-data/grafana-intg:/var/lib/grafana  # Dashboard configs & data
```

#### **7. INTG Prometheus Metrics Service**
```bash
Container Name: ats-intg-prometheus-metrics
Docker Image:   dragonflyer762/ats-genai:latest
Network:        ats-intg-network
External Port:  localhost:4080 → 8080 (container)
Command:        python3 /workspace/scripts/prometheus_metrics_server.py
Restart Policy: unless-stopped
Working Dir:    /workspace

Environment Variables:
- ENVIRONMENT=intg
- DB_HOST=ats-intg-postgres
- DB_PORT=5432
- DB_USER=postgres
- DB_PASSWORD=intg_password
- DB_NAME=intg_db
- PYTHONPATH=/workspace/src

Volume Mounts:
- /home/jianjun/ats-genai-admin:/workspace       # Source code
- /mnt/d/ats-data:/data                         # Training data & minute bars
```

---

### **📊 Shared Monitoring Services**

#### **8. Prometheus Server**
```bash
Container Name: ats-prometheus
Docker Image:   prom/prometheus:latest
Network:        ats-network
External Port:  localhost:9090 → 9090 (container)
Command:        --config.file=/etc/prometheus/prometheus.yml
                --storage.tsdb.path=/prometheus
                --web.console.libraries=/etc/prometheus/console_libraries
                --web.console.templates=/etc/prometheus/consoles
                --web.enable-lifecycle
Restart Policy: unless-stopped

Volume Mounts:
- /home/jianjun/ats-genai-admin/config/prometheus.yml:/etc/prometheus/prometheus.yml:ro
- /mnt/d/ats-data/prometheus:/prometheus         # Metrics storage
```

---

## 🌐 **Service Access URLs**

| Service | Environment | URL | Credentials |
|---------|-------------|-----|-------------|
| **Analytics Dashboard** | DEV | http://localhost:3000/eda | None |
| **Analytics Dashboard** | INTG | http://localhost:4000/eda | None |
| **Analytics API** | DEV | http://localhost:3000/api/ | None |
| **Analytics API** | INTG | http://localhost:4000/api/ | None |
| **Grafana Dashboard** | DEV | http://localhost:3001 | admin/admin123 |
| **Grafana Dashboard** | INTG | http://localhost:4002 | admin/admin123 |
| **Prometheus** | Shared | http://localhost:9090 | None |
| **Metrics Health** | INTG | http://localhost:4080/health | None |
| **Database** | DEV | localhost:3432 | postgres/dev_password |
| **Database** | INTG | localhost:4432 | postgres/intg_password |

---

## 📁 **Volume Mount Details**

### **🗄️ Persistent Data Volumes**
```bash
# Database Storage (Docker Volumes)
postgres-data-new:/var/lib/postgresql/data      # DEV database
postgres-intg-data:/var/lib/postgresql/data     # INTG database

# Host Mount Points (D: Drive)
/mnt/d/ats-data/           # Training data, minute bars, prometheus metrics
/mnt/d/ats-backup/         # Database backups (dev/ and intg/ subdirs)
/mnt/d/ats-logs/           # Service logs and autostart logs
/mnt/d/ats-data/grafana/   # DEV Grafana dashboards & config
/mnt/d/ats-data/grafana-intg/ # INTG Grafana dashboards & config

# Source Code Mount
/home/jianjun/ats-genai-admin:/workspace        # Live source code
```

### **📊 Data Directory Structure**
```
/mnt/d/ats-data/
├── minute-bars/firstrate/    # Raw OHLCV minute data (input)
├── training-data/            # ML-ready datasets (output)  
├── checkpoints/              # API rate limiting checkpoints
├── grafana/                  # DEV Grafana data
├── grafana-intg/             # INTG Grafana data
└── prometheus/               # Prometheus metrics storage

/mnt/d/ats-backup/
├── dev/                      # DEV database backups
└── intg/                     # INTG database backups

/mnt/d/ats-logs/
├── autostart.log             # Main autostart log
├── systemd-autostart.log     # SystemD service logs
└── [service-logs]/           # Individual service logs
```

---

## 🔌 **Network Architecture**

### **Docker Networks**
- **`ats-network`**: DEV environment services
  - ats-dev-postgres (172.17.0.2)
  - ats-dev-analytics (172.17.0.3)
  - ats-grafana (172.17.0.4)
  - ats-prometheus (172.17.0.5)

- **`ats-intg-network`**: INTG environment services  
  - ats-intg-postgres (172.18.0.4)
  - ats-intg-analytics (172.18.0.3)
  - ats-grafana-intg (172.18.0.2)
  - ats-intg-prometheus-metrics (172.18.0.5)

### **Port Mapping Strategy**
- **DEV Ports**: 3000-3999 range
- **INTG Ports**: 4000-4999 range  
- **Shared Services**: 9000+ range

---

## ⚙️ **SystemD Integration**

### **Service File Location**
```bash
/etc/systemd/system/ats-autostart.service
```

### **Service Configuration**
```ini
[Unit]
Description=ATS Development Environment Autostart
After=docker.service
Requires=docker.service
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
User=jianjun
Group=jianjun
WorkingDirectory=/home/jianjun/ats-genai-admin
ExecStart=/home/jianjun/ats-genai-admin/scripts/ats_autostart.sh
RemainAfterExit=yes
StandardOutput=append:/mnt/d/ats-logs/systemd-autostart.log
StandardError=append:/mnt/d/ats-logs/systemd-autostart.log

[Install]
WantedBy=multi-user.target
```

### **Service Management Commands**
```bash
# Check service status
systemctl status ats-autostart

# Enable/disable autostart
sudo systemctl enable ats-autostart
sudo systemctl disable ats-autostart

# Manual start/stop
sudo systemctl start ats-autostart
sudo systemctl stop ats-autostart

# View logs
journalctl -u ats-autostart.service
```

---

## 🔧 **Troubleshooting**

### **Common Issues & Solutions**

#### **1. Services Not Starting**
```bash
# Check Docker daemon
sudo systemctl status docker

# Check service logs
docker logs ats-dev-analytics --tail 20
docker logs ats-intg-postgres --tail 20

# Check autostart log
tail -f /mnt/d/ats-logs/autostart.log
```

#### **2. Database Connection Issues**
```bash
# Test database connectivity
PGPASSWORD=dev_password psql -h localhost -p 3432 -U postgres -d dev_db -c "SELECT 1"
PGPASSWORD=intg_password psql -h localhost -p 4432 -U postgres -d intg_db -c "SELECT 1"

# Check Docker networks
docker network inspect ats-network
docker network inspect ats-intg-network
```

#### **3. Port Conflicts**
```bash
# Check what's using ports
netstat -tulpn | grep -E "(3000|3001|4000|4002|9090)"
sudo lsof -i :3000
sudo lsof -i :4000

# Kill conflicting processes
sudo kill $(sudo lsof -t -i:3000)
```

#### **4. Volume Mount Issues**
```bash
# Check volume permissions
ls -la /mnt/d/ats-data/
ls -la /mnt/d/ats-backup/
ls -la /mnt/d/ats-logs/

# Create missing directories
mkdir -p /mnt/d/ats-data/{grafana,grafana-intg,prometheus}
mkdir -p /mnt/d/ats-backup/{dev,intg}
mkdir -p /mnt/d/ats-logs
```

#### **5. Empty Tables in INTG**
This was caused by wrong Docker network. Fixed by ensuring:
```bash
# Verify both containers on same network
docker inspect ats-intg-postgres | grep NetworkMode
docker inspect ats-intg-analytics | grep NetworkMode
# Both should show "ats-intg-network"
```

### **Manual Recovery Commands**
```bash
# Stop all services
docker stop ats-dev-postgres ats-dev-analytics ats-intg-postgres ats-intg-analytics ats-grafana ats-grafana-intg ats-prometheus ats-intg-prometheus-metrics

# Remove containers (keeps data)
docker rm ats-dev-postgres ats-dev-analytics ats-intg-postgres ats-intg-analytics ats-grafana ats-grafana-intg ats-prometheus ats-intg-prometheus-metrics

# Restart autostart script
bash /home/jianjun/ats-genai-admin/scripts/ats_autostart.sh
```

---

## ✅ **Verification Checklist**

After WSL restart, verify all services are running:

```bash
# 1. Check all containers are running
docker ps | grep -E "(ats-dev|ats-intg|ats-prometheus|ats-grafana)"

# 2. Test database connectivity
python3 scripts/run_dev.py query --query "SELECT 1"
python3 scripts/run_intg.py query --query "SELECT 1"  

# 3. Test web services
curl -s http://localhost:3000/health | grep healthy
curl -s http://localhost:4000/health | grep healthy
curl -s http://localhost:4080/health | grep healthy

# 4. Test dashboards  
curl -s http://localhost:3001/api/health
curl -s http://localhost:4002/api/health
curl -s http://localhost:9090/-/healthy

# 5. Check restart policies
docker inspect ats-dev-analytics --format '{{.HostConfig.RestartPolicy.Name}}'
docker inspect ats-intg-analytics --format '{{.HostConfig.RestartPolicy.Name}}'
# Should show "unless-stopped" for all
```

---

## 🎯 **Success Criteria** 

**✅ All 8 services automatically start on WSL boot**  
**✅ All services have restart policies configured**  
**✅ Database connections work between containers**  
**✅ Web interfaces accessible from host**  
**✅ Persistent data survives container restarts**  
**✅ Monitoring stack captures metrics**

---

*Last Updated: September 9, 2025*  
*Total Services: 8 containers across DEV + INTG environments*  
*Auto-restart Policy: `unless-stopped` on all services*