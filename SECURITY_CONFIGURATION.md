# Security Configuration for ATS Platform

## Docker Security Best Practices

### 1. Remove Privileged Containers
Replace `privileged: true` with specific capabilities:
```yaml
cap_add:
  - SYS_TIME  # Only if needed for time synchronization
  - NET_ADMIN  # Only if needed for network management
```

### 2. Use Non-Root Users
Add to Dockerfile:
```dockerfile
RUN groupadd -r ats && useradd -r -g ats ats
USER ats
```

### 3. Enable Security Scanning
Add to CI/CD pipeline:
```yaml
- name: Security Scan
  run: |
    docker run --rm -v $(pwd):/app aquasec/trivy fs /app
```

### 4. Network Isolation
Use custom networks instead of default:
```yaml
networks:
  ats-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

### 5. Resource Limits
Add resource constraints:
```yaml
deploy:
  resources:
    limits:
      cpus: '0.5'
      memory: 512M
    reservations:
      cpus: '0.25'
      memory: 256M
```
