#!/usr/bin/env python3
"""
Deploy Enhanced Analytics Webapp to Kubernetes

Creates ConfigMap with webapp source and deploys to ats-dev namespace
"""
import subprocess
import tempfile
import yaml
import os
from pathlib import Path

def create_enhanced_webapp_deployment():
    """Create complete deployment with webapp source code"""
    
    # Read the webapp source code
    webapp_source = Path('unified_backtest_analytics_webapp.py').read_text()
    
    # Create deployment YAML with embedded source
    deployment_yaml = f"""
apiVersion: v1
kind: ConfigMap
metadata:
  name: enhanced-webapp-source
  namespace: ats-dev
data:
  unified_backtest_analytics_webapp.py: |
{webapp_source.replace(chr(10), chr(10) + '    ')}

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: enhanced-webapp-config
  namespace: ats-dev
data:
  startup.py: |
    #!/usr/bin/env python3
    import subprocess
    import os
    import sys
    
    def main():
        print("🚀 Starting Enhanced Analytics Webapp in Kubernetes...")
        
        # Install dependencies
        print("📦 Installing dependencies...")
        subprocess.run([
            'pip', 'install', '--no-cache-dir',
            'fastapi==0.104.1', 'uvicorn[standard]==0.24.0', 
            'asyncpg==0.29.0', 'pydantic==2.5.0',
            'numpy==1.24.3', 'pandas==2.0.3'
        ], check=True)
        
        # Setup directories
        print("📁 Setting up directories...")
        os.makedirs('/app/src/config', exist_ok=True)
        os.makedirs('/app/training_data_output', exist_ok=True)
        
        # Copy webapp source
        print("📄 Copying webapp source...")
        import shutil
        shutil.copy('/source/unified_backtest_analytics_webapp.py', '/app/')
        
        # Create minimal environment config
        with open('/app/src/config/__init__.py', 'w') as f:
            f.write('')
            
        with open('/app/src/config/environment.py', 'w') as f:
            f.write('''
import os

class Environment:
    def __init__(self):
        self.env_type = os.getenv('ENVIRONMENT', 'dev')
        
    def get_table_name(self, base_name):
        return f"{{self.env_type}}_{{base_name}}"
        
    def get_database_url(self):
        host = os.getenv('DB_HOST', 'postgres-simple')
        port = os.getenv('DB_PORT', '5432') 
        user = os.getenv('DB_USER', 'postgres')
        password = os.getenv('DB_PASSWORD', 'dev_password')
        db_name = os.getenv('DB_NAME', 'dev_db')
        return f"postgresql://{{user}}:{{password}}@{{host}}:{{port}}/{{db_name}}"
''')
        
        # Set environment and run webapp
        print("🌐 Starting webapp...")
        os.chdir('/app')
        os.environ['PYTHONPATH'] = '/app/src'
        
        subprocess.run(['python', 'unified_backtest_analytics_webapp.py'], check=True)
    
    if __name__ == "__main__":
        main()

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: enhanced-analytics-webapp
  namespace: ats-dev
  labels:
    app: enhanced-analytics-webapp
    component: webapp
spec:
  replicas: 1
  selector:
    matchLabels:
      app: enhanced-analytics-webapp
  template:
    metadata:
      labels:
        app: enhanced-analytics-webapp
        component: webapp
    spec:
      containers:
      - name: webapp
        image: python:3.12-slim
        ports:
        - containerPort: 3000
        command: ["python"]
        args: ["/config/startup.py"]
        volumeMounts:
        - name: config-volume
          mountPath: /config
        - name: source-volume
          mountPath: /source
        env:
        - name: ENVIRONMENT
          value: "dev"
        - name: DB_HOST
          value: "postgres-simple"
        - name: DB_PORT
          value: "5432"
        - name: DB_USER
          value: "postgres"
        - name: DB_PASSWORD
          value: "dev_password"
        - name: DB_NAME
          value: "dev_db"
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
      volumes:
      - name: config-volume
        configMap:
          name: enhanced-webapp-config
          defaultMode: 0755
      - name: source-volume
        configMap:
          name: enhanced-webapp-source

---
apiVersion: v1
kind: Service
metadata:
  name: enhanced-analytics-webapp-service
  namespace: ats-dev
spec:
  selector:
    app: enhanced-analytics-webapp
  ports:
  - port: 3000
    targetPort: 3000
    nodePort: 30002
  type: NodePort
"""
    
    return deployment_yaml

def deploy_to_kubernetes():
    """Deploy the enhanced webapp to Kubernetes"""
    print("🚀 Creating enhanced webapp deployment...")
    
    # Generate deployment YAML
    deployment_yaml = create_enhanced_webapp_deployment()
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(deployment_yaml)
        temp_file = f.name
    
    try:
        # Apply to Kubernetes
        print("📦 Applying to Kubernetes...")
        result = subprocess.run([
            'kubectl', 'apply', '-f', temp_file
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Enhanced webapp deployed successfully!")
            print(f"🌐 Access at: http://192.168.49.2:30002/")
            print("📊 Features: Job Runs + Training Data sections")
            print("🔗 Real database connectivity to postgres-simple")
            return True
        else:
            print(f"❌ Deployment failed: {result.stderr}")
            return False
            
    finally:
        # Cleanup temp file
        os.unlink(temp_file)

if __name__ == "__main__":
    deploy_to_kubernetes()