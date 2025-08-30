#!/usr/bin/env python3
"""
Production Deployment Script for Real-Time Market Data Collector

Deploys the real-time collector with proper configuration and monitoring.
Handles database initialization, API key validation, and service startup.
"""

import subprocess
import sys
import os
import time
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('deploy')

class RealtimeCollectorDeployment:
    """Deployment manager for real-time collector."""
    
    def __init__(self):
        self.namespace = "ats-dev"
        self.deployment_name = "realtime-collector"
        self.service_name = "realtime-collector-service"
        
    def check_prerequisites(self):
        """Check deployment prerequisites."""
        logger.info("🔍 Checking deployment prerequisites...")
        
        # Check if kubectl is available
        try:
            result = subprocess.run(['kubectl', 'version', '--client'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info("✅ kubectl available")
            else:
                logger.error("❌ kubectl not available")
                return False
        except Exception as e:
            logger.error(f"❌ kubectl check failed: {e}")
            return False
        
        # Check if namespace exists
        try:
            result = subprocess.run(['kubectl', 'get', 'namespace', self.namespace], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                logger.info(f"✅ Namespace {self.namespace} exists")
            else:
                logger.info(f"⚠️ Namespace {self.namespace} doesn't exist, creating...")
                self.create_namespace()
        except Exception as e:
            logger.error(f"❌ Namespace check failed: {e}")
            return False
        
        # Check API keys secret
        if not self.check_api_keys_secret():
            self.create_api_keys_secret()
        
        return True
    
    def create_namespace(self):
        """Create the ATS development namespace."""
        try:
            result = subprocess.run([
                'kubectl', 'create', 'namespace', self.namespace
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                logger.info(f"✅ Created namespace {self.namespace}")
            else:
                logger.error(f"❌ Failed to create namespace: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"❌ Namespace creation failed: {e}")
            return False
        
        return True
    
    def check_api_keys_secret(self):
        """Check if API keys secret exists."""
        try:
            result = subprocess.run([
                'kubectl', 'get', 'secret', 'api-keys', '-n', self.namespace
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                logger.info("✅ API keys secret exists")
                return True
            else:
                logger.info("⚠️ API keys secret doesn't exist")
                return False
        except Exception as e:
            logger.error(f"❌ API keys secret check failed: {e}")
            return False
    
    def create_api_keys_secret(self):
        """Create API keys secret from environment variables."""
        logger.info("🔐 Creating API keys secret...")
        
        # Get API keys from environment or .env.test file
        api_keys = {}
        
        # Try to source .env.test if it exists
        env_file = ".env.test"
        if os.path.exists(env_file):
            logger.info("📁 Loading API keys from .env.test")
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key] = value
        
        # Extract API keys
        key_mapping = {
            'polygon-api-key': 'POLYGON_API_KEY',
            'tiingo-api-key': 'TIINGO_API_KEY',
            'fmp-api-key': 'FMP_API_KEY',
            'alpha-vantage-api-key': 'ALPHA_VANTAGE_API_KEY'
        }
        
        for k8s_key, env_key in key_mapping.items():
            value = os.getenv(env_key, '')
            if value:
                api_keys[k8s_key] = value
                logger.info(f"✅ Found {env_key}")
            else:
                logger.warning(f"⚠️ {env_key} not found")
        
        if not api_keys:
            logger.error("❌ No API keys found")
            return False
        
        # Create secret using kubectl
        cmd = ['kubectl', 'create', 'secret', 'generic', 'api-keys', '-n', self.namespace]
        
        for k8s_key, value in api_keys.items():
            cmd.extend([f'--from-literal={k8s_key}={value}'])
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                logger.info(f"✅ Created API keys secret with {len(api_keys)} keys")
                return True
            else:
                logger.error(f"❌ Failed to create secret: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"❌ Secret creation failed: {e}")
            return False
    
    def check_database_connectivity(self):
        """Check if database is accessible from cluster."""
        logger.info("🔌 Checking database connectivity...")
        
        try:
            # Test database connection via run_dev.py
            result = subprocess.run([
                'python3', 'scripts/run_dev.py', 'query', 
                '--query', 'SELECT version();'
            ], capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                logger.info("✅ Database is accessible")
                return True
            else:
                logger.error(f"❌ Database connection failed: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"❌ Database connectivity check failed: {e}")
            return False
    
    def deploy_collector(self):
        """Deploy the real-time collector."""
        logger.info("🚀 Deploying real-time collector...")
        
        try:
            # Apply the deployment YAML
            result = subprocess.run([
                'kubectl', 'apply', '-f', 'k8s/realtime-collector-deployment.yaml'
            ], capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                logger.info("✅ Deployment applied successfully")
                return True
            else:
                logger.error(f"❌ Deployment failed: {result.stderr}")
                return False
        except Exception as e:
            logger.error(f"❌ Deployment failed: {e}")
            return False
    
    def wait_for_deployment(self, timeout_minutes=10):
        """Wait for deployment to be ready."""
        logger.info("⏳ Waiting for deployment to be ready...")
        
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()
        
        while (time.time() - start_time) < timeout_seconds:
            try:
                # Check deployment status
                result = subprocess.run([
                    'kubectl', 'get', 'deployment', self.deployment_name, 
                    '-n', self.namespace, '-o', 'json'
                ], capture_output=True, text=True, timeout=10)
                
                if result.returncode == 0:
                    deployment_info = json.loads(result.stdout)
                    status = deployment_info.get('status', {})
                    ready_replicas = status.get('readyReplicas', 0)
                    replicas = status.get('replicas', 0)
                    
                    if ready_replicas >= replicas and replicas > 0:
                        logger.info(f"✅ Deployment ready: {ready_replicas}/{replicas} replicas")
                        return True
                    else:
                        logger.info(f"⏳ Waiting for replicas: {ready_replicas}/{replicas}")
                
            except Exception as e:
                logger.warning(f"⚠️ Status check error: {e}")
            
            time.sleep(10)
        
        logger.error("❌ Deployment timed out")
        return False
    
    def get_service_endpoint(self):
        """Get the service endpoint for external access."""
        logger.info("🔗 Getting service endpoint...")
        
        try:
            # Get service info
            result = subprocess.run([
                'kubectl', 'get', 'service', self.service_name, 
                '-n', self.namespace, '-o', 'json'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                service_info = json.loads(result.stdout)
                spec = service_info.get('spec', {})
                ports = spec.get('ports', [])
                
                if ports:
                    node_port = ports[0].get('nodePort')
                    if node_port:
                        # Get node IP
                        node_result = subprocess.run([
                            'kubectl', 'get', 'nodes', '-o', 
                            'jsonpath={.items[0].status.addresses[?(@.type=="ExternalIP")].address}'
                        ], capture_output=True, text=True, timeout=10)
                        
                        if node_result.returncode == 0 and node_result.stdout:
                            node_ip = node_result.stdout.strip()
                            endpoint = f"http://{node_ip}:{node_port}"
                        else:
                            # Fallback to internal IP or localhost
                            node_result = subprocess.run([
                                'kubectl', 'get', 'nodes', '-o', 
                                'jsonpath={.items[0].status.addresses[?(@.type=="InternalIP")].address}'
                            ], capture_output=True, text=True, timeout=10)
                            
                            if node_result.returncode == 0 and node_result.stdout:
                                node_ip = node_result.stdout.strip()
                                endpoint = f"http://{node_ip}:{node_port}"
                            else:
                                endpoint = f"http://localhost:{node_port}"
                        
                        logger.info(f"✅ Service endpoint: {endpoint}")
                        return endpoint
            
            logger.warning("⚠️ Could not determine service endpoint")
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get service endpoint: {e}")
            return None
    
    def test_health_endpoint(self, endpoint):
        """Test the health endpoint."""
        if not endpoint:
            return False
        
        logger.info("🏥 Testing health endpoint...")
        
        try:
            import requests
            health_url = f"{endpoint}/health"
            
            # Try multiple times as the service starts up
            for attempt in range(5):
                try:
                    response = requests.get(health_url, timeout=10)
                    if response.status_code == 200:
                        health_data = response.json()
                        logger.info(f"✅ Health check passed: {health_data.get('status')}")
                        return True
                    else:
                        logger.warning(f"⚠️ Health check failed: HTTP {response.status_code}")
                except requests.exceptions.RequestException as e:
                    logger.warning(f"⚠️ Health check attempt {attempt + 1}: {e}")
                
                if attempt < 4:
                    time.sleep(10)
            
            logger.error("❌ Health endpoint not responding")
            return False
            
        except ImportError:
            logger.warning("⚠️ requests module not available, skipping health test")
            return True
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return False
    
    def show_status(self):
        """Show deployment status and useful commands."""
        logger.info("📊 Deployment Status:")
        
        # Show pods
        try:
            result = subprocess.run([
                'kubectl', 'get', 'pods', '-n', self.namespace, 
                '-l', f'app={self.deployment_name}'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                logger.info("Pods:")
                for line in result.stdout.strip().split('\n'):
                    logger.info(f"   {line}")
        except Exception as e:
            logger.warning(f"Could not get pod status: {e}")
        
        # Show useful commands
        logger.info("\n📋 Useful Commands:")
        logger.info(f"   View logs: kubectl logs -f deployment/{self.deployment_name} -n {self.namespace}")
        logger.info(f"   Get status: kubectl get all -n {self.namespace}")
        logger.info(f"   Delete deployment: kubectl delete -f k8s/realtime-collector-deployment.yaml")
        
        endpoint = self.get_service_endpoint()
        if endpoint:
            logger.info(f"   Health check: curl {endpoint}/health")
            logger.info(f"   Status: curl {endpoint}/status")
            logger.info(f"   Metrics: curl {endpoint}/metrics")
    
    def deploy(self):
        """Main deployment method."""
        logger.info("🎯 Starting Real-Time Collector Deployment")
        logger.info("=" * 60)
        
        # Check prerequisites
        if not self.check_prerequisites():
            logger.error("❌ Prerequisites check failed")
            return False
        
        # Check database connectivity
        if not self.check_database_connectivity():
            logger.error("❌ Database connectivity check failed")
            return False
        
        # Deploy collector
        if not self.deploy_collector():
            logger.error("❌ Collector deployment failed")
            return False
        
        # Wait for deployment to be ready
        if not self.wait_for_deployment():
            logger.error("❌ Deployment readiness check failed")
            return False
        
        # Get service endpoint
        endpoint = self.get_service_endpoint()
        
        # Test health endpoint
        self.test_health_endpoint(endpoint)
        
        # Show final status
        self.show_status()
        
        logger.info("\n🎉 Real-Time Collector Deployment Complete!")
        return True

def main():
    """Main entry point."""
    deployment = RealtimeCollectorDeployment()
    
    try:
        success = deployment.deploy()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("❌ Deployment cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Deployment failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()