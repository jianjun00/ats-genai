#!/usr/bin/env python3
"""
ATS Credential Validation Script

Validates that all required credentials are properly configured across environments.
Helps prevent deployment issues due to missing or incorrect credentials.

Usage:
    python scripts/k8s/validate_credentials.py
    python scripts/k8s/validate_credentials.py --env dev
    python scripts/k8s/validate_credentials.py --fix-missing
"""

import argparse
import subprocess
import base64
import json
from typing import Dict, List, Tuple

# Expected credential structure
CREDENTIAL_SCHEMA = {
    "db-credentials": {
        "required_keys": ["DB_USER", "DB_PASSWORD", "DB_NAME", "DB_HOST", "DB_PORT"],
        "optional_keys": []
    },
    "api-keys": {
        "required_keys": [],
        "optional_keys": ["POLYGON_API_KEY", "TIINGO_API_KEY", "FINNHUB_API_KEY"]
    }
}

# Environment configurations
ENVIRONMENTS = {
    "dev": {
        "namespace": "ats-dev",
        "expected_values": {
            "DB_USER": "postgres",
            "DB_PASSWORD": "dev_password",
            "DB_NAME": "dev_db",
            "DB_HOST": "postgres",
            "DB_PORT": "5432"
        }
    },
    "intg": {
        "namespace": "ats-intg", 
        "expected_values": {
            "DB_USER": "postgres",
            "DB_PASSWORD": "intg_password",
            "DB_NAME": "intg_db",
            "DB_HOST": "postgres-intg",
            "DB_PORT": "5432"
        }
    },
    "prod": {
        "namespace": "ats-prod",
        "expected_values": {
            "DB_USER": "postgres",
            "DB_PASSWORD": "prod_secure_password",
            "DB_NAME": "prod_db", 
            "DB_HOST": "postgres-prod",
            "DB_PORT": "5432"
        }
    }
}

class CredentialValidator:
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.successes = []
    
    def run_kubectl(self, command: List[str]) -> Tuple[bool, str]:
        """Run kubectl command and return success status and output"""
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=30)
            return result.returncode == 0, result.stdout if result.returncode == 0 else result.stderr
        except subprocess.TimeoutExpired:
            return False, "Command timed out"
        except Exception as e:
            return False, str(e)
    
    def decode_secret_data(self, secret_data: Dict[str, str]) -> Dict[str, str]:
        """Decode base64 secret values"""
        decoded = {}
        for key, value in secret_data.items():
            try:
                decoded[key] = base64.b64decode(value).decode('utf-8')
            except Exception as e:
                decoded[key] = f"DECODE_ERROR: {e}"
        return decoded
    
    def validate_secret_exists(self, secret_name: str, namespace: str) -> bool:
        """Check if secret exists in namespace"""
        success, output = self.run_kubectl([
            "kubectl", "get", "secret", secret_name, "-n", namespace, "--no-headers"
        ])
        
        if success:
            self.successes.append(f"✅ Secret exists: {secret_name} in {namespace}")
            return True
        else:
            self.errors.append(f"❌ Missing secret: {secret_name} in {namespace}")
            return False
    
    def validate_secret_structure(self, secret_name: str, namespace: str, schema_key: str) -> Dict[str, str]:
        """Validate secret has required structure and return decoded values"""
        success, output = self.run_kubectl([
            "kubectl", "get", "secret", secret_name, "-n", namespace, "-o", "json"
        ])
        
        if not success:
            self.errors.append(f"❌ Cannot read secret: {secret_name} in {namespace} - {output}")
            return {}
        
        try:
            secret_json = json.loads(output)
            secret_data = secret_json.get("data", {})
            decoded_data = self.decode_secret_data(secret_data)
            
            # Validate required keys
            schema = CREDENTIAL_SCHEMA[schema_key]
            missing_keys = []
            
            for required_key in schema["required_keys"]:
                if required_key not in decoded_data:
                    missing_keys.append(required_key)
            
            if missing_keys:
                self.errors.append(f"❌ Secret {secret_name} missing keys: {missing_keys}")
            else:
                self.successes.append(f"✅ Secret {secret_name} has all required keys")
            
            return decoded_data
            
        except json.JSONDecodeError as e:
            self.errors.append(f"❌ Invalid JSON in secret {secret_name}: {e}")
            return {}
        except Exception as e:
            self.errors.append(f"❌ Error reading secret {secret_name}: {e}")
            return {}
    
    def validate_credential_values(self, decoded_data: Dict[str, str], environment: str, secret_name: str):
        """Validate credential values match expected values"""
        expected = ENVIRONMENTS[environment]["expected_values"]
        
        for key, expected_value in expected.items():
            if key in decoded_data:
                actual_value = decoded_data[key]
                if actual_value == expected_value:
                    self.successes.append(f"✅ {secret_name}.{key} = {expected_value} (correct)")
                else:
                    self.errors.append(f"❌ {secret_name}.{key} = '{actual_value}' (expected '{expected_value}')")
            else:
                self.errors.append(f"❌ {secret_name} missing key: {key}")
    
    def validate_environment(self, environment: str) -> bool:
        """Validate all credentials for an environment"""
        if environment not in ENVIRONMENTS:
            self.errors.append(f"❌ Unknown environment: {environment}")
            return False
        
        env_config = ENVIRONMENTS[environment]
        namespace = env_config["namespace"]
        
        print(f"\n🔍 Validating {environment} environment ({namespace})...")
        
        # Check namespace exists
        success, _ = self.run_kubectl(["kubectl", "get", "namespace", namespace, "--no-headers"])
        if not success:
            self.errors.append(f"❌ Namespace does not exist: {namespace}")
            return False
        
        self.successes.append(f"✅ Namespace exists: {namespace}")
        
        # Validate database credentials
        db_secret_name = f"db-credentials-{environment}"
        if self.validate_secret_exists(db_secret_name, namespace):
            decoded_data = self.validate_secret_structure(db_secret_name, namespace, "db-credentials")
            if decoded_data:
                self.validate_credential_values(decoded_data, environment, db_secret_name)
        
        # Validate API keys (optional)
        api_secret_name = f"api-keys-{environment}"
        if self.validate_secret_exists(api_secret_name, namespace):
            self.validate_secret_structure(api_secret_name, namespace, "api-keys")
        else:
            self.warnings.append(f"⚠️  Optional secret missing: {api_secret_name}")
        
        return len(self.errors) == 0
    
    def create_missing_secrets(self, environment: str):
        """Create missing secrets with default values"""
        env_config = ENVIRONMENTS[environment]
        namespace = env_config["namespace"]
        
        print(f"\n🔧 Creating missing secrets for {environment}...")
        
        # Create database credentials secret
        db_secret_name = f"db-credentials-{environment}"
        success, _ = self.run_kubectl([
            "kubectl", "get", "secret", db_secret_name, "-n", namespace, "--no-headers"
        ])
        
        if not success:
            expected = env_config["expected_values"]
            create_cmd = [
                "kubectl", "create", "secret", "generic", db_secret_name,
                "-n", namespace,
                f"--from-literal=DB_USER={expected['DB_USER']}",
                f"--from-literal=DB_PASSWORD={expected['DB_PASSWORD']}",
                f"--from-literal=DB_NAME={expected['DB_NAME']}",
                f"--from-literal=DB_HOST={expected['DB_HOST']}",
                f"--from-literal=DB_PORT={expected['DB_PORT']}"
            ]
            
            success, output = self.run_kubectl(create_cmd)
            if success:
                self.successes.append(f"✅ Created secret: {db_secret_name}")
            else:
                self.errors.append(f"❌ Failed to create secret {db_secret_name}: {output}")
        
        # Create API keys secret (empty)
        api_secret_name = f"api-keys-{environment}"
        success, _ = self.run_kubectl([
            "kubectl", "get", "secret", api_secret_name, "-n", namespace, "--no-headers"
        ])
        
        if not success:
            create_cmd = [
                "kubectl", "create", "secret", "generic", api_secret_name,
                "-n", namespace,
                "--from-literal=POLYGON_API_KEY=",
                "--from-literal=TIINGO_API_KEY=",
                "--from-literal=FINNHUB_API_KEY="
            ]
            
            success, output = self.run_kubectl(create_cmd)
            if success:
                self.successes.append(f"✅ Created secret: {api_secret_name}")
            else:
                self.errors.append(f"❌ Failed to create secret {api_secret_name}: {output}")
    
    def print_summary(self):
        """Print validation summary"""
        print("\n" + "="*80)
        print("🔐 CREDENTIAL VALIDATION SUMMARY")
        print("="*80)
        
        if self.successes:
            print(f"\n✅ SUCCESSES ({len(self.successes)}):")
            for success in self.successes:
                print(f"  {success}")
        
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  {warning}")
        
        if self.errors:
            print(f"\n❌ ERRORS ({len(self.errors)}):")
            for error in self.errors:
                print(f"  {error}")
        
        print(f"\n📊 SUMMARY:")
        print(f"  ✅ Successes: {len(self.successes)}")
        print(f"  ⚠️  Warnings: {len(self.warnings)}")
        print(f"  ❌ Errors: {len(self.errors)}")
        
        if len(self.errors) == 0:
            print(f"\n🎉 ALL CREDENTIALS VALIDATED SUCCESSFULLY!")
        else:
            print(f"\n🚨 VALIDATION FAILED - {len(self.errors)} ERRORS FOUND")

def main():
    parser = argparse.ArgumentParser(description="Validate ATS Kubernetes credentials")
    parser.add_argument("--env", choices=list(ENVIRONMENTS.keys()), 
                       help="Validate specific environment (default: all)")
    parser.add_argument("--fix-missing", action="store_true",
                       help="Create missing secrets with default values")
    parser.add_argument("--check-values", action="store_true", default=True,
                       help="Validate credential values match expected (default: true)")
    
    args = parser.parse_args()
    
    validator = CredentialValidator()
    
    environments_to_check = [args.env] if args.env else list(ENVIRONMENTS.keys())
    
    print("🔐 ATS Credential Validation")
    print("="*50)
    
    for env in environments_to_check:
        if args.fix_missing:
            validator.create_missing_secrets(env)
        
        validator.validate_environment(env)
    
    validator.print_summary()
    
    # Exit with error code if validation failed
    exit_code = 1 if validator.errors else 0
    exit(exit_code)

if __name__ == "__main__":
    main()