#!/usr/bin/env python3
"""
Security and Infrastructure Optimization Script

Identifies and fixes security vulnerabilities and infrastructure inefficiencies
in the ATS platform deployment configuration.

CRITICAL SECURITY ISSUES DETECTED:
- Hardcoded API keys in Docker Compose files
- Plain text credentials in version control
- Missing environment variable validation
- Insecure default configurations

Usage:
    python scripts/security_infrastructure_optimizer.py --scan
    python scripts/security_infrastructure_optimizer.py --fix-security
    python scripts/security_infrastructure_optimizer.py --optimize-docker
"""

import os
import re
import json
import yaml
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Set
from dataclasses import dataclass
import argparse


@dataclass
class SecurityVulnerability:
    """Security vulnerability information."""
    file_path: str
    line_number: int
    vulnerability_type: str
    severity: str
    description: str
    suggested_fix: str


@dataclass
class InfrastructureOptimization:
    """Infrastructure optimization opportunity."""
    component: str
    current_config: str
    optimized_config: str
    benefit: str
    impact: str


class SecurityInfrastructureOptimizer:
    """Comprehensive security and infrastructure optimizer."""

    def __init__(self):
        self.vulnerabilities: List[SecurityVulnerability] = []
        self.infrastructure_optimizations: List[InfrastructureOptimization] = []
        self.docker_compose_files: List[Path] = []
        self.api_keys_found: Set[str] = set()

    def scan_security_vulnerabilities(self) -> Dict[str, any]:
        """Scan for security vulnerabilities across the platform."""
        print("🔍 Scanning ATS Platform for Security Vulnerabilities...")

        # Find Docker Compose files
        self.docker_compose_files = list(Path('.').glob('docker-compose*.yml'))
        print(f"Found {len(self.docker_compose_files)} Docker Compose files")

        # Scan for various vulnerability types
        self._scan_hardcoded_api_keys()
        self._scan_insecure_configurations()
        self._scan_exposed_credentials()
        self._scan_network_security_issues()

        return self._generate_security_report()

    def _scan_hardcoded_api_keys(self):
        """Scan for hardcoded API keys."""
        print("🔑 Scanning for hardcoded API keys...")

        api_key_patterns = [
            (r'POLYGON_API_KEY.*?([A-Za-z0-9_]{20,})', 'Polygon API Key'),
            (r'TIINGO_API_KEY.*?([A-Za-z0-9_]{20,})', 'Tiingo API Key'),
            (r'EODHD_API_KEY.*?([A-Za-z0-9_.]{15,})', 'EODHD API Key'),
            (r'FMP_API_KEY.*?([A-Za-z0-9]{20,})', 'FMP API Key'),
            (r'ALPHA_VANTAGE_API_KEY.*?([A-Za-z0-9]{10,})', 'Alpha Vantage API Key'),
            (r'OPENAI_API_KEY.*?(sk-[A-Za-z0-9]{20,})', 'OpenAI API Key'),
            (r'FIRSTRATE_USER_ID.*?([A-Za-z0-9\-_]{5,})', 'FirstRate User ID')
        ]

        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                content = f.read()
                lines = content.splitlines()

            for line_num, line in enumerate(lines, 1):
                for pattern, key_type in api_key_patterns:
                    matches = re.findall(pattern, line)
                    if matches:
                        for match in matches:
                            if len(match) > 8:  # Skip short/demo keys
                                self.api_keys_found.add(match)
                                self.vulnerabilities.append(SecurityVulnerability(
                                    file_path=str(file_path),
                                    line_number=line_num,
                                    vulnerability_type="Hardcoded API Key",
                                    severity="CRITICAL",
                                    description=f"Hardcoded {key_type} found: {match[:8]}...",
                                    suggested_fix="Use environment variables without defaults"
                                ))

    def _scan_insecure_configurations(self):
        """Scan for insecure Docker configurations."""
        print("🔒 Scanning for insecure configurations...")

        insecure_patterns = [
            (r'privileged:\s*true', 'Privileged container', 'HIGH',
             'Container runs with privileged access', 'Use specific capabilities instead'),
            (r'security_opt:\s*-\s*seccomp:unconfined', 'Disabled seccomp', 'MEDIUM',
             'Security profiles disabled', 'Use default seccomp profile'),
            (r'restart:\s*always', 'Always restart policy', 'LOW',
             'Container always restarts', 'Use unless-stopped for better control'),
            (r'ports:\s*-\s*"(\d+):\1"', 'Direct port mapping', 'MEDIUM',
             'Port directly exposed to host', 'Use reverse proxy or firewall'),
        ]

        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                content = f.read()
                lines = content.splitlines()

            for line_num, line in enumerate(lines, 1):
                for pattern, vuln_type, severity, desc, fix in insecure_patterns:
                    if re.search(pattern, line):
                        self.vulnerabilities.append(SecurityVulnerability(
                            file_path=str(file_path),
                            line_number=line_num,
                            vulnerability_type=vuln_type,
                            severity=severity,
                            description=desc,
                            suggested_fix=fix
                        ))

    def _scan_exposed_credentials(self):
        """Scan for exposed database credentials."""
        print("🔐 Scanning for exposed credentials...")

        credential_patterns = [
            (r'DB_PASSWORD.*?([A-Za-z0-9_]{5,})', 'Database Password'),
            (r'MINIO_SECRET_KEY.*?([A-Za-z0-9_]{5,})', 'MinIO Secret Key'),
            (r'password.*?([A-Za-z0-9_]{5,})', 'Generic Password')
        ]

        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                content = f.read()
                lines = content.splitlines()

            for line_num, line in enumerate(lines, 1):
                for pattern, cred_type in credential_patterns:
                    matches = re.findall(pattern, line, re.IGNORECASE)
                    if matches and not any(placeholder in line.lower() for placeholder in ['${', 'example', 'placeholder']):
                        self.vulnerabilities.append(SecurityVulnerability(
                            file_path=str(file_path),
                            line_number=line_num,
                            vulnerability_type="Exposed Credential",
                            severity="HIGH",
                            description=f"Hardcoded {cred_type} found",
                            suggested_fix="Use Docker secrets or environment variables"
                        ))

    def _scan_network_security_issues(self):
        """Scan for network security issues."""
        print("🌐 Scanning for network security issues...")

        network_issues = [
            (r'network_mode:\s*host', 'Host network mode', 'MEDIUM',
             'Container uses host networking', 'Use custom Docker networks'),
            (r'external_links:', 'External links', 'LOW',
             'Using deprecated external links', 'Use networks instead'),
        ]

        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                content = f.read()
                lines = content.splitlines()

            for line_num, line in enumerate(lines, 1):
                for pattern, issue_type, severity, desc, fix in network_issues:
                    if re.search(pattern, line):
                        self.vulnerabilities.append(SecurityVulnerability(
                            file_path=str(file_path),
                            line_number=line_num,
                            vulnerability_type=issue_type,
                            severity=severity,
                            description=desc,
                            suggested_fix=fix
                        ))

    def _generate_security_report(self) -> Dict[str, any]:
        """Generate comprehensive security report."""
        vulnerabilities_by_severity = {
            'CRITICAL': [v for v in self.vulnerabilities if v.severity == 'CRITICAL'],
            'HIGH': [v for v in self.vulnerabilities if v.severity == 'HIGH'],
            'MEDIUM': [v for v in self.vulnerabilities if v.severity == 'MEDIUM'],
            'LOW': [v for v in self.vulnerabilities if v.severity == 'LOW']
        }

        return {
            'summary': {
                'total_vulnerabilities': len(self.vulnerabilities),
                'critical': len(vulnerabilities_by_severity['CRITICAL']),
                'high': len(vulnerabilities_by_severity['HIGH']),
                'medium': len(vulnerabilities_by_severity['MEDIUM']),
                'low': len(vulnerabilities_by_severity['LOW']),
                'api_keys_found': len(self.api_keys_found),
                'files_scanned': len(self.docker_compose_files)
            },
            'vulnerabilities_by_severity': {
                severity: [
                    {
                        'file': v.file_path,
                        'line': v.line_number,
                        'type': v.vulnerability_type,
                        'description': v.description,
                        'fix': v.suggested_fix
                    }
                    for v in vulns
                ]
                for severity, vulns in vulnerabilities_by_severity.items()
            },
            'exposed_api_keys': list(self.api_keys_found),
            'recommendations': self._generate_security_recommendations()
        }

    def _generate_security_recommendations(self) -> List[str]:
        """Generate security recommendations."""
        recommendations = []

        if any(v.vulnerability_type == "Hardcoded API Key" for v in self.vulnerabilities):
            recommendations.append(
                "🚨 CRITICAL: Remove all hardcoded API keys from Docker Compose files"
            )
            recommendations.append(
                "🔐 Use .env files or Docker secrets for sensitive credentials"
            )

        if any(v.vulnerability_type == "Privileged container" for v in self.vulnerabilities):
            recommendations.append(
                "⚠️  Remove privileged: true from containers - use specific capabilities"
            )

        if any(v.vulnerability_type == "Exposed Credential" for v in self.vulnerabilities):
            recommendations.append(
                "🔒 Move database passwords to environment variables or secrets"
            )

        recommendations.extend([
            "📝 Create .env.example with placeholder values",
            "🔍 Add pre-commit hooks to scan for credentials",
            "🛡️  Implement secret scanning in CI/CD pipeline",
            "📋 Regular security audits and penetration testing"
        ])

        return recommendations

    def scan_infrastructure_optimizations(self) -> Dict[str, any]:
        """Scan for infrastructure optimization opportunities."""
        print("⚙️  Scanning for infrastructure optimization opportunities...")

        self._analyze_docker_compose_efficiency()
        self._analyze_resource_allocation()
        self._analyze_networking_configuration()
        self._analyze_storage_optimization()

        return self._generate_infrastructure_report()

    def _analyze_docker_compose_efficiency(self):
        """Analyze Docker Compose configuration efficiency."""
        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                compose_data = yaml.safe_load(f)

            # Check for duplicate configurations
            services = compose_data.get('services', {})
            common_env_vars = self._find_common_environment_variables(services)

            if len(common_env_vars) > 3:
                self.infrastructure_optimizations.append(InfrastructureOptimization(
                    component=f"Docker Compose ({file_path})",
                    current_config="Duplicate environment variables across services",
                    optimized_config="Use .env file or YAML anchors for common variables",
                    benefit="Reduced configuration duplication",
                    impact="Easier maintenance, fewer errors"
                ))

            # Check for missing health checks
            services_without_healthcheck = [
                name for name, config in services.items()
                if 'healthcheck' not in config and 'image' in config
            ]

            if services_without_healthcheck:
                self.infrastructure_optimizations.append(InfrastructureOptimization(
                    component="Health Checks",
                    current_config=f"{len(services_without_healthcheck)} services without health checks",
                    optimized_config="Add health checks to all services",
                    benefit="Better service monitoring and reliability",
                    impact="Improved deployment reliability"
                ))

    def _find_common_environment_variables(self, services: Dict) -> List[str]:
        """Find environment variables common across services."""
        env_var_counts = {}

        for service_config in services.values():
            env_vars = service_config.get('environment', [])
            if isinstance(env_vars, dict):
                env_vars = [f"{k}={v}" for k, v in env_vars.items()]

            for env_var in env_vars:
                key = env_var.split('=')[0] if '=' in env_var else env_var
                env_var_counts[key] = env_var_counts.get(key, 0) + 1

        # Return variables that appear in multiple services
        return [key for key, count in env_var_counts.items() if count > 1]

    def _analyze_resource_allocation(self):
        """Analyze resource allocation efficiency."""
        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                compose_data = yaml.safe_load(f)

            services = compose_data.get('services', {})
            services_without_limits = []

            for service_name, service_config in services.items():
                deploy_config = service_config.get('deploy', {})
                if 'resources' not in deploy_config:
                    services_without_limits.append(service_name)

            if services_without_limits:
                self.infrastructure_optimizations.append(InfrastructureOptimization(
                    component="Resource Limits",
                    current_config=f"{len(services_without_limits)} services without resource limits",
                    optimized_config="Add memory and CPU limits to all services",
                    benefit="Better resource management and system stability",
                    impact="Prevents resource exhaustion"
                ))

    def _analyze_networking_configuration(self):
        """Analyze networking configuration."""
        network_configs = {}

        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                compose_data = yaml.safe_load(f)

            networks = compose_data.get('networks', {})
            services = compose_data.get('services', {})

            # Check for services using default network
            services_on_default = []
            for service_name, service_config in services.items():
                if 'networks' not in service_config and len(networks) > 0:
                    services_on_default.append(service_name)

            if services_on_default and networks:
                self.infrastructure_optimizations.append(InfrastructureOptimization(
                    component="Network Configuration",
                    current_config=f"{len(services_on_default)} services on default network",
                    optimized_config="Assign services to custom networks",
                    benefit="Better network isolation and security",
                    impact="Improved service isolation"
                ))

    def _analyze_storage_optimization(self):
        """Analyze storage optimization opportunities."""
        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                compose_data = yaml.safe_load(f)

            services = compose_data.get('services', {})

            # Check for bind mounts that could use volumes
            bind_mount_services = []
            for service_name, service_config in services.items():
                volumes = service_config.get('volumes', [])
                for volume in volumes:
                    if isinstance(volume, str) and ':' in volume and not volume.startswith('/'):
                        continue  # Named volume
                    elif isinstance(volume, str) and volume.startswith('./'):
                        bind_mount_services.append(service_name)
                        break

            if bind_mount_services:
                self.infrastructure_optimizations.append(InfrastructureOptimization(
                    component="Storage",
                    current_config=f"{len(bind_mount_services)} services using bind mounts",
                    optimized_config="Consider using named volumes for data persistence",
                    benefit="Better portability and backup capabilities",
                    impact="Improved data management"
                ))

    def _generate_infrastructure_report(self) -> Dict[str, any]:
        """Generate infrastructure optimization report."""
        return {
            'summary': {
                'total_optimizations': len(self.infrastructure_optimizations),
                'files_analyzed': len(self.docker_compose_files)
            },
            'optimizations': [
                {
                    'component': opt.component,
                    'current': opt.current_config,
                    'optimized': opt.optimized_config,
                    'benefit': opt.benefit,
                    'impact': opt.impact
                }
                for opt in self.infrastructure_optimizations
            ],
            'recommendations': self._generate_infrastructure_recommendations()
        }

    def _generate_infrastructure_recommendations(self) -> List[str]:
        """Generate infrastructure recommendations."""
        return [
            "📦 Use multi-stage Docker builds to reduce image size",
            "🔄 Implement container health checks for all services",
            "💾 Use named volumes instead of bind mounts for persistence",
            "🌐 Configure custom networks for service isolation",
            "📊 Add resource limits to prevent resource exhaustion",
            "🔍 Use Docker Compose override files for environment-specific configs",
            "⚡ Optimize container startup order with depends_on",
            "📝 Document all environment variables and their purposes"
        ]

    def fix_security_vulnerabilities(self):
        """Fix identified security vulnerabilities."""
        print("🔧 Fixing security vulnerabilities...")

        self._create_env_template()
        self._fix_hardcoded_credentials()
        self._add_security_headers()
        self._create_security_documentation()

        print("✅ Security fixes applied")

    def _create_env_template(self):
        """Create .env template file with secure defaults."""
        env_template_content = """# ATS Platform Environment Configuration Template
# Copy this file to .env and fill in your actual values

# ============================================================================
# CRITICAL: Never commit actual API keys to version control!
# ============================================================================

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_secure_password_here
DB_NAME=ats_db

# Market Data API Keys (Required for data collection)
POLYGON_API_KEY=your_polygon_api_key_here
TIINGO_API_KEY=your_tiingo_api_key_here
EODHD_API_KEY=your_eodhd_api_key_here
FMP_API_KEY=your_fmp_api_key_here
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key_here
FIRSTRATE_USER_ID=your_firstrate_user_id_here

# AI/LLM API Keys (Optional)
OPENAI_API_KEY=your_openai_api_key_here

# Application Configuration
ENVIRONMENT=dev
LOG_LEVEL=INFO
DEBUG=false

# Security Configuration
ENABLE_CORS=false
ALLOWED_ORIGINS=http://localhost:3000,http://localhost:4000
SECRET_KEY=your_secret_key_here

# Storage Configuration
ATS_DATA_PATH=/mnt/d/ats-data
ATS_BACKUP_PATH=/mnt/d/ats-backup
ATS_LOGS_PATH=/mnt/d/ats-logs

# ============================================================================
# API Key Setup Instructions:
# ============================================================================
# 1. Polygon: https://polygon.io/dashboard (Free tier: 5 calls/min)
# 2. Tiingo: https://api.tiingo.com/account/api-token (Free tier: 1000 calls/hr)
# 3. EODHD: https://eodhd.com/cp/dashboard (Free tier: 20 calls/min)
# 4. FMP: https://financialmodelingprep.com/developer/docs (Free tier: 250 calls/day)
# 5. Alpha Vantage: https://www.alphavantage.co/support/#api-key (Free tier: 25 calls/day)
# ============================================================================
"""

        Path('.env.template').write_text(env_template_content)
        print("📝 Created .env.template with secure configuration")

    def _fix_hardcoded_credentials(self):
        """Fix hardcoded credentials in Docker Compose files."""
        for file_path in self.docker_compose_files:
            with open(file_path, 'r') as f:
                content = f.read()

            # Replace hardcoded API keys with environment variable references
            api_key_replacements = {
                r'POLYGON_API_KEY=\$\{POLYGON_API_KEY:-[^}]+\}': 'POLYGON_API_KEY=${POLYGON_API_KEY}',
                r'TIINGO_API_KEY=\$\{TIINGO_API_KEY:-[^}]+\}': 'TIINGO_API_KEY=${TIINGO_API_KEY}',
                r'EODHD_API_KEY=\$\{EODHD_API_KEY:-[^}]+\}': 'EODHD_API_KEY=${EODHD_API_KEY}',
                r'FMP_API_KEY=\$\{FMP_API_KEY:-[^}]+\}': 'FMP_API_KEY=${FMP_API_KEY}',
                r'ALPHA_VANTAGE_API_KEY=\$\{ALPHA_VANTAGE_API_KEY:-[^}]+\}': 'ALPHA_VANTAGE_API_KEY=${ALPHA_VANTAGE_API_KEY}',
            }

            original_content = content
            for pattern, replacement in api_key_replacements.items():
                content = re.sub(pattern, replacement, content)

            if content != original_content:
                # Create backup
                backup_path = f"{file_path}.security_backup"
                Path(backup_path).write_text(original_content)

                # Write fixed content
                Path(file_path).write_text(content)
                print(f"🔒 Fixed hardcoded credentials in {file_path}")

    def _add_security_headers(self):
        """Add security configuration suggestions."""
        security_config = """# Security Configuration for ATS Platform

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
"""

        Path('SECURITY_CONFIGURATION.md').write_text(security_config)
        print("📋 Created security configuration guide")

    def _create_security_documentation(self):
        """Create security documentation."""
        security_docs = """# ATS Platform Security Guide

## 🚨 Critical Security Requirements

### Environment Variables
- **NEVER** commit API keys or passwords to version control
- Use `.env` files for local development (add to .gitignore)
- Use Docker secrets or cloud secret managers for production

### API Key Management
1. Copy `.env.template` to `.env`
2. Fill in actual API keys from vendor dashboards
3. Verify `.env` is in `.gitignore`
4. Rotate API keys regularly

### Database Security
- Use strong passwords (minimum 16 characters)
- Enable SSL/TLS for database connections
- Restrict database access to specific IPs
- Regular database backups with encryption

### Container Security
- Run containers as non-root users
- Use specific image tags (avoid `:latest`)
- Regular security scans with Trivy or Snyk
- Minimize attack surface with multi-stage builds

### Network Security
- Use custom Docker networks for isolation
- Enable firewall rules for exposed ports
- Use reverse proxy (nginx/traefik) for external access
- Regular penetration testing

## 🔍 Security Monitoring
- Monitor logs for suspicious activity
- Set up alerts for failed authentication attempts
- Regular security audits and updates
- Incident response procedures

## 📞 Emergency Response
If you discover a security vulnerability:
1. Do NOT commit fixes to public repositories
2. Create private security patch
3. Coordinate disclosure responsibly
4. Update all affected deployments immediately
"""

        Path('SECURITY_GUIDE.md').write_text(security_docs)
        print("📚 Created comprehensive security guide")


def main():
    """Main function for security and infrastructure optimization."""
    parser = argparse.ArgumentParser(description="Security and Infrastructure Optimizer")
    parser.add_argument('--scan', action='store_true', help='Scan for security vulnerabilities')
    parser.add_argument('--infrastructure', action='store_true', help='Analyze infrastructure optimizations')
    parser.add_argument('--fix-security', action='store_true', help='Fix security vulnerabilities')
    parser.add_argument('--report', default='security_report.json', help='Output report file')

    args = parser.parse_args()

    optimizer = SecurityInfrastructureOptimizer()

    if args.scan or not any([args.infrastructure, args.fix_security]):
        # Security scan
        security_report = optimizer.scan_security_vulnerabilities()

        print("\n" + "="*80)
        print("🚨 ATS PLATFORM SECURITY VULNERABILITY REPORT")
        print("="*80)

        summary = security_report['summary']
        print(f"\n📊 VULNERABILITY SUMMARY:")
        print(f"  Total vulnerabilities: {summary['total_vulnerabilities']}")
        print(f"  Critical: {summary['critical']}")
        print(f"  High: {summary['high']}")
        print(f"  Medium: {summary['medium']}")
        print(f"  Low: {summary['low']}")
        print(f"  API keys found: {summary['api_keys_found']}")

        # Show critical vulnerabilities
        critical_vulns = security_report['vulnerabilities_by_severity']['CRITICAL']
        if critical_vulns:
            print(f"\n🚨 CRITICAL VULNERABILITIES ({len(critical_vulns)}):")
            for vuln in critical_vulns[:5]:  # Show first 5
                print(f"  ❌ {vuln['file']}:{vuln['line']} - {vuln['description']}")

        # Show recommendations
        print(f"\n💡 SECURITY RECOMMENDATIONS:")
        for rec in security_report['recommendations']:
            print(f"  {rec}")

        # Save full report
        with open(args.report, 'w') as f:
            json.dump(security_report, f, indent=2)
        print(f"\n✅ Full security report saved: {args.report}")

    if args.infrastructure:
        # Infrastructure analysis
        infra_report = optimizer.scan_infrastructure_optimizations()

        print(f"\n⚙️  INFRASTRUCTURE OPTIMIZATION OPPORTUNITIES:")
        print(f"  Total optimizations: {infra_report['summary']['total_optimizations']}")

        for opt in infra_report['optimizations'][:5]:  # Show first 5
            print(f"\n  🔧 {opt['component']}:")
            print(f"     Current: {opt['current']}")
            print(f"     Optimized: {opt['optimized']}")
            print(f"     Benefit: {opt['benefit']}")

    if args.fix_security:
        # Apply security fixes
        optimizer.fix_security_vulnerabilities()
        print(f"\n🔒 Security fixes applied!")
        print(f"📝 Review .env.template and SECURITY_GUIDE.md for next steps")

    print(f"\n🚀 Security and infrastructure analysis complete!")


if __name__ == "__main__":
    main()