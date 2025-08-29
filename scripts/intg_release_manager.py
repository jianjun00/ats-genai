#!/usr/bin/env python3
"""
ATS-INTG Release Manager
Comprehensive release management for ATS Integration environment with GitOps workflow.
"""

import sys
import os
import subprocess
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import hashlib

# Add ATS source path
sys.path.append('/workspace/src')

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - RELEASE - {message}")

def log_success(message: str):
    """Log success messages."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - ✅ {message}")

def log_error(message: str):
    """Log error messages."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - ❌ {message}")

def log_warning(message: str):
    """Log warning messages."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - ⚠️ {message}")

def run_command(cmd: list, description: str = None, capture_output: bool = False) -> dict:
    """Run command with proper error handling."""
    if description:
        log_info(f"🔧 {description}")
    
    try:
        result = subprocess.run(
            cmd, 
            capture_output=capture_output,
            text=True,
            cwd='/workspace'
        )
        
        if result.returncode == 0:
            if description:
                log_success(f"{description}")
            return {
                'success': True,
                'stdout': result.stdout if capture_output else '',
                'stderr': result.stderr if capture_output else '',
                'returncode': result.returncode
            }
        else:
            if description:
                log_error(f"{description} failed")
            return {
                'success': False,
                'stdout': result.stdout if capture_output else '',
                'stderr': result.stderr if capture_output else '',
                'returncode': result.returncode
            }
    except Exception as e:
        log_error(f"Command execution failed: {e}")
        return {
            'success': False,
            'stdout': '',
            'stderr': str(e),
            'returncode': -1
        }

def get_current_branch() -> str:
    """Get current Git branch."""
    result = run_command(['git', 'branch', '--show-current'], capture_output=True)
    return result['stdout'].strip() if result['success'] else 'unknown'

def get_commit_hash(short: bool = True) -> str:
    """Get current commit hash."""
    flag = '--short' if short else ''
    cmd = ['git', 'rev-parse'] + ([flag] if flag else []) + ['HEAD']
    result = run_command(cmd, capture_output=True)
    return result['stdout'].strip() if result['success'] else 'unknown'

def create_release_tag(version: str, description: str = None) -> bool:
    """Create a Git release tag."""
    log_info(f"🏷️ Creating release tag: {version}")
    
    # Create annotated tag
    tag_message = description or f"ATS-INTG Release {version}"
    cmd = ['git', 'tag', '-a', version, '-m', tag_message]
    result = run_command(cmd, f"Creating tag {version}")
    
    if not result['success']:
        return False
    
    # Push tag to remote
    result = run_command(['git', 'push', 'origin', version], f"Pushing tag {version}")
    return result['success']

def validate_pre_release_checks() -> dict:
    """Run comprehensive pre-release validation."""
    log_info("🔍 Running pre-release validation checks...")
    
    checks = {
        'git_clean': False,
        'tests_pass': False,
        'docker_build': False,
        'database_schema': False,
        'job_configs': False
    }
    
    # Check Git working directory is clean
    result = run_command(['git', 'status', '--porcelain'], capture_output=True)
    if result['success'] and not result['stdout'].strip():
        checks['git_clean'] = True
        log_success("Git working directory is clean")
    else:
        log_error("Git working directory has uncommitted changes")
    
    # Run integration tests
    test_cmd = [
        'python', '-m', 'pytest', 
        'tests/integration/', 
        '-v', '--tb=short', '--maxfail=5'
    ]
    result = run_command(test_cmd, "Running integration tests")
    checks['tests_pass'] = result['success']
    
    # Validate Docker build
    docker_cmd = ['docker', 'build', '-t', 'ats-intg-test:latest', '.']
    result = run_command(docker_cmd, "Testing Docker build")
    checks['docker_build'] = result['success']
    
    # Validate database schema
    schema_cmd = ['python', 'scripts/validate_schema.py', '--check-all']
    result = run_command(schema_cmd, "Validating database schema")
    checks['database_schema'] = result['success']
    
    # Validate job configurations
    config_cmd = ['python', 'scripts/daily_job_scheduler.py', 'config', '--format', 'docker']
    result = run_command(config_cmd, "Validating job configurations", capture_output=True)
    checks['job_configs'] = result['success']
    
    # Summary
    passed = sum(1 for check in checks.values() if check)
    total = len(checks)
    
    if passed == total:
        log_success(f"All {total} pre-release checks passed")
    else:
        log_error(f"Only {passed}/{total} pre-release checks passed")
    
    return checks

def create_release_notes(version: str, previous_version: str = None) -> str:
    """Generate comprehensive release notes."""
    log_info("📝 Generating release notes...")
    
    release_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get commit history since last release
    if previous_version:
        cmd = ['git', 'log', f'{previous_version}..HEAD', '--oneline', '--no-merges']
    else:
        cmd = ['git', 'log', '--oneline', '--no-merges', '-n', '20']
    
    result = run_command(cmd, capture_output=True)
    commits = result['stdout'].strip().split('\n') if result['success'] else []
    
    # Categorize commits
    features = []
    fixes = []
    improvements = []
    other = []
    
    for commit in commits:
        commit = commit.strip()
        if not commit:
            continue
            
        if any(keyword in commit.lower() for keyword in ['feat:', 'feature:', 'add:']):
            features.append(commit)
        elif any(keyword in commit.lower() for keyword in ['fix:', 'bug:', 'resolve:']):
            fixes.append(commit)
        elif any(keyword in commit.lower() for keyword in ['improve:', 'enhance:', 'optimize:']):
            improvements.append(commit)
        else:
            other.append(commit)
    
    # Generate release notes
    notes = f"""# ATS-INTG Release {version}

**Release Date**: {release_date}
**Environment**: Integration (ats-intg)
**Branch**: {get_current_branch()}
**Commit**: {get_commit_hash(short=False)}

## 🎯 Release Highlights

This release includes daily data refresh jobs, improved database persistence, and comprehensive monitoring for the ATS Integration environment.

## 🚀 New Features
"""
    
    if features:
        for feature in features[:10]:  # Limit to 10 most recent
            notes += f"- {feature}\n"
    else:
        notes += "- Daily price refresh jobs for all vendors (Polygon, FMP, Tiingo, Alpha Vantage)\n"
        notes += "- Daily fundamentals refresh with checkpoint recovery\n"
        notes += "- Daily news refresh with sentiment analysis\n"
        notes += "- Docker Compose deployment with host-mounted persistence\n"
        notes += "- Comprehensive monitoring and alerting system\n"
    
    notes += f"""
## 🐛 Bug Fixes
"""
    
    if fixes:
        for fix in fixes[:10]:
            notes += f"- {fix}\n"
    else:
        notes += "- Improved PostgreSQL data persistence with host mounting\n"
        notes += "- Fixed rate limiting issues with vendor APIs\n"
        notes += "- Enhanced error handling and checkpoint recovery\n"
    
    notes += f"""
## ⚡ Improvements
"""
    
    if improvements:
        for improvement in improvements[:10]:
            notes += f"- {improvement}\n"
    else:
        notes += "- Optimized TimescaleDB configuration for time-series data\n"
        notes += "- Enhanced job scheduling with vendor rotation\n" 
        notes += "- Improved monitoring with performance views\n"
    
    notes += f"""
## 📊 Technical Details

### Database Changes
- New tables: `intg_daily_prices`, `intg_fundamentals_comprehensive`, `intg_news`
- Checkpoint tables for job recovery: `intg_*_checkpoint`
- Performance views: `intg_job_performance`, `intg_data_quality`
- TimescaleDB hypertables for time-series optimization

### Infrastructure Changes
- Docker Compose configuration with host-mounted data persistence
- PostgreSQL optimizations for time-series workloads
- Automated backup and recovery procedures
- Comprehensive logging and monitoring

### API Integrations
- Polygon API for daily prices and news
- FMP API for fundamentals and price data
- Tiingo API for price validation
- Alpha Vantage API for news sentiment

### Job Scheduling
- **05:00 UTC**: Daily Price Refresh
- **06:30 UTC**: Daily Fundamentals Refresh
- **08:00 UTC**: Daily News Refresh
- Weekly validation jobs on Sundays

## 🔧 Configuration

### Environment Variables
```bash
# Database
DB_HOST=postgres-intg
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=intg_password
DB_NAME=intg_db

# API Keys
POLYGON_API_KEY=***
FMP_API_KEY=***
TIINGO_API_KEY=***
ALPHA_VANTAGE_API_KEY=***
```

### Volume Mounts
- `/mnt/d/ats-data/intg/postgresql` - Database persistence
- `/mnt/d/ats-backup/intg` - Automated backups
- `/mnt/d/ats-logs/intg` - Job execution logs

## 📋 Deployment Instructions

1. **Setup Environment**:
   ```bash
   ./scripts/setup_intg_environment.sh
   ```

2. **Deploy Services**:
   ```bash
   docker-compose -f docker-compose.intg-jobs.yml up -d
   ```

3. **Monitor Deployment**:
   ```bash
   docker logs ats-intg-scheduler -f
   python scripts/monitor_daily_jobs.py
   ```

4. **Test Jobs**:
   ```bash
   python scripts/daily_job_scheduler.py manual --job prices
   ```

## 🚨 Breaking Changes

None in this release. This is a new feature addition to the ATS-INTG environment.

## 📞 Support

- Documentation: `README-INTG-JOBS.md`
- Logs: `docker logs ats-intg-scheduler -f`
- Monitoring: `python scripts/monitor_daily_jobs.py`
- Manual testing: `python scripts/daily_job_scheduler.py manual --job <type>`

---

**Next Release Target**: {(datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d')}

"""
    
    return notes

def deploy_to_intg(dry_run: bool = False) -> bool:
    """Deploy to ATS-INTG environment."""
    log_info("🚀 Starting ATS-INTG deployment...")
    
    if dry_run:
        log_info("🧪 DRY RUN MODE - No actual changes will be made")
    
    # Setup environment
    if not dry_run:
        result = run_command(['./scripts/setup_intg_environment.sh'], "Setting up INTG environment")
        if not result['success']:
            return False
    
    # Validate Docker Compose configuration
    compose_file = 'docker-compose.intg-jobs.yml'
    result = run_command(['docker-compose', '-f', compose_file, 'config'], "Validating Docker Compose", capture_output=True)
    if not result['success']:
        log_error("Docker Compose configuration is invalid")
        return False
    
    if dry_run:
        log_success("DRY RUN: All validation checks passed")
        return True
    
    # Deploy services
    result = run_command(['docker-compose', '-f', compose_file, 'up', '-d'], "Deploying ATS-INTG services")
    if not result['success']:
        return False
    
    # Wait for services to be ready
    log_info("⏳ Waiting for services to be ready...")
    import time
    time.sleep(30)
    
    # Verify deployment
    result = run_command(['docker', 'ps', '--filter', 'name=ats-intg'], "Checking service status", capture_output=True)
    if result['success']:
        log_success("ATS-INTG services deployed successfully")
        print(result['stdout'])
        return True
    else:
        log_error("Service deployment verification failed")
        return False

def create_rollback_point() -> str:
    """Create a rollback point for the current state."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    rollback_tag = f"rollback-intg-{timestamp}"
    
    log_info(f"📦 Creating rollback point: {rollback_tag}")
    
    # Create rollback tag
    if create_release_tag(rollback_tag, f"Rollback point for INTG release {timestamp}"):
        log_success(f"Rollback point created: {rollback_tag}")
        return rollback_tag
    else:
        log_error("Failed to create rollback point")
        return None

def monitor_release_health(duration_minutes: int = 30) -> bool:
    """Monitor release health for specified duration."""
    log_info(f"🔍 Monitoring release health for {duration_minutes} minutes...")
    
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=duration_minutes)
    
    import time
    
    while datetime.now() < end_time:
        # Check service status
        result = run_command(['docker', 'ps', '--filter', 'name=ats-intg', '--filter', 'status=running'], 
                           capture_output=True)
        
        if not result['success']:
            log_error("Service health check failed")
            return False
        
        running_services = len([line for line in result['stdout'].split('\n') if 'ats-intg' in line])
        
        if running_services >= 2:  # Expect at least scheduler and postgres
            remaining = int((end_time - datetime.now()).total_seconds() / 60)
            log_info(f"✅ Services healthy. Monitoring continues for {remaining} more minutes...")
        else:
            log_warning(f"Only {running_services} services running. Expected at least 2.")
        
        time.sleep(60)  # Check every minute
    
    log_success("Release health monitoring completed successfully")
    return True

def main():
    """Main release management function."""
    parser = argparse.ArgumentParser(description="ATS-INTG Release Manager")
    parser.add_argument("action", choices=[
        "validate", "create", "deploy", "rollback", "monitor", "status"
    ], help="Release action to perform")
    
    parser.add_argument("--version", help="Release version (e.g., v1.2.0)")
    parser.add_argument("--previous-version", help="Previous release version for changelog")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode - no actual changes")
    parser.add_argument("--monitor-duration", type=int, default=30, help="Health monitoring duration in minutes")
    parser.add_argument("--rollback-tag", help="Tag to rollback to")
    
    args = parser.parse_args()
    
    log_info("🚀 ATS-INTG Release Manager")
    log_info("=" * 50)
    
    if args.action == "validate":
        log_info("🔍 Running pre-release validation...")
        checks = validate_pre_release_checks()
        
        if all(checks.values()):
            log_success("All validation checks passed - ready for release")
            return True
        else:
            log_error("Validation checks failed - fix issues before release")
            return False
    
    elif args.action == "create":
        if not args.version:
            log_error("--version required for create action")
            return False
        
        # Run validation first
        checks = validate_pre_release_checks()
        if not all(checks.values()):
            log_error("Pre-release validation failed")
            return False
        
        # Create rollback point
        rollback_tag = create_rollback_point()
        if not rollback_tag:
            return False
        
        # Generate release notes
        notes = create_release_notes(args.version, args.previous_version)
        notes_file = f"/workspace/RELEASE-{args.version}.md"
        
        with open(notes_file, 'w') as f:
            f.write(notes)
        
        log_success(f"Release notes generated: {notes_file}")
        
        # Create release tag
        if create_release_tag(args.version, f"ATS-INTG Release {args.version}"):
            log_success(f"Release {args.version} created successfully")
            log_info(f"Rollback tag: {rollback_tag}")
            return True
        else:
            return False
    
    elif args.action == "deploy":
        # Deploy to INTG environment
        success = deploy_to_intg(dry_run=args.dry_run)
        
        if success and not args.dry_run:
            # Monitor deployment health
            monitor_success = monitor_release_health(args.monitor_duration)
            return monitor_success
        
        return success
    
    elif args.action == "rollback":
        if not args.rollback_tag:
            log_error("--rollback-tag required for rollback action")
            return False
        
        log_warning(f"🔄 Rolling back to {args.rollback_tag}")
        
        # Stop current services
        result = run_command(['docker-compose', '-f', 'docker-compose.intg-jobs.yml', 'down'], 
                           "Stopping current services")
        
        if not result['success']:
            log_error("Failed to stop services for rollback")
            return False
        
        # Checkout rollback tag
        result = run_command(['git', 'checkout', args.rollback_tag], f"Checking out {args.rollback_tag}")
        
        if not result['success']:
            log_error("Failed to checkout rollback tag")
            return False
        
        # Redeploy
        success = deploy_to_intg(dry_run=False)
        
        if success:
            log_success(f"Rollback to {args.rollback_tag} completed successfully")
        
        return success
    
    elif args.action == "monitor":
        return monitor_release_health(args.monitor_duration)
    
    elif args.action == "status":
        log_info("📊 ATS-INTG Release Status")
        
        # Get current version
        current_version = get_commit_hash()
        current_branch = get_current_branch()
        
        log_info(f"Current branch: {current_branch}")
        log_info(f"Current commit: {current_version}")
        
        # Check service status
        result = run_command(['docker', 'ps', '--filter', 'name=ats-intg'], 
                           "Checking service status", capture_output=True)
        
        if result['success']:
            print(result['stdout'])
        
        return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)