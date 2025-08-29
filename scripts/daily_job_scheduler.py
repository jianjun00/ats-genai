#!/usr/bin/env python3
"""
Daily Job Scheduler for ATS-INTG Environment

Provides optimal scheduling recommendations and deployment configuration
for daily refresh jobs (prices, fundamentals, news) across all vendors.

SCHEDULING RECOMMENDATIONS:

1. **STAGGERED EXECUTION** - Jobs run at different times to avoid API rate limits
2. **VENDOR ROTATION** - Different vendors prioritized on different days  
3. **FAILURE RESILIENCE** - Jobs can restart from checkpoints
4. **RESOURCE OPTIMIZATION** - Conservative threading and rate limiting
5. **MONITORING INTEGRATION** - Comprehensive logging and status tracking

OPTIMAL DAILY SCHEDULE:
- 05:00 UTC: Daily Price Refresh (yesterday's closing prices available)
- 06:30 UTC: Daily Fundamentals Refresh (quarterly/annual data updates)
- 08:00 UTC: Daily News Refresh (overnight news accumulation)

VENDOR PRIORITY ROTATION:
- Monday/Thursday: Polygon → FMP → Alpha Vantage → Tiingo
- Tuesday/Friday: FMP → Polygon → Tiingo → Alpha Vantage  
- Wednesday/Saturday: Tiingo → Alpha Vantage → Polygon → FMP
- Sunday: Alpha Vantage → Tiingo → FMP → Polygon
"""

import sys
import os
import subprocess
import json
from datetime import datetime, timedelta
import argparse

# Add ATS source path
sys.path.append('/workspace/src')

def log_info(message: str):
    """Enhanced logging with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - SCHEDULER - {message}")

def get_vendor_priority_for_day(day_of_week: int) -> list:
    """Get vendor priority order based on day of week (0=Monday, 6=Sunday)."""
    priority_schedules = {
        0: ['polygon', 'fmp', 'alpha_vantage', 'tiingo'],  # Monday
        1: ['fmp', 'polygon', 'tiingo', 'alpha_vantage'],  # Tuesday
        2: ['tiingo', 'alpha_vantage', 'polygon', 'fmp'],  # Wednesday
        3: ['polygon', 'fmp', 'alpha_vantage', 'tiingo'],  # Thursday
        4: ['fmp', 'polygon', 'tiingo', 'alpha_vantage'],  # Friday
        5: ['tiingo', 'alpha_vantage', 'polygon', 'fmp'],  # Saturday
        6: ['alpha_vantage', 'tiingo', 'fmp', 'polygon']   # Sunday
    }
    
    return priority_schedules.get(day_of_week, priority_schedules[0])

def create_cron_configuration() -> str:
    """Generate cron configuration for daily jobs."""
    
    cron_config = """
# ATS-INTG Daily Refresh Jobs
# Timezone: UTC (adjust for your system timezone)

# Daily Price Refresh - 05:00 UTC (after market close, before pre-market)
0 5 * * * cd /workspace && python scripts/run_intg.py run --script scripts/daily_price_refresh_job.py --env '{"JOB_TYPE":"daily_prices"}' >> /logs/daily_prices.log 2>&1

# Daily Fundamentals Refresh - 06:30 UTC (staggered after price refresh)
30 6 * * * cd /workspace && python scripts/run_intg.py run --script scripts/daily_fundamentals_refresh_job.py --env '{"JOB_TYPE":"daily_fundamentals"}' >> /logs/daily_fundamentals.log 2>&1

# Daily News Refresh - 08:00 UTC (after overnight news accumulation)
0 8 * * * cd /workspace && python scripts/run_intg.py run --script scripts/daily_news_refresh_job.py --env '{"JOB_TYPE":"daily_news"}' >> /logs/daily_news.log 2>&1

# Weekly Full Refresh - Sundays at 02:00 UTC (comprehensive data validation)
0 2 * * 0 cd /workspace && python scripts/run_intg.py run --script scripts/weekly_data_validation_job.py --env '{"JOB_TYPE":"weekly_validation"}' >> /logs/weekly_validation.log 2>&1

# Health Check - Every hour during business hours (12:00-22:00 UTC)
0 12-22 * * 1-5 cd /workspace && python scripts/run_intg.py query --query "SELECT 'ATS-INTG Health Check: ' || CURRENT_TIMESTAMP || ' - Services Running'" >> /logs/health_check.log 2>&1
"""
    
    return cron_config.strip()

def create_docker_compose_configuration() -> str:
    """Generate Docker Compose configuration for scheduled jobs."""
    
    docker_compose = """
version: '3.8'

services:
  ats-intg-scheduler:
    image: dragonflyer762/ats-genai:latest
    container_name: ats-intg-scheduler
    restart: unless-stopped
    
    environment:
      - PYTHONPATH=/workspace/src
      - ATS_DATA_PATH=/data
      - ATS_BACKUP_PATH=/backup
      - ATS_LOGS_PATH=/logs
      - ENVIRONMENT=intg
      - DB_HOST=postgres-intg
      - DB_PORT=5432
      - DB_USER=postgres
      - DB_PASSWORD=intg_password
      - DB_NAME=intg_db
      
      # API Keys
      - POLYGON_API_KEY=wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD
      - FMP_API_KEY=Qf5MGG5HrOnEaWTumhVJzx3Onb3kw7Rr
      - TIINGO_API_KEY=5f40b4f36e171405746304ec0e5a6f3aa9ca77e5
      - ALPHA_VANTAGE_API_KEY=9GI0NZ3V4VNFX271
    
    volumes:
      - ./:/workspace
      - /mnt/d/ats-data/intg:/data
      - /mnt/d/ats-backup/intg:/backup
      - /mnt/d/ats-logs/intg:/logs
      - /etc/localtime:/etc/localtime:ro
    
    working_dir: /workspace
    
    # Install cron and setup scheduled jobs
    command: >
      bash -c "
        apt-get update && apt-get install -y cron &&
        echo '# ATS-INTG Daily Refresh Jobs' > /etc/cron.d/ats-intg-jobs &&
        echo '0 5 * * * root cd /workspace && python scripts/run_intg.py run --script scripts/daily_price_refresh_job.py >> /logs/daily_prices.log 2>&1' >> /etc/cron.d/ats-intg-jobs &&
        echo '30 6 * * * root cd /workspace && python scripts/run_intg.py run --script scripts/daily_fundamentals_refresh_job.py >> /logs/daily_fundamentals.log 2>&1' >> /etc/cron.d/ats-intg-jobs &&
        echo '0 8 * * * root cd /workspace && python scripts/run_intg.py run --script scripts/daily_news_refresh_job.py >> /logs/daily_news.log 2>&1' >> /etc/cron.d/ats-intg-jobs &&
        chmod 0644 /etc/cron.d/ats-intg-jobs &&
        crontab /etc/cron.d/ats-intg-jobs &&
        echo 'ATS-INTG Scheduler started. Jobs scheduled for:' &&
        echo '  Daily Prices: 05:00 UTC' &&
        echo '  Daily Fundamentals: 06:30 UTC' &&
        echo '  Daily News: 08:00 UTC' &&
        cron -f
      "
    
    depends_on:
      - postgres-intg
    
    networks:
      - ats-intg-network

  postgres-intg:
    image: timescale/timescaledb:latest-pg13
    container_name: postgres-intg
    restart: unless-stopped
    
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: intg_password
      POSTGRES_DB: intg_db
    
    ports:
      - "5433:5432"
    
    volumes:
      - postgres-intg-data:/var/lib/postgresql/data
      - /mnt/d/ats-backup/intg:/backup
    
    networks:
      - ats-intg-network

networks:
  ats-intg-network:
    driver: bridge

volumes:
  postgres-intg-data:
    driver: local
"""
    
    return docker_compose.strip()

def create_systemd_configuration() -> str:
    """Generate systemd service configuration for scheduled jobs."""
    
    systemd_service = """
[Unit]
Description=ATS-INTG Daily Refresh Jobs Scheduler
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
WorkingDirectory=/workspace
Environment=PYTHONPATH=/workspace/src
Environment=ATS_DATA_PATH=/data
Environment=ATS_BACKUP_PATH=/backup
Environment=ATS_LOGS_PATH=/logs
Environment=ENVIRONMENT=intg

# Daily Price Refresh Service
ExecStart=/usr/bin/python3 /workspace/scripts/daily_price_refresh_job.py

# Restart policy
Restart=on-failure
RestartSec=300
TimeoutStartSec=1800
TimeoutStopSec=120

# Logging
StandardOutput=append:/logs/ats-intg-daily-jobs.log
StandardError=append:/logs/ats-intg-daily-jobs-error.log

[Install]
WantedBy=multi-user.target
"""
    
    systemd_timer = """
[Unit]
Description=ATS-INTG Daily Jobs Timer
Requires=ats-intg-daily-jobs.service

[Timer]
OnCalendar=*-*-* 05:00:00
Persistent=true
AccuracySec=1min

[Install]
WantedBy=timers.target
"""
    
    return {"service": systemd_service.strip(), "timer": systemd_timer.strip()}

def run_manual_job(job_type: str) -> bool:
    """Run a specific job manually for testing."""
    
    job_scripts = {
        'prices': 'scripts/daily_price_refresh_job.py',
        'fundamentals': 'scripts/daily_fundamentals_refresh_job.py', 
        'news': 'scripts/daily_news_refresh_job.py'
    }
    
    if job_type not in job_scripts:
        log_info(f"❌ Unknown job type: {job_type}")
        log_info(f"Available jobs: {', '.join(job_scripts.keys())}")
        return False
    
    script_path = job_scripts[job_type]
    log_info(f"🚀 Running manual {job_type} refresh job...")
    
    try:
        cmd = [
            'python3', 'scripts/run_intg.py', 'run', 
            '--script', script_path,
            '--env', json.dumps({"JOB_TYPE": f"manual_{job_type}", "MANUAL_RUN": "true"})
        ]
        
        result = subprocess.run(cmd, cwd='/workspace', capture_output=False, text=True)
        
        if result.returncode == 0:
            log_info(f"✅ Manual {job_type} job completed successfully")
            return True
        else:
            log_info(f"❌ Manual {job_type} job failed with exit code: {result.returncode}")
            return False
            
    except Exception as e:
        log_info(f"❌ Error running manual {job_type} job: {e}")
        return False

def check_job_status() -> dict:
    """Check the status of recent jobs."""
    
    log_info("📊 Checking job status...")
    
    status = {
        'prices': {'last_run': None, 'status': 'unknown'},
        'fundamentals': {'last_run': None, 'status': 'unknown'},
        'news': {'last_run': None, 'status': 'unknown'}
    }
    
    try:
        # Check price refresh status
        price_query = "SELECT job_date, status, symbols_processed, records_inserted FROM intg_daily_price_checkpoint ORDER BY job_date DESC LIMIT 1"
        # Note: This would need actual database connection in real implementation
        
        # Check fundamentals status  
        fundamentals_query = "SELECT job_date, status, symbols_processed, records_inserted FROM intg_fundamentals_checkpoint ORDER BY job_date DESC LIMIT 1"
        
        # Check news status
        news_query = "SELECT job_date, status, symbols_processed, news_items_inserted FROM intg_news_checkpoint ORDER BY job_date DESC LIMIT 1"
        
        log_info("ℹ️  Job status check requires active database connection")
        log_info("ℹ️  Run individual jobs with --manual flag to see detailed status")
        
    except Exception as e:
        log_info(f"⚠️  Could not check job status: {e}")
    
    return status

def generate_monitoring_dashboard() -> str:
    """Generate a simple monitoring dashboard script."""
    
    dashboard = """#!/usr/bin/env python3
'''
ATS-INTG Jobs Monitoring Dashboard
Simple status monitor for daily refresh jobs
'''

import subprocess
import json
from datetime import datetime

def check_database_status():
    '''Check if database is accessible'''
    try:
        result = subprocess.run([
            'python3', 'scripts/run_intg.py', 'query', 
            '--query', 'SELECT CURRENT_TIMESTAMP as status'
        ], capture_output=True, text=True, cwd='/workspace')
        
        return result.returncode == 0
    except:
        return False

def get_recent_job_stats():
    '''Get statistics for recent job runs'''
    today = datetime.now().strftime('%Y-%m-%d')
    
    queries = {
        'prices': f"SELECT COUNT(*) FROM intg_daily_prices WHERE date >= '{today}' - INTERVAL '1 day'",
        'fundamentals': f"SELECT COUNT(*) FROM intg_fundamentals_comprehensive WHERE date >= '{today}' - INTERVAL '7 days'", 
        'news': f"SELECT COUNT(*) FROM intg_news WHERE DATE(published_at) >= '{today}' - INTERVAL '1 day'"
    }
    
    stats = {}
    for job_type, query in queries.items():
        try:
            result = subprocess.run([
                'python3', 'scripts/run_intg.py', 'query', '--query', query
            ], capture_output=True, text=True, cwd='/workspace')
            
            if result.returncode == 0:
                # Parse count from result
                lines = result.stdout.strip().split('\\n')
                for line in lines:
                    if line.strip().isdigit():
                        stats[job_type] = int(line.strip())
                        break
                else:
                    stats[job_type] = 0
            else:
                stats[job_type] = 'error'
        except:
            stats[job_type] = 'error'
    
    return stats

def main():
    print("=" * 60)
    print("ATS-INTG Daily Jobs Monitoring Dashboard")  
    print("=" * 60)
    print(f"Timestamp: {datetime.now()}")
    print()
    
    # Database connectivity
    db_status = "🟢 Connected" if check_database_status() else "🔴 Disconnected"
    print(f"Database Status: {db_status}")
    print()
    
    # Recent job statistics
    print("Recent Job Statistics:")
    stats = get_recent_job_stats()
    
    for job_type, count in stats.items():
        if count == 'error':
            status_icon = "🔴"
            count_text = "Error"
        elif count == 0:
            status_icon = "🟡" 
            count_text = "0 records"
        else:
            status_icon = "🟢"
            count_text = f"{count} records"
        
        print(f"  {status_icon} {job_type.capitalize()}: {count_text}")
    
    print()
    print("=" * 60)

if __name__ == "__main__":
    main()
"""
    
    return dashboard.strip()

def main():
    """Main scheduler configuration and management function."""
    
    parser = argparse.ArgumentParser(description="ATS-INTG Daily Job Scheduler")
    parser.add_argument("action", choices=[
        "schedule", "manual", "status", "config", "monitor"
    ], help="Action to perform")
    
    parser.add_argument("--job", choices=["prices", "fundamentals", "news"], 
                       help="Specific job for manual execution")
    parser.add_argument("--format", choices=["cron", "docker", "systemd"], 
                       default="cron", help="Configuration format")
    
    args = parser.parse_args()
    
    if args.action == "schedule":
        log_info("📋 ATS-INTG Daily Jobs Scheduling Recommendations:")
        print()
        print(__doc__)
        return True
        
    elif args.action == "manual":
        if not args.job:
            log_info("❌ --job required for manual execution")
            log_info("Available jobs: prices, fundamentals, news")
            return False
        return run_manual_job(args.job)
        
    elif args.action == "status":
        status = check_job_status()
        return True
        
    elif args.action == "config":
        log_info(f"📄 Generating {args.format.upper()} configuration...")
        print()
        
        if args.format == "cron":
            print(create_cron_configuration())
        elif args.format == "docker":
            print(create_docker_compose_configuration())
        elif args.format == "systemd":
            configs = create_systemd_configuration()
            print("# ats-intg-daily-jobs.service")
            print(configs["service"])
            print()
            print("# ats-intg-daily-jobs.timer")  
            print(configs["timer"])
        
        return True
        
    elif args.action == "monitor":
        log_info("📊 Generating monitoring dashboard...")
        dashboard_script = generate_monitoring_dashboard()
        
        # Write dashboard script
        with open('/workspace/scripts/monitor_daily_jobs.py', 'w') as f:
            f.write(dashboard_script)
        
        log_info("✅ Monitoring dashboard created: scripts/monitor_daily_jobs.py")
        log_info("Usage: python scripts/monitor_daily_jobs.py")
        
        return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)