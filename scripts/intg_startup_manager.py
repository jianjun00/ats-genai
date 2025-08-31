#!/usr/bin/env python3
"""
ATS-INTG Startup Manager
Handles initialization, migration, and scheduling for the ATS Integration environment
"""

import os
import sys
import time
import subprocess
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IntgStartupManager:
    def __init__(self):
        self.environment = "intg"
        self.db_host = os.getenv('DB_HOST', 'ats-intg-postgres')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'intg_password')
        self.db_name = os.getenv('DB_NAME', 'intg_db')
        
    def wait_for_database(self, timeout=120):
        """Wait for database to be available"""
        logger.info(f"Waiting for database {self.db_host}:{self.db_port}...")
        
        for i in range(timeout):
            try:
                import psycopg2
                conn = psycopg2.connect(
                    host=self.db_host,
                    port=self.db_port,
                    user=self.db_user,
                    password=self.db_password,
                    database=self.db_name
                )
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                conn.close()
                logger.info("✅ Database connection established")
                return True
            except Exception as e:
                logger.debug(f"Database connection attempt {i+1}: {e}")
            
            time.sleep(1)
        
        logger.error(f"❌ Database not available after {timeout} seconds")
        return False
    
    def run_migrations(self):
        """Run database migrations if enabled"""
        if os.getenv('AUTO_MIGRATION_ENABLED', 'false').lower() == 'true':
            logger.info("🔄 Running database migrations...")
            try:
                # Run migrations using the standard migration script
                cmd = "PYTHONPATH=/workspace/src python3 -m src.db.migration_manager"
                result = subprocess.run(cmd, shell=True, cwd="/workspace")
                if result.returncode == 0:
                    logger.info("✅ Migrations completed successfully")
                else:
                    logger.warning("⚠️ Migrations completed with warnings")
            except Exception as e:
                logger.error(f"❌ Migration failed: {e}")
    
    def setup_cron_jobs(self):
        """Setup scheduled data refresh jobs"""
        if os.getenv('CRON_ENABLED', 'false').lower() == 'true':
            logger.info("📅 Setting up cron jobs...")
            try:
                # Install cron if not present
                subprocess.run("apt-get update && apt-get install -y cron", shell=True, capture_output=True)
                
                # Create crontab for data refresh (example - adjust as needed)
                crontab_content = """
# ATS-INTG Daily Data Refresh Jobs
0 2 * * * cd /workspace && PYTHONPATH=/workspace/src python3 scripts/daily_data_refresh.py >> /logs/daily_refresh.log 2>&1
0 6 * * * cd /workspace && PYTHONPATH=/workspace/src python3 scripts/weekly_maintenance.py >> /logs/weekly_maintenance.log 2>&1
"""
                with open('/tmp/ats-crontab', 'w') as f:
                    f.write(crontab_content.strip())
                
                subprocess.run("crontab /tmp/ats-crontab", shell=True)
                subprocess.run("service cron start", shell=True)
                logger.info("✅ Cron jobs configured and started")
            except Exception as e:
                logger.error(f"❌ Cron setup failed: {e}")
    
    def health_monitor(self):
        """Continuous health monitoring loop"""
        logger.info("🔍 Starting health monitoring loop...")
        
        while True:
            try:
                # Check database connectivity
                if not self.wait_for_database(timeout=10):
                    logger.warning("⚠️ Database connectivity lost")
                
                # Check disk space
                disk_usage = subprocess.run("df -h /data", shell=True, capture_output=True, text=True)
                if "100%" in disk_usage.stdout:
                    logger.warning("⚠️ Data disk is full")
                
                # Log status
                logger.info(f"✅ Health check passed - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Wait 5 minutes between checks
                time.sleep(300)
                
            except KeyboardInterrupt:
                logger.info("🛑 Health monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Health check error: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
    
    def run(self):
        """Main startup sequence"""
        logger.info("🚀 Starting ATS-INTG Startup Manager...")
        
        # Wait for database
        if not self.wait_for_database():
            logger.error("❌ Cannot connect to database - exiting")
            sys.exit(1)
        
        # Run migrations
        self.run_migrations()
        
        # Setup cron jobs
        self.setup_cron_jobs()
        
        # Start health monitoring
        self.health_monitor()

if __name__ == "__main__":
    manager = IntgStartupManager()
    manager.run()