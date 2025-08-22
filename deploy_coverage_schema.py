#!/usr/bin/env python3
"""
Deploy Coverage Catalog Schema to Kubernetes Development Environment

This script runs the coverage catalog database migrations in the K8s environment.
"""

import asyncio
import asyncpg
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

async def deploy_coverage_schema():
    """Deploy the coverage catalog schema"""
    
    # Database connection from K8s environment
    db_url = "postgresql://postgres:dev_password@postgres-simple:5432/dev_db"
    
    try:
        # Connect to database
        conn = await asyncpg.connect(db_url)
        logger.info("✅ Connected to database")
        
        # Enable TimescaleDB if not already enabled
        await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
        logger.info("✅ TimescaleDB extension enabled")
        
        # Create coverage_intervals table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS coverage_intervals (
                interval_id BIGSERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                vendor VARCHAR(50) NOT NULL,
                data_type VARCHAR(20) NOT NULL,
                start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ NOT NULL,
                record_count BIGINT NOT NULL,
                expected_count BIGINT NOT NULL,
                completeness_ratio NUMERIC(5,4) NOT NULL,
                avg_quality_score NUMERIC(3,2),
                has_gaps BOOLEAN DEFAULT FALSE,
                gap_count INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("✅ Created coverage_intervals table")
        
        # Create TimescaleDB hypertable
        try:
            await conn.execute("SELECT create_hypertable('coverage_intervals', 'start_time', if_not_exists => TRUE)")
            logger.info("✅ Created TimescaleDB hypertable for coverage_intervals")
        except Exception as e:
            logger.warning(f"Hypertable may already exist: {e}")
        
        # Create coverage_gaps table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS coverage_gaps (
                gap_id BIGSERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                vendor VARCHAR(50) NOT NULL,
                data_type VARCHAR(20) NOT NULL,
                gap_start TIMESTAMPTZ NOT NULL,
                gap_end TIMESTAMPTZ NOT NULL,
                gap_duration_minutes INTEGER NOT NULL,
                expected_records INTEGER NOT NULL,
                gap_type VARCHAR(20) NOT NULL,
                gap_severity VARCHAR(20) NOT NULL,
                trading_day DATE NOT NULL,
                is_market_hours BOOLEAN DEFAULT TRUE,
                detection_method VARCHAR(20) NOT NULL,
                detection_confidence NUMERIC(3,2) DEFAULT 0.95,
                is_resolved BOOLEAN DEFAULT FALSE,
                detected_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        logger.info("✅ Created coverage_gaps table")
        
        # Create coverage_summary view
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS coverage_summary (
                symbol VARCHAR(10) NOT NULL,
                vendor VARCHAR(50) NOT NULL,
                data_type VARCHAR(20) NOT NULL,
                current_status VARCHAR(20) NOT NULL,
                coverage_24h NUMERIC(5,2) NOT NULL,
                quality_24h NUMERIC(3,2),
                gaps_24h INTEGER DEFAULT 0,
                records_24h INTEGER DEFAULT 0,
                coverage_7d NUMERIC(5,2),
                coverage_30d NUMERIC(5,2),
                latest_data_time TIMESTAMPTZ,
                hours_since_update NUMERIC(8,2),
                coverage_trend VARCHAR(20),
                quality_trend VARCHAR(20),
                last_updated TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (symbol, vendor, data_type)
            )
        """)
        logger.info("✅ Created coverage_summary table")
        
        # Create basic indexes
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_coverage_intervals_symbol_vendor ON coverage_intervals(symbol, vendor, data_type)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_coverage_gaps_symbol_vendor ON coverage_gaps(symbol, vendor, data_type)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_coverage_gaps_detected_at ON coverage_gaps(detected_at)")
        logger.info("✅ Created indexes")
        
        # Insert some sample SLA configurations
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS coverage_sla_config (
                symbol VARCHAR(10),
                vendor VARCHAR(50) NOT NULL,
                data_type VARCHAR(20) NOT NULL,
                min_coverage_percentage NUMERIC(5,2) NOT NULL DEFAULT 95.0,
                warning_threshold NUMERIC(5,2) NOT NULL DEFAULT 90.0,
                critical_threshold NUMERIC(5,2) NOT NULL DEFAULT 80.0,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (COALESCE(symbol, ''), vendor, data_type)
            )
        """)
        logger.info("✅ Created coverage_sla_config table")
        
        # Insert default SLA configuration
        await conn.execute("""
            INSERT INTO coverage_sla_config (symbol, vendor, data_type, min_coverage_percentage, warning_threshold, critical_threshold)
            VALUES (NULL, 'polygon', 'minute', 95.0, 90.0, 80.0)
            ON CONFLICT DO NOTHING
        """)
        logger.info("✅ Inserted default SLA configuration")
        
        await conn.close()
        logger.info("🎉 Coverage catalog schema deployed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Failed to deploy schema: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(deploy_coverage_schema())