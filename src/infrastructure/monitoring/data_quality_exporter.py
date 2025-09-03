#!/usr/bin/env python3
"""
ATS Data Quality Metrics Exporter

This service exports Prometheus metrics for data quality monitoring including:
- Instrument counts by vendor
- Daily price counts by vendor  
- Data freshness metrics
- System health indicators
"""

import os
import asyncio
import logging
from prometheus_client import start_http_server, Gauge, Counter, Histogram
from shared.utils.environment import Environment, EnvironmentType
import gin

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("data_quality_exporter")

# Prometheus metrics
instrument_counts = Gauge('ats_instruments_count', 'Number of instruments by vendor', ['vendor', 'environment'])
daily_price_counts = Gauge('ats_daily_prices_count', 'Number of daily prices by vendor', ['vendor', 'environment'])
daily_price_last_update = Gauge('ats_daily_prices_last_update', 'Timestamp of last daily price update by vendor', ['vendor', 'environment'])
data_quality_score = Gauge('ats_data_quality_score', 'Data quality score by vendor (0-1)', ['vendor', 'metric', 'environment'])
scrape_duration = Histogram('ats_metrics_scrape_duration_seconds', 'Time spent scraping metrics')
scrape_errors = Counter('ats_metrics_scrape_errors_total', 'Total scrape errors', ['error_type'])

class DataQualityExporter:
    def __init__(self, environment: str):
        self.environment = environment
        self.env_type = EnvironmentType(environment)
        
        # Initialize environment configuration
        gin_config_map = {
            'dev': 'config/app_dev.gin',
            'intg': 'config/app_intg.gin',
            'prod': 'config/app_prod.gin',
            'test': 'config/app_test.gin'
        }
        
        gin_config_path = gin_config_map.get(environment)
        if not gin_config_path or not os.path.exists(gin_config_path):
            raise ValueError(f"Invalid environment or missing config: {environment}")
        
        gin.parse_config_file(gin_config_path)
        self.env = Environment(gin_config_path=gin_config_path, env_type=self.env_type)
        
        logger.info(f"Initialized Data Quality Exporter for environment: {environment}")

    async def get_database_connection(self):
        """Get database connection using centralized config"""
        from shared.utils.database import Database
        pool = await Database.create_connection_pool(env=self.env, max_retries=3, timeout=10.0)
        return pool

    async def collect_instrument_metrics(self, conn):
        """Collect instrument count metrics by vendor"""
        try:
            # Polygon instruments
            polygon_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {self.env.get_table_name('instrument_polygon')}"
            )
            instrument_counts.labels(vendor='polygon', environment=self.environment).set(polygon_count)
            
            # Tiingo instruments  
            tiingo_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {self.env.get_table_name('instrument_tiingo')}"
            )
            instrument_counts.labels(vendor='tiingo', environment=self.environment).set(tiingo_count)
            
            # EODHD instruments
            eodhd_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {self.env.get_table_name('instrument_eodhd')}"
            )
            instrument_counts.labels(vendor='eodhd', environment=self.environment).set(eodhd_count)
            
            # Total instruments
            total_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {self.env.get_table_name('instruments')}"
            )
            instrument_counts.labels(vendor='total', environment=self.environment).set(total_count)
            
            logger.debug(f"Instrument counts - Polygon: {polygon_count}, Tiingo: {tiingo_count}, EODHD: {eodhd_count}, Total: {total_count}")
            
        except Exception as e:
            logger.error(f"Error collecting instrument metrics: {e}")
            scrape_errors.labels(error_type='instrument_metrics').inc()

    async def collect_price_metrics(self, conn):
        """Collect daily price metrics by vendor"""
        try:
            # Get vendor-specific daily price counts and last update times
            vendors = ['polygon', 'tiingo', 'eodhd', 'alphavantage']
            
            for vendor in vendors:
                try:
                    # Count daily prices
                    price_count = await conn.fetchval(f"""
                        SELECT COUNT(*) FROM {self.env.get_table_name('daily_prices')} 
                        WHERE vendor = $1
                    """, vendor)
                    daily_price_counts.labels(vendor=vendor, environment=self.environment).set(price_count)
                    
                    # Get last update timestamp
                    last_update = await conn.fetchval(f"""
                        SELECT EXTRACT(EPOCH FROM MAX(updated_at)) FROM {self.env.get_table_name('daily_prices')} 
                        WHERE vendor = $1
                    """, vendor)
                    
                    if last_update:
                        daily_price_last_update.labels(vendor=vendor, environment=self.environment).set(last_update)
                    else:
                        daily_price_last_update.labels(vendor=vendor, environment=self.environment).set(0)
                        
                    logger.debug(f"Price metrics - {vendor}: count={price_count}, last_update={last_update}")
                        
                except Exception as e:
                    logger.warning(f"Error collecting price metrics for {vendor}: {e}")
                    scrape_errors.labels(error_type=f'price_metrics_{vendor}').inc()
            
        except Exception as e:
            logger.error(f"Error in collect_price_metrics: {e}")
            scrape_errors.labels(error_type='price_metrics').inc()

    async def collect_data_quality_scores(self, conn):
        """Calculate and collect data quality scores"""
        try:
            vendors = ['polygon', 'tiingo', 'eodhd']
            
            for vendor in vendors:
                try:
                    # Calculate completeness score (instruments with recent price data)
                    completeness_score = await conn.fetchval(f"""
                        WITH instrument_counts AS (
                            SELECT COUNT(*) as total_instruments 
                            FROM {self.env.get_table_name(f'instrument_{vendor}')}
                        ),
                        instruments_with_prices AS (
                            SELECT COUNT(DISTINCT symbol) as instruments_with_data
                            FROM {self.env.get_table_name('daily_prices')} 
                            WHERE vendor = $1 
                              AND date >= CURRENT_DATE - INTERVAL '7 days'
                        )
                        SELECT CASE 
                            WHEN ic.total_instruments = 0 THEN 0 
                            ELSE LEAST(1.0, iwp.instruments_with_data::float / ic.total_instruments::float)
                        END as completeness
                        FROM instrument_counts ic, instruments_with_prices iwp
                    """, vendor)
                    
                    if completeness_score is not None:
                        data_quality_score.labels(
                            vendor=vendor, metric='completeness', environment=self.environment
                        ).set(completeness_score)
                    
                    # Calculate freshness score (how recent is the latest data)
                    latest_price_age = await conn.fetchval(f"""
                        SELECT EXTRACT(EPOCH FROM (NOW() - MAX(date))) / 86400.0 as days_old
                        FROM {self.env.get_table_name('daily_prices')} 
                        WHERE vendor = $1
                    """, vendor)
                    
                    if latest_price_age is not None:
                        # Freshness score: 1.0 if < 1 day old, decreasing to 0 at 7 days old
                        freshness_score = max(0.0, 1.0 - (latest_price_age / 7.0))
                        data_quality_score.labels(
                            vendor=vendor, metric='freshness', environment=self.environment
                        ).set(freshness_score)
                    
                    logger.debug(f"Quality scores - {vendor}: completeness={completeness_score:.3f}, freshness_age={latest_price_age:.1f}d")
                    
                except Exception as e:
                    logger.warning(f"Error calculating quality scores for {vendor}: {e}")
                    scrape_errors.labels(error_type=f'quality_score_{vendor}').inc()
                    
        except Exception as e:
            logger.error(f"Error in collect_data_quality_scores: {e}")
            scrape_errors.labels(error_type='data_quality_scores').inc()

    @scrape_duration.time()
    async def collect_all_metrics(self):
        """Collect all metrics"""
        try:
            pool = await self.get_database_connection()
            
            async with pool.acquire() as conn:
                await asyncio.gather(
                    self.collect_instrument_metrics(conn),
                    self.collect_price_metrics(conn),
                    self.collect_data_quality_scores(conn),
                    return_exceptions=True
                )
            
            await pool.close()
            logger.info("Successfully collected all metrics")
            
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}")
            scrape_errors.labels(error_type='general').inc()

async def main():
    """Main exporter loop"""
    environment = os.getenv('ATS_ENVIRONMENT', 'dev')
    port = int(os.getenv('METRICS_PORT', '8080'))
    scrape_interval = int(os.getenv('SCRAPE_INTERVAL', '30'))
    
    logger.info(f"Starting ATS Data Quality Exporter on port {port} for environment {environment}")
    
    try:
        exporter = DataQualityExporter(environment)
        
        # Start Prometheus metrics server
        start_http_server(port)
        logger.info(f"Metrics server started on port {port}")
        
        # Main collection loop
        while True:
            try:
                await exporter.collect_all_metrics()
            except Exception as e:
                logger.error(f"Error in metrics collection cycle: {e}")
                scrape_errors.labels(error_type='collection_cycle').inc()
            
            await asyncio.sleep(scrape_interval)
            
    except Exception as e:
        logger.error(f"Fatal error starting exporter: {e}")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())