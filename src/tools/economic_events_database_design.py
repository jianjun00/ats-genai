"""
Economic Events Database Design

Creates separate tables for economic events data from each vendor:
1. TIINGO_ECONOMIC_EVENTS - Economic news and analysis
2. EODHD_ECONOMIC_EVENTS - Economic calendar and macro indicators  
3. POLYGON_ECONOMIC_EVENTS - Market status and technical indicators (future)

Each vendor has different data structures, so separate tables preserve data integrity
while enabling unified queries through views.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Union
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EconomicEventsDBDesigner:
    """Design and create economic events database schema"""
    
    def __init__(self, db_connection: asyncpg.Connection):
        self.conn = db_connection
        
    async def create_tiingo_economic_events_table(self):
        """Create Tiingo economic news events table"""
        logger.info("Creating Tiingo economic events table...")
        
        await self.conn.execute("""
            DROP TABLE IF EXISTS dev_tiingo_economic_events CASCADE;
            
            CREATE TABLE dev_tiingo_economic_events (
                id SERIAL PRIMARY KEY,
                article_id VARCHAR(100) UNIQUE,
                title TEXT NOT NULL,
                description TEXT,
                published_date TIMESTAMP WITH TIME ZONE,
                url TEXT,
                tags TEXT[],
                source VARCHAR(100),
                
                -- Content analysis fields
                content_preview TEXT,
                crawl_date TIMESTAMP DEFAULT now(),
                
                -- Classification fields
                event_category VARCHAR(50),
                importance_level INTEGER DEFAULT 3, -- 1=high, 2=medium, 3=low
                market_impact VARCHAR(20), -- 'positive', 'negative', 'neutral', 'unknown'
                
                -- Metadata
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now()
            );
            
            -- Indexes for efficient queries
            CREATE INDEX idx_tiingo_events_published ON dev_tiingo_economic_events(published_date DESC);
            CREATE INDEX idx_tiingo_events_category ON dev_tiingo_economic_events(event_category);
            CREATE INDEX idx_tiingo_events_importance ON dev_tiingo_economic_events(importance_level);
            CREATE INDEX idx_tiingo_events_tags ON dev_tiingo_economic_events USING GIN(tags);
            CREATE INDEX idx_tiingo_events_source ON dev_tiingo_economic_events(source);
            
            -- Trigger for updated_at
            CREATE OR REPLACE FUNCTION update_tiingo_events_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = now();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE TRIGGER tiingo_events_updated_at
                BEFORE UPDATE ON dev_tiingo_economic_events
                FOR EACH ROW EXECUTE FUNCTION update_tiingo_events_updated_at();
        """)
        
        logger.info("✅ Tiingo economic events table created")
        
    async def create_eodhd_economic_events_table(self):
        """Create EODHD economic calendar table"""
        logger.info("Creating EODHD economic events table...")
        
        await self.conn.execute("""
            DROP TABLE IF EXISTS dev_eodhd_economic_events CASCADE;
            
            CREATE TABLE dev_eodhd_economic_events (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(100) UNIQUE,
                event_name TEXT NOT NULL,
                country VARCHAR(10) DEFAULT 'US',
                
                -- Event timing
                event_date DATE NOT NULL,
                event_time TIME,
                event_datetime TIMESTAMP,
                
                -- Economic data
                importance INTEGER, -- 1=low, 2=medium, 3=high
                actual_value DECIMAL(15,4),
                forecast_value DECIMAL(15,4),
                previous_value DECIMAL(15,4),
                
                -- Additional fields
                unit VARCHAR(50), -- %, millions, billions, etc.
                frequency VARCHAR(20), -- daily, weekly, monthly, quarterly, yearly
                category VARCHAR(50), -- gdp, inflation, employment, etc.
                
                -- Market impact analysis
                market_impact VARCHAR(20), -- 'positive', 'negative', 'neutral'
                volatility_expected BOOLEAN DEFAULT FALSE,
                
                -- Metadata
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now()
            );
            
            -- Indexes for efficient queries
            CREATE INDEX idx_eodhd_events_date ON dev_eodhd_economic_events(event_date DESC);
            CREATE INDEX idx_eodhd_events_datetime ON dev_eodhd_economic_events(event_datetime DESC);
            CREATE INDEX idx_eodhd_events_importance ON dev_eodhd_economic_events(importance DESC);
            CREATE INDEX idx_eodhd_events_country ON dev_eodhd_economic_events(country);
            CREATE INDEX idx_eodhd_events_category ON dev_eodhd_economic_events(category);
            CREATE INDEX idx_eodhd_events_name ON dev_eodhd_economic_events(event_name);
            
            -- Trigger for updated_at
            CREATE OR REPLACE FUNCTION update_eodhd_events_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = now();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE TRIGGER eodhd_events_updated_at
                BEFORE UPDATE ON dev_eodhd_economic_events
                FOR EACH ROW EXECUTE FUNCTION update_eodhd_events_updated_at();
        """)
        
        logger.info("✅ EODHD economic events table created")
        
    async def create_polygon_economic_events_table(self):
        """Create Polygon market status and indicators table"""
        logger.info("Creating Polygon economic events table...")
        
        await self.conn.execute("""
            DROP TABLE IF EXISTS dev_polygon_economic_events CASCADE;
            
            CREATE TABLE dev_polygon_economic_events (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(100) UNIQUE,
                event_type VARCHAR(50) NOT NULL, -- 'market_status', 'holiday', 'indicator'
                event_name TEXT NOT NULL,
                
                -- Event timing
                event_date DATE NOT NULL,
                event_time TIME,
                event_datetime TIMESTAMP,
                
                -- Market status specific
                market_status VARCHAR(20), -- 'open', 'closed', 'early_close'
                exchange VARCHAR(20), -- 'NYSE', 'NASDAQ', etc.
                
                -- Technical indicator data (JSON)
                indicator_data JSONB,
                indicator_values DECIMAL[],
                
                -- Market impact
                affects_trading BOOLEAN DEFAULT FALSE,
                market_impact VARCHAR(20),
                
                -- Metadata
                description TEXT,
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now()
            );
            
            -- Indexes for efficient queries  
            CREATE INDEX idx_polygon_events_date ON dev_polygon_economic_events(event_date DESC);
            CREATE INDEX idx_polygon_events_type ON dev_polygon_economic_events(event_type);
            CREATE INDEX idx_polygon_events_status ON dev_polygon_economic_events(market_status);
            CREATE INDEX idx_polygon_events_exchange ON dev_polygon_economic_events(exchange);
            CREATE INDEX idx_polygon_events_trading ON dev_polygon_economic_events(affects_trading);
            CREATE INDEX idx_polygon_events_indicator_data ON dev_polygon_economic_events USING GIN(indicator_data);
            
            -- Trigger for updated_at
            CREATE OR REPLACE FUNCTION update_polygon_events_updated_at()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = now();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            
            CREATE TRIGGER polygon_events_updated_at
                BEFORE UPDATE ON dev_polygon_economic_events
                FOR EACH ROW EXECUTE FUNCTION update_polygon_events_updated_at();
        """)
        
        logger.info("✅ Polygon economic events table created")
        
    async def create_unified_economic_events_view(self):
        """Create unified view across all economic events tables"""
        logger.info("Creating unified economic events view...")
        
        await self.conn.execute("""
            DROP VIEW IF EXISTS dev_unified_economic_events CASCADE;
            
            CREATE VIEW dev_unified_economic_events AS
            
            -- Tiingo events (news/analysis)
            SELECT 
                'tiingo' as vendor,
                article_id as event_id,
                title as event_name,
                description,
                published_date::date as event_date,
                published_date as event_datetime,
                importance_level,
                event_category as category,
                market_impact,
                tags,
                url,
                content_preview as additional_data,
                created_at
            FROM dev_tiingo_economic_events
            
            UNION ALL
            
            -- EODHD events (calendar/macro)
            SELECT 
                'eodhd' as vendor,
                event_id,
                event_name,
                CASE 
                    WHEN actual_value IS NOT NULL THEN 
                        'Actual: ' || actual_value || COALESCE(' ' || unit, '')
                    ELSE NULL
                END as description,
                event_date,
                event_datetime,
                importance as importance_level,
                category,
                market_impact,
                ARRAY[country, frequency] as tags,
                NULL as url,
                JSON_BUILD_OBJECT(
                    'actual', actual_value,
                    'forecast', forecast_value, 
                    'previous', previous_value,
                    'unit', unit,
                    'frequency', frequency
                )::text as additional_data,
                created_at
            FROM dev_eodhd_economic_events
            
            UNION ALL
            
            -- Polygon events (market status/indicators)
            SELECT 
                'polygon' as vendor,
                event_id,
                event_name,
                description,
                event_date,
                event_datetime,
                CASE 
                    WHEN affects_trading THEN 1
                    WHEN market_status IS NOT NULL THEN 2
                    ELSE 3
                END as importance_level,
                event_type as category,
                market_impact,
                ARRAY[COALESCE(exchange, 'ALL'), event_type] as tags,
                NULL as url,
                indicator_data::text as additional_data,
                created_at
            FROM dev_polygon_economic_events;
            
            -- Index on the view for common queries
            CREATE INDEX idx_unified_events_vendor_date 
            ON dev_tiingo_economic_events(published_date::date DESC),
               dev_eodhd_economic_events(event_date DESC),
               dev_polygon_economic_events(event_date DESC);
        """)
        
        logger.info("✅ Unified economic events view created")
        
    async def create_sample_data(self):
        """Insert sample data for testing"""
        logger.info("Inserting sample economic events data...")
        
        # Sample Tiingo data
        await self.conn.execute("""
            INSERT INTO dev_tiingo_economic_events 
            (article_id, title, description, published_date, url, tags, source, 
             event_category, importance_level, market_impact, content_preview)
            VALUES 
            ('tiingo_001', 'Fed Chair Powell Signals Rate Cut Possibilities', 
             'Federal Reserve Chair Jerome Powell indicated potential rate cuts in upcoming meetings',
             '2025-08-24 14:30:00+00', 'https://tiingo.com/news/fed-powell-rate-cuts',
             ARRAY['fed', 'monetary_policy', 'rates'], 'Federal Reserve',
             'monetary_policy', 1, 'positive',
             'In testimony to Congress, Powell suggested the Fed may consider rate cuts if inflation continues to moderate...'),
             
            ('tiingo_002', 'Q2 GDP Growth Exceeds Expectations', 
             'US GDP grew 2.4% in Q2, beating forecasts of 2.0%',
             '2025-08-24 08:30:00+00', 'https://tiingo.com/news/gdp-q2-growth',
             ARRAY['gdp', 'growth', 'economy'], 'Bureau of Economic Analysis',
             'economic_data', 2, 'positive',
             'The Commerce Department reported stronger than expected GDP growth driven by consumer spending...');
        """)
        
        # Sample EODHD data
        await self.conn.execute("""
            INSERT INTO dev_eodhd_economic_events 
            (event_id, event_name, country, event_date, event_time, importance,
             actual_value, forecast_value, previous_value, unit, category, market_impact)
            VALUES 
            ('eodhd_001', 'Core CPI m/m', 'US', '2025-08-25', '08:30:00', 3,
             0.3, 0.2, 0.1, '%', 'inflation', 'negative'),
             
            ('eodhd_002', 'Non-Farm Payrolls', 'US', '2025-08-26', '08:30:00', 3,
             NULL, 185.0, 209.0, 'K', 'employment', 'neutral'),
             
            ('eodhd_003', 'Federal Funds Rate', 'US', '2025-08-27', '14:00:00', 3,
             NULL, 5.25, 5.50, '%', 'monetary_policy', 'neutral');
        """)
        
        # Sample Polygon data  
        await self.conn.execute("""
            INSERT INTO dev_polygon_economic_events 
            (event_id, event_type, event_name, event_date, event_time, 
             market_status, exchange, affects_trading, description, market_impact)
            VALUES 
            ('polygon_001', 'holiday', 'Labor Day', '2025-09-01', NULL,
             'closed', 'NYSE', true, 'US Labor Day - Markets Closed', 'neutral'),
             
            ('polygon_002', 'market_status', 'Early Close', '2025-11-29', '13:00:00',
             'early_close', 'NYSE', true, 'Day after Thanksgiving - Early Close', 'neutral'),
             
            ('polygon_003', 'indicator', 'VIX Spike Alert', '2025-08-24', '15:45:00',
             'open', 'CBOE', false, 'VIX exceeded 30 threshold', 'negative');
        """)
        
        logger.info("✅ Sample economic events data inserted")
        
    async def create_economic_events_functions(self):
        """Create utility functions for economic events"""
        logger.info("Creating economic events utility functions...")
        
        await self.conn.execute("""
            -- Function to get upcoming high importance events
            CREATE OR REPLACE FUNCTION get_upcoming_high_impact_events(days_ahead INTEGER DEFAULT 7)
            RETURNS TABLE(
                vendor TEXT,
                event_name TEXT,
                event_date DATE,
                importance_level INTEGER,
                market_impact TEXT,
                description TEXT
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    v.vendor,
                    v.event_name,
                    v.event_date,
                    v.importance_level,
                    v.market_impact,
                    v.description
                FROM dev_unified_economic_events v
                WHERE v.event_date BETWEEN CURRENT_DATE AND (CURRENT_DATE + days_ahead)
                AND v.importance_level <= 2  -- High and medium importance
                ORDER BY v.event_date, v.importance_level;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Function to analyze market impact by category
            CREATE OR REPLACE FUNCTION analyze_economic_impact_by_category(
                start_date DATE DEFAULT CURRENT_DATE - INTERVAL '30 days',
                end_date DATE DEFAULT CURRENT_DATE
            )
            RETURNS TABLE(
                category TEXT,
                total_events BIGINT,
                positive_impact BIGINT,
                negative_impact BIGINT,
                neutral_impact BIGINT,
                avg_importance NUMERIC
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    v.category,
                    COUNT(*) as total_events,
                    COUNT(*) FILTER (WHERE v.market_impact = 'positive') as positive_impact,
                    COUNT(*) FILTER (WHERE v.market_impact = 'negative') as negative_impact,
                    COUNT(*) FILTER (WHERE v.market_impact = 'neutral') as neutral_impact,
                    ROUND(AVG(v.importance_level), 2) as avg_importance
                FROM dev_unified_economic_events v
                WHERE v.event_date BETWEEN start_date AND end_date
                GROUP BY v.category
                ORDER BY total_events DESC;
            END;
            $$ LANGUAGE plpgsql;
            
            -- Function to get events by importance and timeframe
            CREATE OR REPLACE FUNCTION get_events_by_timeframe(
                importance_threshold INTEGER DEFAULT 2,
                days_back INTEGER DEFAULT 7,
                days_ahead INTEGER DEFAULT 7
            )
            RETURNS TABLE(
                event_date DATE,
                event_count BIGINT,
                high_impact_count BIGINT,
                categories TEXT[]
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    v.event_date,
                    COUNT(*) as event_count,
                    COUNT(*) FILTER (WHERE v.importance_level <= importance_threshold) as high_impact_count,
                    ARRAY_AGG(DISTINCT v.category) as categories
                FROM dev_unified_economic_events v
                WHERE v.event_date BETWEEN 
                    (CURRENT_DATE - days_back) AND (CURRENT_DATE + days_ahead)
                GROUP BY v.event_date
                ORDER BY v.event_date;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        logger.info("✅ Economic events utility functions created")
        
    async def setup_complete_economic_events_schema(self):
        """Setup complete economic events database schema"""
        logger.info("🚀 Setting up complete economic events database schema...")
        
        try:
            # Create individual vendor tables
            await self.create_tiingo_economic_events_table()
            await self.create_eodhd_economic_events_table()
            await self.create_polygon_economic_events_table()
            
            # Create unified view
            await self.create_unified_economic_events_view()
            
            # Create utility functions
            await self.create_economic_events_functions()
            
            # Insert sample data
            await self.create_sample_data()
            
            logger.info("✅ Complete economic events schema setup completed!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to setup economic events schema: {e}")
            return False
            
    async def validate_schema(self):
        """Validate the created schema"""
        logger.info("🔍 Validating economic events schema...")
        
        try:
            # Check tables exist
            tables = await self.conn.fetch("""
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE '%economic_events%'
                ORDER BY table_name
            """)
            
            logger.info(f"📊 Found {len(tables)} economic events tables/views:")
            for table in tables:
                logger.info(f"   • {table['table_name']} ({table['table_type']})")
            
            # Check sample data
            for vendor_table in ['tiingo', 'eodhd', 'polygon']:
                count = await self.conn.fetchval(f"""
                    SELECT COUNT(*) FROM dev_{vendor_table}_economic_events
                """)
                logger.info(f"📊 {vendor_table.upper()}: {count} sample events")
            
            # Test unified view
            unified_count = await self.conn.fetchval("""
                SELECT COUNT(*) FROM dev_unified_economic_events
            """)
            logger.info(f"📊 Unified view: {unified_count} total events")
            
            # Test utility functions
            upcoming_events = await self.conn.fetch("""
                SELECT * FROM get_upcoming_high_impact_events(30)
            """)
            logger.info(f"📊 Upcoming high-impact events: {len(upcoming_events)}")
            
            impact_analysis = await self.conn.fetch("""
                SELECT * FROM analyze_economic_impact_by_category()
            """)
            logger.info(f"📊 Impact analysis categories: {len(impact_analysis)}")
            
            logger.info("✅ Schema validation completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema validation failed: {e}")
            return False


async def main():
    """Main function to setup economic events database"""
    
    try:
        # Connect to database
        conn = await asyncpg.connect(
            host='postgres',
            port=5432,
            user='postgres', 
            password='dev_password',
            database='dev_db'
        )
        
        # Setup schema
        designer = EconomicEventsDBDesigner(conn)
        success = await designer.setup_complete_economic_events_schema()
        
        if success:
            # Validate schema
            await designer.validate_schema()
            
            logger.info("🎉 Economic events database setup completed successfully!")
        else:
            logger.error("❌ Economic events database setup failed!")
            
        await conn.close()
        return success
        
    except Exception as e:
        logger.error(f"💥 Failed to setup economic events database: {e}")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)