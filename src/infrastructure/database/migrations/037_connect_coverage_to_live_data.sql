-- Migration 037: Connect Coverage Catalog to Live Data
-- Create triggers and functions that work with the actual data schema
-- Tables: dev_minute_prices_polygon, dev_minute_prices_tiingo, dev_daily_prices

-- =====================================================
-- Coverage Calculation Functions
-- =====================================================

-- Function to get symbol from instrument_id
CREATE OR REPLACE FUNCTION get_symbol_from_instrument_id(instrument_id INTEGER)
RETURNS TEXT AS $$
DECLARE
    symbol_name TEXT;
BEGIN
    SELECT symbol INTO symbol_name 
    FROM dev_instruments 
    WHERE id = instrument_id;
    
    RETURN COALESCE(symbol_name, 'UNKNOWN');
END;
$$ LANGUAGE plpgsql;

-- Function to calculate minute coverage for a specific day
CREATE OR REPLACE FUNCTION calculate_minute_coverage(
    p_symbol TEXT,
    p_vendor TEXT,
    p_date DATE
) RETURNS VOID AS $$
DECLARE
    start_time TIMESTAMPTZ;
    end_time TIMESTAMPTZ;
    record_count BIGINT;
    expected_count INTEGER := 390; -- 6.5 hours * 60 minutes
    completeness_ratio NUMERIC;
    avg_quality NUMERIC;
    gap_count INTEGER := 0;
    table_name TEXT;
    instrument_id_val INTEGER;
BEGIN
    -- Get instrument_id for symbol
    SELECT id INTO instrument_id_val FROM dev_instruments WHERE symbol = p_symbol;
    IF instrument_id_val IS NULL THEN
        RETURN;
    END IF;
    
    -- Set up time boundaries (market hours: 9:30 AM - 4:00 PM EST)
    start_time := p_date + INTERVAL '13:30:00'; -- 9:30 AM EST in UTC
    end_time := p_date + INTERVAL '20:00:00';   -- 4:00 PM EST in UTC
    
    -- Determine table name based on vendor
    table_name := 'dev_minute_prices_' || p_vendor;
    
    -- Calculate record count and quality for this day
    EXECUTE format('
        SELECT COUNT(*), AVG(CASE WHEN volume > 0 THEN 1.0 ELSE 0.8 END)
        FROM %I 
        WHERE instrument_id = $1 
        AND timestamp >= $2 
        AND timestamp < $3
    ', table_name) 
    INTO record_count, avg_quality
    USING instrument_id_val, start_time, end_time;
    
    -- Calculate completeness ratio
    completeness_ratio := CASE 
        WHEN expected_count > 0 THEN record_count::NUMERIC / expected_count 
        ELSE 0 
    END;
    
    -- Simple gap detection: if we have significantly fewer records than expected
    IF completeness_ratio < 0.95 THEN
        gap_count := 1;
    END IF;
    
    -- Insert or update coverage interval
    INSERT INTO coverage_intervals (
        symbol, vendor, data_type, start_time, end_time,
        record_count, expected_count, completeness_ratio,
        avg_quality_score, has_gaps, gap_count
    ) VALUES (
        p_symbol, p_vendor, 'minute', start_time, end_time,
        record_count, expected_count, completeness_ratio,
        avg_quality, gap_count > 0, gap_count
    )
    ON CONFLICT (symbol, vendor, data_type, start_time, end_time) 
    DO UPDATE SET
        record_count = EXCLUDED.record_count,
        expected_count = EXCLUDED.expected_count,
        completeness_ratio = EXCLUDED.completeness_ratio,
        avg_quality_score = EXCLUDED.avg_quality_score,
        has_gaps = EXCLUDED.has_gaps,
        gap_count = EXCLUDED.gap_count,
        created_at = NOW();
    
    -- Update coverage summary
    INSERT INTO coverage_summary (
        symbol, vendor, data_type, current_status,
        coverage_24h, quality_24h, records_24h, last_updated
    ) VALUES (
        p_symbol, p_vendor, 'minute',
        CASE WHEN completeness_ratio >= 0.95 THEN 'active' 
             WHEN completeness_ratio >= 0.80 THEN 'stale' 
             ELSE 'missing' END,
        completeness_ratio * 100,
        avg_quality,
        record_count,
        NOW()
    )
    ON CONFLICT (symbol, vendor, data_type) 
    DO UPDATE SET
        current_status = EXCLUDED.current_status,
        coverage_24h = EXCLUDED.coverage_24h,
        quality_24h = EXCLUDED.quality_24h,
        records_24h = EXCLUDED.records_24h,
        last_updated = NOW();
        
END;
$$ LANGUAGE plpgsql;

-- Function to calculate daily coverage
CREATE OR REPLACE FUNCTION calculate_daily_coverage(
    p_symbol TEXT,
    p_vendor TEXT,
    p_start_date DATE,
    p_end_date DATE
) RETURNS VOID AS $$
DECLARE
    record_count BIGINT;
    expected_count INTEGER;
    completeness_ratio NUMERIC;
    instrument_id_val INTEGER;
BEGIN
    -- Get instrument_id for symbol
    SELECT id INTO instrument_id_val FROM dev_instruments WHERE symbol = p_symbol;
    IF instrument_id_val IS NULL THEN
        RETURN;
    END IF;
    
    -- Calculate expected trading days (roughly 5 days per week)
    expected_count := (p_end_date - p_start_date) * 5 / 7;
    
    -- Count actual records for this symbol and date range
    SELECT COUNT(*) INTO record_count
    FROM dev_daily_prices
    WHERE instrument_id = instrument_id_val
    AND date >= p_start_date 
    AND date <= p_end_date
    AND primary_vendor = p_vendor;
    
    -- Calculate completeness ratio
    completeness_ratio := CASE 
        WHEN expected_count > 0 THEN record_count::NUMERIC / expected_count 
        ELSE 0 
    END;
    
    -- Insert coverage interval for daily data
    INSERT INTO coverage_intervals (
        symbol, vendor, data_type, start_time, end_time,
        record_count, expected_count, completeness_ratio,
        has_gaps, gap_count
    ) VALUES (
        p_symbol, p_vendor, 'daily', 
        p_start_date::TIMESTAMPTZ, p_end_date::TIMESTAMPTZ,
        record_count, expected_count, completeness_ratio,
        completeness_ratio < 1.0, 
        CASE WHEN completeness_ratio < 1.0 THEN 1 ELSE 0 END
    )
    ON CONFLICT (symbol, vendor, data_type, start_time, end_time) 
    DO UPDATE SET
        record_count = EXCLUDED.record_count,
        completeness_ratio = EXCLUDED.completeness_ratio,
        has_gaps = EXCLUDED.has_gaps,
        gap_count = EXCLUDED.gap_count,
        created_at = NOW();
        
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Trigger Functions for Real-time Updates
-- =====================================================

-- Trigger function for minute price updates
CREATE OR REPLACE FUNCTION update_minute_coverage_trigger()
RETURNS TRIGGER AS $$
DECLARE
    symbol_name TEXT;
    vendor_name TEXT;
    price_date DATE;
BEGIN
    -- Get symbol from instrument_id
    SELECT symbol INTO symbol_name 
    FROM dev_instruments 
    WHERE id = NEW.instrument_id;
    
    IF symbol_name IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Extract vendor from table name (TG_TABLE_NAME = 'dev_minute_prices_polygon')
    vendor_name := REPLACE(TG_TABLE_NAME, 'dev_minute_prices_', '');
    
    -- Get date from timestamp
    price_date := NEW.timestamp::DATE;
    
    -- Calculate coverage for this symbol/vendor/date
    PERFORM calculate_minute_coverage(symbol_name, vendor_name, price_date);
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger function for daily price updates
CREATE OR REPLACE FUNCTION update_daily_coverage_trigger()
RETURNS TRIGGER AS $$
DECLARE
    symbol_name TEXT;
    vendor_name TEXT;
BEGIN
    -- Get symbol from instrument_id
    SELECT symbol INTO symbol_name 
    FROM dev_instruments 
    WHERE id = NEW.instrument_id;
    
    IF symbol_name IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Use primary_vendor field
    vendor_name := NEW.primary_vendor;
    
    -- Calculate coverage for a week around this date
    PERFORM calculate_daily_coverage(
        symbol_name, 
        vendor_name, 
        NEW.date - INTERVAL '3 days',
        NEW.date + INTERVAL '3 days'
    );
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Create Triggers on Live Data Tables
-- =====================================================

-- Triggers for minute data tables
DROP TRIGGER IF EXISTS trigger_minute_coverage_polygon ON dev_minute_prices_polygon;
CREATE TRIGGER trigger_minute_coverage_polygon
    AFTER INSERT OR UPDATE ON dev_minute_prices_polygon
    FOR EACH ROW EXECUTE FUNCTION update_minute_coverage_trigger();

DROP TRIGGER IF EXISTS trigger_minute_coverage_tiingo ON dev_minute_prices_tiingo;
CREATE TRIGGER trigger_minute_coverage_tiingo
    AFTER INSERT OR UPDATE ON dev_minute_prices_tiingo
    FOR EACH ROW EXECUTE FUNCTION update_minute_coverage_trigger();

-- Trigger for daily data table
DROP TRIGGER IF EXISTS trigger_daily_coverage ON dev_daily_prices;
CREATE TRIGGER trigger_daily_coverage
    AFTER INSERT OR UPDATE ON dev_daily_prices
    FOR EACH ROW EXECUTE FUNCTION update_daily_coverage_trigger();

-- =====================================================
-- Utility Functions for Manual Coverage Updates
-- =====================================================

-- Function to refresh coverage for all symbols on a specific date
CREATE OR REPLACE FUNCTION refresh_coverage_for_date(p_date DATE)
RETURNS TEXT AS $$
DECLARE
    symbol_record RECORD;
    result_text TEXT := '';
    symbol_count INTEGER := 0;
BEGIN
    -- Process minute data for polygon
    FOR symbol_record IN 
        SELECT DISTINCT i.symbol 
        FROM dev_minute_prices_polygon mp
        JOIN dev_instruments i ON i.id = mp.instrument_id
        WHERE mp.timestamp::DATE = p_date
    LOOP
        PERFORM calculate_minute_coverage(symbol_record.symbol, 'polygon', p_date);
        symbol_count := symbol_count + 1;
    END LOOP;
    
    result_text := result_text || format('Processed %s symbols for polygon minute data on %s. ', symbol_count, p_date);
    symbol_count := 0;
    
    -- Process minute data for tiingo
    FOR symbol_record IN 
        SELECT DISTINCT i.symbol 
        FROM dev_minute_prices_tiingo mp
        JOIN dev_instruments i ON i.id = mp.instrument_id
        WHERE mp.timestamp::DATE = p_date
    LOOP
        PERFORM calculate_minute_coverage(symbol_record.symbol, 'tiingo', p_date);
        symbol_count := symbol_count + 1;
    END LOOP;
    
    result_text := result_text || format('Processed %s symbols for tiingo minute data on %s. ', symbol_count, p_date);
    
    RETURN result_text;
END;
$$ LANGUAGE plpgsql;

-- Function to get coverage overview
CREATE OR REPLACE FUNCTION get_coverage_overview()
RETURNS TABLE (
    vendor TEXT,
    data_type TEXT,
    total_symbols BIGINT,
    avg_coverage NUMERIC,
    active_symbols BIGINT,
    stale_symbols BIGINT,
    missing_symbols BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cs.vendor,
        cs.data_type,
        COUNT(*) as total_symbols,
        ROUND(AVG(cs.coverage_24h), 2) as avg_coverage,
        COUNT(*) FILTER (WHERE cs.current_status = 'active') as active_symbols,
        COUNT(*) FILTER (WHERE cs.current_status = 'stale') as stale_symbols,
        COUNT(*) FILTER (WHERE cs.current_status = 'missing') as missing_symbols
    FROM coverage_summary cs
    GROUP BY cs.vendor, cs.data_type
    ORDER BY cs.vendor, cs.data_type;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Comments and Documentation
-- =====================================================

COMMENT ON FUNCTION calculate_minute_coverage IS 'Calculates coverage metrics for minute data for a specific symbol/vendor/date';
COMMENT ON FUNCTION calculate_daily_coverage IS 'Calculates coverage metrics for daily data for a specific symbol/vendor/date range';
COMMENT ON FUNCTION refresh_coverage_for_date IS 'Manually refresh coverage calculations for all symbols on a specific date';
COMMENT ON FUNCTION get_coverage_overview IS 'Get summary statistics of coverage across all vendors and data types';