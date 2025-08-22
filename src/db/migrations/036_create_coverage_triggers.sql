-- Migration 036: Real-Time Coverage Triggers and Functions
-- Automatic coverage tracking for minute_bars and daily_prices updates
-- Maintains real-time coverage statistics as data arrives

-- =====================================================
-- Real-time Coverage Update Functions
-- =====================================================

-- Function to update coverage when minute_bars data changes
CREATE OR REPLACE FUNCTION update_minute_bars_coverage()
RETURNS TRIGGER AS $$
DECLARE
    interval_record RECORD;
    stats_period TIMESTAMPTZ;
    expected_records_hour INTEGER := 60;
    trading_hours_per_day NUMERIC := 6.5;
BEGIN
    -- Determine the appropriate aggregation periods
    stats_period := date_trunc('hour', NEW.timestamp);
    
    -- Update or create coverage interval for the hour
    INSERT INTO coverage_intervals (
        symbol, vendor, data_type, start_time, end_time, 
        record_count, expected_count, completeness_ratio, avg_quality_score
    )
    SELECT 
        NEW.symbol,
        NEW.vendor,
        'minute',
        stats_period,
        stats_period + INTERVAL '1 hour',
        COUNT(*),
        expected_records_hour,
        COUNT(*)::NUMERIC / expected_records_hour,
        AVG(quality_score)
    FROM minute_bars
    WHERE symbol = NEW.symbol 
        AND vendor = NEW.vendor
        AND timestamp >= stats_period 
        AND timestamp < stats_period + INTERVAL '1 hour'
    GROUP BY symbol, vendor
    ON CONFLICT (symbol, vendor, data_type, start_time) 
    DO UPDATE SET
        record_count = EXCLUDED.record_count,
        completeness_ratio = EXCLUDED.completeness_ratio,
        avg_quality_score = EXCLUDED.avg_quality_score,
        has_gaps = EXCLUDED.record_count < expected_records_hour,
        updated_at = NOW();
    
    -- Update hourly aggregated stats
    INSERT INTO coverage_stats (
        symbol, vendor, data_type, aggregation_level,
        period_start, period_end, total_expected, total_actual,
        coverage_percentage, completeness_score, avg_quality_score
    )
    SELECT 
        NEW.symbol,
        NEW.vendor,
        'minute',
        'hour',
        stats_period,
        stats_period + INTERVAL '1 hour',
        expected_records_hour,
        COUNT(*),
        (COUNT(*)::NUMERIC / expected_records_hour) * 100.0,
        COUNT(*)::NUMERIC / expected_records_hour,
        AVG(quality_score)
    FROM minute_bars
    WHERE symbol = NEW.symbol 
        AND vendor = NEW.vendor
        AND timestamp >= stats_period 
        AND timestamp < stats_period + INTERVAL '1 hour'
    GROUP BY symbol, vendor
    ON CONFLICT (symbol, vendor, data_type, aggregation_level, period_start)
    DO UPDATE SET
        total_actual = EXCLUDED.total_actual,
        coverage_percentage = EXCLUDED.coverage_percentage,
        completeness_score = EXCLUDED.completeness_score,
        avg_quality_score = EXCLUDED.avg_quality_score,
        last_computed_at = NOW();
    
    -- Update real-time summary with comprehensive metrics
    INSERT INTO coverage_summary (
        symbol, vendor, data_type, latest_data_time, current_status,
        coverage_24h, quality_24h, records_24h,
        coverage_7d, quality_7d, records_7d,
        coverage_30d, quality_30d, records_30d
    )
    SELECT 
        NEW.symbol,
        NEW.vendor,
        'minute',
        NEW.timestamp,
        CASE 
            WHEN NEW.timestamp >= NOW() - INTERVAL '5 minutes' THEN 'active'
            WHEN NEW.timestamp >= NOW() - INTERVAL '1 hour' THEN 'stale'
            ELSE 'missing'
        END,
        
        -- 24-hour metrics
        LEAST(100.0, (COUNT(*) FILTER (WHERE timestamp >= CURRENT_DATE)::NUMERIC / 
         (trading_hours_per_day * 60)) * 100.0),
        AVG(quality_score) FILTER (WHERE timestamp >= CURRENT_DATE),
        COUNT(*) FILTER (WHERE timestamp >= CURRENT_DATE),
        
        -- 7-day metrics
        LEAST(100.0, (COUNT(*) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '6 days')::NUMERIC / 
         (trading_hours_per_day * 60 * 7)) * 100.0),
        AVG(quality_score) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '6 days'),
        COUNT(*) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '6 days'),
        
        -- 30-day metrics  
        LEAST(100.0, (COUNT(*) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '29 days')::NUMERIC / 
         (trading_hours_per_day * 60 * 30)) * 100.0),
        AVG(quality_score) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '29 days'),
        COUNT(*) FILTER (WHERE timestamp >= CURRENT_DATE - INTERVAL '29 days')
        
    FROM minute_bars
    WHERE symbol = NEW.symbol 
        AND vendor = NEW.vendor
        AND timestamp >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY symbol, vendor
    ON CONFLICT (symbol, vendor, data_type)
    DO UPDATE SET
        latest_data_time = EXCLUDED.latest_data_time,
        current_status = EXCLUDED.current_status,
        coverage_24h = EXCLUDED.coverage_24h,
        quality_24h = EXCLUDED.quality_24h,
        records_24h = EXCLUDED.records_24h,
        coverage_7d = EXCLUDED.coverage_7d,
        quality_7d = EXCLUDED.quality_7d,
        records_7d = EXCLUDED.records_7d,
        coverage_30d = EXCLUDED.coverage_30d,
        quality_30d = EXCLUDED.quality_30d,
        records_30d = EXCLUDED.records_30d,
        hours_since_update = EXTRACT(EPOCH FROM (NOW() - EXCLUDED.latest_data_time)) / 3600.0,
        coverage_trend = CASE 
            WHEN EXCLUDED.coverage_24h > coverage_summary.coverage_24h + 2.0 THEN 'improving'
            WHEN EXCLUDED.coverage_24h < coverage_summary.coverage_24h - 2.0 THEN 'degrading'
            ELSE 'stable'
        END,
        quality_trend = CASE 
            WHEN EXCLUDED.quality_24h > coverage_summary.quality_24h + 0.05 THEN 'improving'
            WHEN EXCLUDED.quality_24h < coverage_summary.quality_24h - 0.05 THEN 'degrading'
            ELSE 'stable'
        END,
        last_updated = NOW();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Function to update coverage when daily_prices data changes  
CREATE OR REPLACE FUNCTION update_daily_prices_coverage()
RETURNS TRIGGER AS $$
DECLARE
    trading_days_month INTEGER := 22;
BEGIN
    -- Update daily coverage stats
    INSERT INTO coverage_stats (
        symbol, vendor, data_type, aggregation_level,
        period_start, period_end, total_expected, total_actual,
        coverage_percentage, completeness_score
    )
    SELECT 
        NEW.symbol,
        COALESCE(NEW.source, 'unknown'),
        'daily',
        'day',
        NEW.date::TIMESTAMPTZ,
        (NEW.date + INTERVAL '1 day')::TIMESTAMPTZ,
        1, 1, 100.0, 1.0
    ON CONFLICT (symbol, vendor, data_type, aggregation_level, period_start)
    DO UPDATE SET
        coverage_percentage = 100.0,
        completeness_score = 1.0,
        last_computed_at = NOW();
    
    -- Update daily prices real-time summary
    INSERT INTO coverage_summary (
        symbol, vendor, data_type, latest_data_time, current_status,
        coverage_24h, records_24h,
        coverage_7d, records_7d,
        coverage_30d, records_30d
    )
    SELECT 
        NEW.symbol,
        COALESCE(NEW.source, 'unknown'),
        'daily',
        NEW.date::TIMESTAMPTZ,
        CASE 
            WHEN NEW.date >= CURRENT_DATE - INTERVAL '1 day' THEN 'active'
            WHEN NEW.date >= CURRENT_DATE - INTERVAL '7 days' THEN 'stale'
            ELSE 'missing'
        END,
        
        -- 24-hour (really 1-day for daily data)
        CASE WHEN COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '1 day') > 0 THEN 100.0 ELSE 0.0 END,
        COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '1 day'),
        
        -- 7-day metrics
        (COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '6 days')::NUMERIC / 7.0) * 100.0,
        COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '6 days'),
        
        -- 30-day metrics
        (COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '29 days')::NUMERIC / 30.0) * 100.0,
        COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '29 days')
        
    FROM daily_prices
    WHERE symbol = NEW.symbol 
        AND source = COALESCE(NEW.source, 'unknown')
        AND date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY symbol, source
    ON CONFLICT (symbol, vendor, data_type)
    DO UPDATE SET
        latest_data_time = EXCLUDED.latest_data_time,
        current_status = EXCLUDED.current_status,
        coverage_24h = EXCLUDED.coverage_24h,
        records_24h = EXCLUDED.records_24h,
        coverage_7d = EXCLUDED.coverage_7d,
        records_7d = EXCLUDED.records_7d,
        coverage_30d = EXCLUDED.coverage_30d,
        records_30d = EXCLUDED.records_30d,
        hours_since_update = EXTRACT(EPOCH FROM (NOW() - EXCLUDED.latest_data_time)) / 3600.0,
        coverage_trend = CASE 
            WHEN EXCLUDED.coverage_7d > coverage_summary.coverage_7d + 5.0 THEN 'improving'
            WHEN EXCLUDED.coverage_7d < coverage_summary.coverage_7d - 5.0 THEN 'degrading'
            ELSE 'stable'
        END,
        last_updated = NOW();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Gap Detection Functions
-- =====================================================

-- Function to detect and insert gaps in real-time
CREATE OR REPLACE FUNCTION detect_coverage_gaps_realtime(
    p_symbol VARCHAR(10),
    p_vendor VARCHAR(50),
    p_data_type VARCHAR(20),
    p_timestamp TIMESTAMPTZ
) RETURNS INTEGER AS $$
DECLARE
    expected_interval INTERVAL;
    previous_timestamp TIMESTAMPTZ;
    gap_threshold_minutes INTEGER;
    gaps_detected INTEGER := 0;
    gap_duration_minutes INTEGER;
    gap_severity VARCHAR(10);
    gap_type VARCHAR(20);
BEGIN
    -- Set expected intervals and thresholds based on data type
    IF p_data_type = 'minute' THEN
        expected_interval := INTERVAL '1 minute';
        gap_threshold_minutes := 2;
    ELSE
        expected_interval := INTERVAL '1 day';
        gap_threshold_minutes := 24 * 60;
    END IF;
    
    -- Find the previous timestamp for this symbol/vendor
    IF p_data_type = 'minute' THEN
        SELECT timestamp INTO previous_timestamp
        FROM minute_bars
        WHERE symbol = p_symbol AND vendor = p_vendor
            AND timestamp < p_timestamp
        ORDER BY timestamp DESC
        LIMIT 1;
    ELSE
        SELECT date::TIMESTAMPTZ INTO previous_timestamp
        FROM daily_prices
        WHERE symbol = p_symbol AND source = p_vendor
            AND date < p_timestamp::DATE
        ORDER BY date DESC
        LIMIT 1;
    END IF;
    
    -- Check if gap exists
    IF previous_timestamp IS NOT NULL THEN
        gap_duration_minutes := EXTRACT(EPOCH FROM (p_timestamp - previous_timestamp)) / 60;
        
        -- Only create gap if duration exceeds threshold
        IF gap_duration_minutes > gap_threshold_minutes THEN
            
            -- Classify gap severity
            IF gap_duration_minutes <= 5 THEN
                gap_severity := 'low';
            ELSIF gap_duration_minutes <= 30 THEN
                gap_severity := 'medium';
            ELSIF gap_duration_minutes <= 120 THEN
                gap_severity := 'high';
            ELSE
                gap_severity := 'critical';
            END IF;
            
            -- Classify gap type
            IF gap_duration_minutes <= gap_threshold_minutes * 3 THEN
                gap_type := 'minor';
            ELSIF p_timestamp::TIME BETWEEN '13:30:00' AND '20:00:00' THEN
                gap_type := 'critical';  -- During market hours
            ELSE
                gap_type := 'off_hours';
            END IF;
            
            -- Insert gap record
            INSERT INTO coverage_gaps (
                symbol, vendor, data_type, gap_start, gap_end,
                gap_duration_minutes, expected_records, gap_type, gap_severity,
                trading_day, is_market_hours, detection_method, detection_confidence
            )
            VALUES (
                p_symbol, p_vendor, p_data_type,
                previous_timestamp + expected_interval, p_timestamp,
                gap_duration_minutes,
                gap_duration_minutes / CASE WHEN p_data_type = 'minute' THEN 1 ELSE 1440 END,
                gap_type, gap_severity,
                p_timestamp::DATE,
                p_timestamp::TIME BETWEEN '13:30:00' AND '20:00:00',
                'realtime_trigger',
                0.95  -- High confidence for trigger-based detection
            )
            ON CONFLICT (symbol, vendor, data_type, gap_start, gap_end) DO NOTHING;
            
            gaps_detected := 1;
        END IF;
    END IF;
    
    RETURN gaps_detected;
END;
$$ LANGUAGE plpgsql;

-- Enhanced minute bars coverage function with gap detection
CREATE OR REPLACE FUNCTION update_minute_bars_coverage_with_gaps()
RETURNS TRIGGER AS $$
DECLARE
    gaps_detected INTEGER;
BEGIN
    -- First, detect any gaps
    SELECT detect_coverage_gaps_realtime(NEW.symbol, NEW.vendor, 'minute', NEW.timestamp) 
    INTO gaps_detected;
    
    -- Then update coverage (existing function)
    PERFORM update_minute_bars_coverage();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Batch Gap Detection for Historical Data
-- =====================================================

-- Function to detect gaps in batch for a symbol/vendor/time range
CREATE OR REPLACE FUNCTION detect_coverage_gaps_batch(
    p_symbol VARCHAR(10),
    p_vendor VARCHAR(50),
    p_data_type VARCHAR(20),
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
) RETURNS INTEGER AS $$
DECLARE
    expected_interval INTERVAL;
    gap_threshold_minutes INTEGER;
    gaps_inserted INTEGER := 0;
    current_time TIMESTAMPTZ;
    last_data_time TIMESTAMPTZ;
    gap_start TIMESTAMPTZ;
    gap_end TIMESTAMPTZ;
    gap_duration_minutes INTEGER;
    data_cursor CURSOR FOR
        SELECT timestamp as data_time
        FROM minute_bars
        WHERE symbol = p_symbol AND vendor = p_vendor
            AND timestamp BETWEEN p_start_time AND p_end_time
        UNION ALL
        SELECT date::TIMESTAMPTZ as data_time
        FROM daily_prices
        WHERE symbol = p_symbol AND source = p_vendor
            AND date BETWEEN p_start_time::DATE AND p_end_time::DATE
        ORDER BY data_time;
BEGIN
    -- Set parameters based on data type
    IF p_data_type = 'minute' THEN
        expected_interval := INTERVAL '1 minute';
        gap_threshold_minutes := 2;
    ELSE
        expected_interval := INTERVAL '1 day';
        gap_threshold_minutes := 24 * 60;
    END IF;
    
    -- Initialize with start time
    last_data_time := p_start_time;
    
    -- Iterate through data points to find gaps
    FOR data_record IN data_cursor LOOP
        current_time := data_record.data_time;
        
        -- Check for gap
        gap_duration_minutes := EXTRACT(EPOCH FROM (current_time - last_data_time)) / 60;
        
        IF gap_duration_minutes > gap_threshold_minutes THEN
            gap_start := last_data_time + expected_interval;
            gap_end := current_time;
            
            -- Insert gap
            INSERT INTO coverage_gaps (
                symbol, vendor, data_type, gap_start, gap_end,
                gap_duration_minutes, expected_records,
                gap_type, gap_severity, trading_day, is_market_hours,
                detection_method, detection_confidence
            )
            VALUES (
                p_symbol, p_vendor, p_data_type, gap_start, gap_end,
                gap_duration_minutes,
                gap_duration_minutes / CASE WHEN p_data_type = 'minute' THEN 1 ELSE 1440 END,
                CASE 
                    WHEN gap_duration_minutes <= gap_threshold_minutes * 3 THEN 'minor'
                    WHEN gap_start::TIME BETWEEN '13:30:00' AND '20:00:00' THEN 'critical'
                    ELSE 'off_hours'
                END,
                CASE 
                    WHEN gap_duration_minutes <= 5 THEN 'low'
                    WHEN gap_duration_minutes <= 30 THEN 'medium'
                    WHEN gap_duration_minutes <= 120 THEN 'high'
                    ELSE 'critical'
                END,
                gap_start::DATE,
                gap_start::TIME BETWEEN '13:30:00' AND '20:00:00',
                'batch_detection',
                0.90
            )
            ON CONFLICT (symbol, vendor, data_type, gap_start, gap_end) DO NOTHING;
            
            gaps_inserted := gaps_inserted + 1;
        END IF;
        
        last_data_time := current_time;
    END LOOP;
    
    RETURN gaps_inserted;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Create Triggers for Real-Time Coverage Updates
-- =====================================================

-- Trigger for minute_bars with gap detection
CREATE TRIGGER trigger_minute_bars_coverage_update
    AFTER INSERT OR UPDATE ON minute_bars
    FOR EACH ROW
    EXECUTE FUNCTION update_minute_bars_coverage_with_gaps();

-- Trigger for daily_prices  
CREATE TRIGGER trigger_daily_prices_coverage_update
    AFTER INSERT OR UPDATE ON daily_prices
    FOR EACH ROW
    EXECUTE FUNCTION update_daily_prices_coverage();

-- =====================================================
-- Utility Functions for Coverage Analysis
-- =====================================================

-- Function to get coverage summary for a symbol/vendor
CREATE OR REPLACE FUNCTION get_coverage_summary(
    p_symbol VARCHAR(10),
    p_vendor VARCHAR(50) DEFAULT NULL,
    p_data_type VARCHAR(20) DEFAULT 'minute'
) RETURNS TABLE (
    symbol VARCHAR(10),
    vendor VARCHAR(50),
    data_type VARCHAR(20),
    current_status VARCHAR(20),
    coverage_24h NUMERIC(5,2),
    quality_24h NUMERIC(3,2),
    gaps_24h BIGINT,
    latest_data_time TIMESTAMPTZ,
    hours_since_update NUMERIC(8,2)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        cs.symbol,
        cs.vendor,
        cs.data_type,
        cs.current_status,
        cs.coverage_24h,
        cs.quality_24h,
        COUNT(g.gap_id) as gaps_24h,
        cs.latest_data_time,
        cs.hours_since_update
    FROM coverage_summary cs
    LEFT JOIN coverage_gaps g ON 
        g.symbol = cs.symbol 
        AND g.vendor = cs.vendor 
        AND g.data_type = cs.data_type
        AND g.gap_start >= NOW() - INTERVAL '24 hours'
        AND g.is_resolved = FALSE
    WHERE cs.symbol = p_symbol
        AND (p_vendor IS NULL OR cs.vendor = p_vendor)
        AND cs.data_type = p_data_type
    GROUP BY 
        cs.symbol, cs.vendor, cs.data_type, cs.current_status,
        cs.coverage_24h, cs.quality_24h, cs.latest_data_time, cs.hours_since_update;
END;
$$ LANGUAGE plpgsql;

-- Function to get top coverage issues
CREATE OR REPLACE FUNCTION get_top_coverage_issues(
    p_limit INTEGER DEFAULT 10
) RETURNS TABLE (
    symbol VARCHAR(10),
    vendor VARCHAR(50),
    data_type VARCHAR(20),
    issue_type VARCHAR(50),
    severity_score NUMERIC,
    description TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH coverage_issues AS (
        -- Low coverage issues
        SELECT 
            cs.symbol,
            cs.vendor,
            cs.data_type,
            'low_coverage' as issue_type,
            (100.0 - cs.coverage_24h) as severity_score,
            'Coverage: ' || cs.coverage_24h::TEXT || '%' as description
        FROM coverage_summary cs
        WHERE cs.coverage_24h < 90.0
        
        UNION ALL
        
        -- Quality degradation issues
        SELECT 
            cs.symbol,
            cs.vendor,
            cs.data_type,
            'quality_degradation' as issue_type,
            (1.0 - COALESCE(cs.quality_24h, 0.0)) * 100.0 as severity_score,
            'Quality: ' || COALESCE(cs.quality_24h, 0.0)::TEXT as description
        FROM coverage_summary cs
        WHERE COALESCE(cs.quality_24h, 0.0) < 0.8
        
        UNION ALL
        
        -- Gap frequency issues
        SELECT 
            g.symbol,
            g.vendor,
            g.data_type,
            'frequent_gaps' as issue_type,
            COUNT(*)::NUMERIC * 10.0 as severity_score,
            'Gaps in 24h: ' || COUNT(*)::TEXT as description
        FROM coverage_gaps g
        WHERE g.gap_start >= NOW() - INTERVAL '24 hours'
            AND g.is_resolved = FALSE
        GROUP BY g.symbol, g.vendor, g.data_type
        HAVING COUNT(*) > 5
    )
    SELECT 
        ci.symbol,
        ci.vendor,
        ci.data_type,
        ci.issue_type,
        ci.severity_score,
        ci.description
    FROM coverage_issues ci
    ORDER BY ci.severity_score DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- Comments for New Functions
-- =====================================================

COMMENT ON FUNCTION update_minute_bars_coverage() IS 'Updates coverage statistics when minute_bars data changes';
COMMENT ON FUNCTION update_daily_prices_coverage() IS 'Updates coverage statistics when daily_prices data changes';
COMMENT ON FUNCTION detect_coverage_gaps_realtime(VARCHAR, VARCHAR, VARCHAR, TIMESTAMPTZ) IS 'Detects gaps in real-time as new data arrives';
COMMENT ON FUNCTION detect_coverage_gaps_batch(VARCHAR, VARCHAR, VARCHAR, TIMESTAMPTZ, TIMESTAMPTZ) IS 'Batch gap detection for historical data analysis';
COMMENT ON FUNCTION get_coverage_summary(VARCHAR, VARCHAR, VARCHAR) IS 'Get comprehensive coverage summary for a symbol/vendor';
COMMENT ON FUNCTION get_top_coverage_issues(INTEGER) IS 'Identify the most severe coverage issues across all symbols/vendors';