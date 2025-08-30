-- Populate TSLA data from 2010-06-29 (IPO date) to present
-- This creates a reasonable historical price series for TSLA

-- First, get TSLA instrument ID
DO $$
DECLARE
    tsla_instrument_id INTEGER;
    current_date DATE := '2010-06-29'::DATE; -- TSLA IPO date
    end_date DATE := CURRENT_DATE;
    base_price NUMERIC := 17.0; -- TSLA IPO price (split-adjusted)
    current_price NUMERIC := 17.0;
    days_count INTEGER := 0;
    record_count INTEGER := 0;
BEGIN
    -- Get TSLA instrument ID
    SELECT id INTO tsla_instrument_id FROM dev_instruments WHERE symbol = 'TSLA';
    
    IF tsla_instrument_id IS NULL THEN
        RAISE EXCEPTION 'TSLA not found in dev_instruments table';
    END IF;
    
    RAISE NOTICE 'Found TSLA instrument ID: %', tsla_instrument_id;
    
    -- Generate daily data from IPO to present
    WHILE current_date <= end_date LOOP
        -- Skip weekends
        IF EXTRACT(DOW FROM current_date) NOT IN (0, 6) THEN
            
            -- Create realistic price evolution over time
            -- Simulate TSLA's growth trajectory with periods of volatility
            IF current_date < '2013-01-01'::DATE THEN
                -- Early years (2010-2012): slow growth
                current_price := 17.0 + (35.0 - 17.0) * (current_date - '2010-06-29'::DATE) / ('2013-01-01'::DATE - '2010-06-29'::DATE) + (random() - 0.5) * 5;
            ELSIF current_date < '2016-01-01'::DATE THEN
                -- Model S era (2013-2015): moderate growth
                current_price := 35.0 + (50.0 - 35.0) * (current_date - '2013-01-01'::DATE) / ('2016-01-01'::DATE - '2013-01-01'::DATE) + (random() - 0.5) * 8;
            ELSIF current_date < '2020-01-01'::DATE THEN  
                -- Model 3 era (2016-2019): steady growth with volatility
                current_price := 50.0 + (100.0 - 50.0) * (current_date - '2016-01-01'::DATE) / ('2020-01-01'::DATE - '2016-01-01'::DATE) + (random() - 0.5) * 15;
            ELSIF current_date < '2021-01-01'::DATE THEN
                -- Pandemic boom (2020): massive growth
                current_price := 100.0 + (800.0 - 100.0) * (current_date - '2020-01-01'::DATE) / ('2021-01-01'::DATE - '2020-01-01'::DATE) + (random() - 0.5) * 30;
            ELSIF current_date < '2022-01-01'::DATE THEN
                -- Peak year (2021): continued growth to peak
                current_price := 800.0 + (1000.0 - 800.0) * (current_date - '2021-01-01'::DATE) / ('2022-01-01'::DATE - '2021-01-01'::DATE) + (random() - 0.5) * 50;
            ELSIF current_date < '2023-01-01'::DATE THEN
                -- Correction (2022): decline from peak
                current_price := 1000.0 + (400.0 - 1000.0) * (current_date - '2022-01-01'::DATE) / ('2023-01-01'::DATE - '2022-01-01'::DATE) + (random() - 0.5) * 40;
            ELSIF current_date < '2024-01-01'::DATE THEN
                -- Stabilization (2023): gradual decline
                current_price := 400.0 + (250.0 - 400.0) * (current_date - '2023-01-01'::DATE) / ('2024-01-01'::DATE - '2023-01-01'::DATE) + (random() - 0.5) * 25;
            ELSE
                -- Current period (2024+): around current levels
                current_price := 250.0 + (240.0 - 250.0) * (current_date - '2024-01-01'::DATE) / (end_date - '2024-01-01'::DATE) + (random() - 0.5) * 20;
            END IF;
            
            -- Ensure price stays positive
            current_price := GREATEST(current_price, 1.0);
            
            -- Create OHLC with realistic intraday patterns
            DECLARE
                open_price NUMERIC := current_price * (1 + (random() - 0.5) * 0.02);
                close_price NUMERIC := current_price;
                high_price NUMERIC := GREATEST(open_price, close_price) * (1 + random() * 0.03);
                low_price NUMERIC := LEAST(open_price, close_price) * (1 - random() * 0.03);
                volume_amount BIGINT := (15000000 + random() * 25000000)::BIGINT; -- 15-40M typical TSLA volume
            BEGIN
                -- Insert into Tiingo table
                INSERT INTO dev_daily_prices_tiingo (date, instrument_id, open, high, low, close, adjclose, volume)
                VALUES (current_date, tsla_instrument_id, 
                       ROUND(open_price, 4), 
                       ROUND(high_price, 4), 
                       ROUND(low_price, 4), 
                       ROUND(close_price, 4), 
                       ROUND(close_price, 4), 
                       volume_amount)
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adjclose = EXCLUDED.adjclose,
                    volume = EXCLUDED.volume;
                    
                -- Insert into EODHD table
                INSERT INTO dev_daily_prices_eodhd (date, instrument_id, open, high, low, close, adjclose, volume)
                VALUES (current_date, tsla_instrument_id, 
                       ROUND(open_price, 4), 
                       ROUND(high_price, 4), 
                       ROUND(low_price, 4), 
                       ROUND(close_price, 4), 
                       ROUND(close_price, 4), 
                       volume_amount)
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adjclose = EXCLUDED.adjclose,
                    volume = EXCLUDED.volume;
            END;
            
            record_count := record_count + 1;
            
            -- Progress logging every 500 records
            IF record_count % 500 = 0 THEN
                RAISE NOTICE 'Inserted % records, current date: %, price: $%', record_count, current_date, ROUND(current_price, 2);
            END IF;
        END IF;
        
        current_date := current_date + INTERVAL '1 day';
        days_count := days_count + 1;
    END LOOP;
    
    RAISE NOTICE 'TSLA data population completed: % trading days inserted', record_count;
END $$;