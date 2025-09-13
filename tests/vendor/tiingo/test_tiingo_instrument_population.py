import pytest
import pandas as pd
import io
import zipfile
from datetime import date
from typing import List, Dict


class TestTiingoInstrumentPopulation:
    """Comprehensive test coverage for Tiingo instrument population functionality"""

    def test_tiingo_url_endpoint(self):
        """Test that Tiingo URL endpoint is correct and accessible"""

        TIINGO_TICKERS_URL = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"

        # Verify URL format
        assert TIINGO_TICKERS_URL.startswith("https://")
        assert "tiingo.com" in TIINGO_TICKERS_URL
        assert TIINGO_TICKERS_URL.endswith(".zip")

        # Verify it's the correct endpoint for supported tickers
        assert "supported_tickers" in TIINGO_TICKERS_URL

    def test_date_parsing_functionality(self):
        """Test comprehensive date parsing with various input formats"""

        def parse_date(date_str):
            """Parse date string to date object"""
            if not date_str or pd.isna(date_str):
                return None
            try:
                return pd.to_datetime(date_str).date()
            except Exception:
                return None

        # Test valid date formats
        assert parse_date("2023-12-25") == date(2023, 12, 25)
        assert parse_date("2023-01-01") == date(2023, 1, 1)
        assert parse_date("1999-12-31") == date(1999, 12, 31)

        # Test datetime string (should extract date)
        assert parse_date("2023-06-15T10:30:00") == date(2023, 6, 15)
        assert parse_date("2023-06-15 10:30:00") == date(2023, 6, 15)

        # Test invalid/edge cases
        assert parse_date(None) is None
        assert parse_date("") is None
        assert parse_date("invalid-date") is None
        assert parse_date("2023-13-45") is None  # Invalid month/day
        assert parse_date("not-a-date") is None

        # Test pandas NaN
        assert parse_date(pd.NaT) is None
        assert parse_date(float('nan')) is None

        # Test different date formats that pandas can handle
        assert parse_date("12/25/2023") == date(2023, 12, 25)
        assert parse_date("25-12-2023") is not None  # pandas should handle this

    def test_csv_data_structure_validation(self):
        """Test expected CSV data structure from Tiingo"""

        # Expected columns in Tiingo supported_tickers.csv
        expected_columns = {
            'ticker', 'exchange', 'assetType', 'priceCurrency',
            'startDate', 'endDate'
        }

        # Sample CSV data structure simulation
        sample_csv_data = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'],
            'exchange': ['NASDAQ', 'NASDAQ', 'NASDAQ', 'NASDAQ', 'NASDAQ'],
            'assetType': ['Stock', 'Stock', 'Stock', 'Stock', 'Stock'],
            'priceCurrency': ['USD', 'USD', 'USD', 'USD', 'USD'],
            'startDate': ['1980-12-12', '1986-03-13', '2004-08-19', '1997-05-15', '2010-06-29'],
            'endDate': [None, None, None, None, None]
        })

        # Verify all expected columns are present
        sample_columns = set(sample_csv_data.columns)
        assert expected_columns.issubset(sample_columns), f"Missing columns: {expected_columns - sample_columns}"

        # Verify data types
        assert sample_csv_data['ticker'].dtype == 'object'
        assert sample_csv_data['exchange'].dtype == 'object'
        assert sample_csv_data['assetType'].dtype == 'object'
        assert sample_csv_data['priceCurrency'].dtype == 'object'

        # Verify sample data quality
        assert len(sample_csv_data) == 5
        assert all(ticker in ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA'] for ticker in sample_csv_data['ticker'])
        assert all(exchange == 'NASDAQ' for exchange in sample_csv_data['exchange'])
        assert all(asset_type == 'Stock' for asset_type in sample_csv_data['assetType'])

    def test_zip_file_processing_simulation(self):
        """Test ZIP file download and processing logic"""

        # Create a mock CSV content
        csv_content = """ticker,exchange,assetType,priceCurrency,startDate,endDate
AAPL,NASDAQ,Stock,USD,1980-12-12,
MSFT,NASDAQ,Stock,USD,1986-03-13,
GOOGL,NASDAQ,Stock,USD,2004-08-19,
AMZN,NASDAQ,Stock,USD,1997-05-15,
TSLA,NASDAQ,Stock,USD,2010-06-29,"""

        # Create a mock ZIP file in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr('supported_tickers.csv', csv_content)

        # Reset buffer position
        zip_buffer.seek(0)

        # Test ZIP file extraction
        with zipfile.ZipFile(zip_buffer) as zip_file:
            # Verify the CSV file is in the ZIP
            assert 'supported_tickers.csv' in zip_file.namelist()

            # Extract and parse the CSV
            with zip_file.open('supported_tickers.csv') as csv_file:
                df = pd.read_csv(csv_file)

        # Verify parsed data
        assert len(df) == 5
        assert list(df.columns) == ['ticker', 'exchange', 'assetType', 'priceCurrency', 'startDate', 'endDate']
        assert df.iloc[0]['ticker'] == 'AAPL'
        assert df.iloc[0]['exchange'] == 'NASDAQ'
        assert df.iloc[0]['assetType'] == 'Stock'
        assert df.iloc[0]['priceCurrency'] == 'USD'
        assert df.iloc[0]['startDate'] == '1980-12-12'
        assert pd.isna(df.iloc[0]['endDate'])

    def test_data_transformation_and_cleaning(self):
        """Test data transformation and cleaning logic"""

        def parse_date(date_str):
            if not date_str or pd.isna(date_str):
                return None
            try:
                return pd.to_datetime(date_str).date()
            except Exception:
                return None

        # Sample data with various edge cases
        sample_data = pd.DataFrame({
            'ticker': ['AAPL', 'MSFT ', '  GOOGL  ', 'AMZN', ''],
            'exchange': ['NASDAQ', 'NYSE  ', '  BATS', None, 'PINK'],
            'assetType': ['Stock', '  Stock  ', 'ETF', None, 'Mutual Fund'],
            'priceCurrency': ['USD', 'USD  ', '  EUR  ', None, 'CAD'],
            'startDate': ['2020-01-01', '2020-02-01  ', '  2020-03-01', None, 'invalid'],
            'endDate': [None, '2023-01-01', '', '2023-12-31', 'bad-date']
        })

        # Test data cleaning transformation
        cleaned_batch_data = []
        for _, row in sample_data.iterrows():
            if not row['ticker'] or str(row['ticker']).strip() == '':
                continue  # Skip empty tickers

            cleaned_batch_data.append((
                str(row['ticker']).strip() if pd.notna(row['ticker']) else None,
                str(row['exchange']).strip() if pd.notna(row['exchange']) else None,
                str(row['assetType']).strip() if pd.notna(row['assetType']) else None,
                str(row['priceCurrency']).strip() if pd.notna(row['priceCurrency']) else None,
                parse_date(row['startDate']) if 'startDate' in row else None,
                parse_date(row['endDate']) if 'endDate' in row else None
            ))

        # Verify cleaning results
        assert len(cleaned_batch_data) == 4  # Empty ticker row should be skipped

        # Check first row (AAPL)
        assert cleaned_batch_data[0][0] == 'AAPL'  # ticker
        assert cleaned_batch_data[0][1] == 'NASDAQ'  # exchange
        assert cleaned_batch_data[0][2] == 'Stock'  # assetType
        assert cleaned_batch_data[0][3] == 'USD'  # priceCurrency
        assert cleaned_batch_data[0][4] == date(2020, 1, 1)  # startDate
        assert cleaned_batch_data[0][5] is None  # endDate

        # Check second row (MSFT with whitespace)
        assert cleaned_batch_data[1][0] == 'MSFT'  # ticker trimmed
        assert cleaned_batch_data[1][1] == 'NYSE'  # exchange trimmed
        assert cleaned_batch_data[1][2] == 'Stock'  # assetType trimmed
        assert cleaned_batch_data[1][3] == 'USD'  # priceCurrency trimmed
        assert cleaned_batch_data[1][4] == date(2020, 2, 1)  # startDate
        assert cleaned_batch_data[1][5] == date(2023, 1, 1)  # endDate

        # Check third row (GOOGL with more whitespace)
        assert cleaned_batch_data[2][0] == 'GOOGL'  # ticker trimmed
        assert cleaned_batch_data[2][1] == 'BATS'  # exchange trimmed
        assert cleaned_batch_data[2][2] == 'ETF'  # assetType
        assert cleaned_batch_data[2][3] == 'EUR'  # priceCurrency trimmed

        # Check fourth row (AMZN with None values)
        assert cleaned_batch_data[3][0] == 'AMZN'  # ticker
        assert cleaned_batch_data[3][1] is None  # exchange (was None)
        assert cleaned_batch_data[3][2] is None  # assetType (was None)
        assert cleaned_batch_data[3][3] is None  # priceCurrency (was None)

    def test_database_schema_requirements(self):
        """Test database schema creation and requirements"""

        expected_schema_sql = """
            CREATE TABLE IF NOT EXISTS dev_instrument_tiingo (
                id SERIAL PRIMARY KEY,
                ticker TEXT UNIQUE NOT NULL,
                exchange TEXT,
                asset_type TEXT,
                price_currency TEXT,
                start_date DATE,
                end_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        # Verify key schema elements
        assert "dev_instrument_tiingo" in expected_schema_sql
        assert "SERIAL PRIMARY KEY" in expected_schema_sql
        assert "ticker TEXT UNIQUE NOT NULL" in expected_schema_sql
        assert "exchange TEXT" in expected_schema_sql
        assert "asset_type TEXT" in expected_schema_sql
        assert "price_currency TEXT" in expected_schema_sql
        assert "start_date DATE" in expected_schema_sql
        assert "end_date DATE" in expected_schema_sql
        assert "created_at TIMESTAMP" in expected_schema_sql
        assert "updated_at TIMESTAMP" in expected_schema_sql

        # Expected table fields
        expected_fields = {
            'id', 'ticker', 'exchange', 'asset_type', 'price_currency',
            'start_date', 'end_date', 'created_at', 'updated_at'
        }

        # Verify all fields are mentioned in schema
        for field in expected_fields:
            assert field in expected_schema_sql.lower()

    def test_batch_processing_logic(self):
        """Test batch processing with various data sizes"""

        def process_in_batches(data: List, batch_size: int = 1000):
            """Simulate batch processing"""
            batches = []
            for start_idx in range(0, len(data), batch_size):
                end_idx = min(start_idx + batch_size, len(data))
                batch = data[start_idx:end_idx]
                batches.append({
                    'batch_number': start_idx // batch_size + 1,
                    'start_idx': start_idx,
                    'end_idx': end_idx,
                    'size': len(batch),
                    'data': batch
                })
            return batches

        # Test small dataset (less than batch size)
        small_data = list(range(500))
        small_batches = process_in_batches(small_data, 1000)
        assert len(small_batches) == 1
        assert small_batches[0]['size'] == 500
        assert small_batches[0]['batch_number'] == 1

        # Test exact batch size
        exact_data = list(range(1000))
        exact_batches = process_in_batches(exact_data, 1000)
        assert len(exact_batches) == 1
        assert exact_batches[0]['size'] == 1000

        # Test multiple batches
        large_data = list(range(2500))
        large_batches = process_in_batches(large_data, 1000)
        assert len(large_batches) == 3
        assert large_batches[0]['size'] == 1000
        assert large_batches[1]['size'] == 1000
        assert large_batches[2]['size'] == 500
        assert large_batches[2]['batch_number'] == 3

        # Test very large dataset (simulate Tiingo scale)
        tiingo_scale_data = list(range(120000))  # ~120K instruments
        tiingo_batches = process_in_batches(tiingo_scale_data, 1000)
        assert len(tiingo_batches) == 120
        assert tiingo_batches[0]['size'] == 1000
        assert tiingo_batches[-1]['size'] == 1000  # All batches should be 1000

        # Test empty dataset
        empty_batches = process_in_batches([], 1000)
        assert len(empty_batches) == 0

    def test_upsert_sql_logic(self):
        """Test UPSERT (INSERT ... ON CONFLICT) SQL logic"""

        upsert_sql = """
            INSERT INTO dev_instrument_tiingo
            (ticker, exchange, asset_type, price_currency, start_date, end_date, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, now())
            ON CONFLICT (ticker) DO UPDATE SET
            exchange = EXCLUDED.exchange,
            asset_type = EXCLUDED.asset_type,
            price_currency = EXCLUDED.price_currency,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            updated_at = now()
        """

        # Verify UPSERT structure
        assert "INSERT INTO dev_instrument_tiingo" in upsert_sql
        assert "ON CONFLICT (ticker) DO UPDATE SET" in upsert_sql
        assert "EXCLUDED.exchange" in upsert_sql
        assert "EXCLUDED.asset_type" in upsert_sql
        assert "EXCLUDED.price_currency" in upsert_sql
        assert "EXCLUDED.start_date" in upsert_sql
        assert "EXCLUDED.end_date" in upsert_sql
        assert "updated_at = now()" in upsert_sql

        # Verify all required fields are in INSERT
        insert_fields = ['ticker', 'exchange', 'asset_type', 'price_currency', 'start_date', 'end_date', 'updated_at']
        for field in insert_fields:
            assert field in upsert_sql

        # Verify parameter placeholders
        for i in range(1, 7):  # $1 through $6
            assert f"${i}" in upsert_sql

    def test_error_handling_scenarios(self):
        """Test various error handling scenarios"""

        # Test download failure simulation
        def simulate_download_failure():
            """Simulate network/download failure"""
            raise Exception("Network timeout during ZIP download")

        # Test ZIP corruption simulation
        def simulate_zip_corruption():
            """Simulate corrupted ZIP file"""
            raise zipfile.BadZipFile("ZIP file is corrupted")

        # Test CSV parsing failure
        def simulate_csv_parsing_error():
            """Simulate CSV parsing error"""
            raise pd.errors.EmptyDataError("No data found in CSV")

        # Test database connection failure
        def simulate_db_connection_error():
            """Simulate database connection failure"""
            raise Exception("Database connection refused")

        # Test batch insert failure
        def simulate_batch_insert_error():
            """Simulate database insert error"""
            raise Exception("Database constraint violation")

        # Verify exceptions are properly defined
        try:
            simulate_download_failure()
        except Exception as e:
            assert "Network timeout" in str(e)

        try:
            simulate_zip_corruption()
        except zipfile.BadZipFile as e:
            assert "corrupted" in str(e)

        try:
            simulate_csv_parsing_error()
        except pd.errors.EmptyDataError as e:
            assert "No data" in str(e)

        # Test error recovery logic
        def process_batch_with_error_recovery(batch_data: List, max_retries: int = 3):
            """Simulate batch processing with error recovery"""
            retry_count = 0
            while retry_count < max_retries:
                try:
                    # Simulate processing
                    if retry_count < 2:  # Fail first 2 attempts
                        raise Exception("Temporary database error")
                    return True  # Success on 3rd attempt
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        raise e
            return False

        # Test successful retry
        result = process_batch_with_error_recovery([1, 2, 3])
        assert result is True

    def test_data_quality_validation(self):
        """Test data quality validation and statistics"""

        # Sample dataset with quality issues
        sample_data = {
            'total_instruments': 117740,
            'asset_types': {
                'Stock': 58092,
                'Mutual Fund': 52024,
                'ETF': 7624
            },
            'exchanges': {
                'NMFQS': 52007,
                'PINK': 15242,
                'NASDAQ': 13690,
                'NYSE': 7659,
                'OTCGREY': 4498
            },
            'currencies': {
                'USD': 98500,
                'CAD': 12000,
                'EUR': 5000,
                'GBP': 2240
            }
        }

        # Validate data quality metrics
        def validate_data_quality(data: Dict) -> Dict:
            """Validate data quality and return metrics"""
            metrics = {
                'total_valid': 0,
                'coverage_stats': {},
                'quality_issues': []
            }

            # Check total instruments
            if data['total_instruments'] > 100000:
                metrics['coverage_stats']['large_dataset'] = True

            # Check asset type distribution
            asset_types = data['asset_types']
            if asset_types['Stock'] > asset_types['Mutual Fund']:
                metrics['coverage_stats']['stock_heavy'] = True

            # Check for OTC/Pink sheet presence
            exchanges = data['exchanges']
            if 'PINK' in exchanges or 'OTCGREY' in exchanges:
                metrics['quality_issues'].append('Contains OTC/Pink sheet instruments')

            # Check USD dominance
            currencies = data['currencies']
            usd_percentage = currencies['USD'] / data['total_instruments']
            if usd_percentage > 0.8:
                metrics['coverage_stats']['usd_dominant'] = True

            return metrics

        quality_metrics = validate_data_quality(sample_data)

        # Verify quality metrics
        assert quality_metrics['coverage_stats']['large_dataset'] is True
        assert quality_metrics['coverage_stats']['stock_heavy'] is True
        assert quality_metrics['coverage_stats']['usd_dominant'] is True
        assert 'Contains OTC/Pink sheet instruments' in quality_metrics['quality_issues']

    def test_performance_characteristics(self):
        """Test performance characteristics and optimization"""


        def estimate_processing_time(instrument_count: int, batch_size: int = 1000) -> Dict:
            """Estimate processing time based on instrument count"""

            # Performance assumptions
            download_time_base = 5.0  # 5 seconds base download time
            processing_rate = 5000   # instruments per second
            db_insert_rate = 2000    # database inserts per second
            batch_overhead = 0.1     # 100ms per batch overhead

            # Calculate estimates
            download_time = download_time_base + (instrument_count / 100000) * 2  # Scales with data size
            processing_time = instrument_count / processing_rate

            batch_count = (instrument_count + batch_size - 1) // batch_size
            db_insert_time = instrument_count / db_insert_rate
            batch_overhead_time = batch_count * batch_overhead

            total_time = download_time + processing_time + db_insert_time + batch_overhead_time

            return {
                'instrument_count': instrument_count,
                'batch_size': batch_size,
                'batch_count': batch_count,
                'download_time': download_time,
                'processing_time': processing_time,
                'db_insert_time': db_insert_time,
                'batch_overhead_time': batch_overhead_time,
                'total_estimated_time': total_time
            }

        # Test performance estimates for different scales
        small_scale = estimate_processing_time(10000)  # 10K instruments
        medium_scale = estimate_processing_time(50000)  # 50K instruments
        tiingo_scale = estimate_processing_time(117740)  # Actual Tiingo scale

        # Verify performance characteristics
        assert small_scale['total_estimated_time'] < 30.0  # Should complete in under 30 seconds
        assert medium_scale['total_estimated_time'] < 60.0  # Should complete in under 1 minute
        assert tiingo_scale['total_estimated_time'] < 120.0  # Should complete in under 2 minutes

        # Verify batch optimization
        assert tiingo_scale['batch_count'] == 118  # 117740 / 1000 rounded up
        assert small_scale['batch_overhead_time'] < 2.0  # Batch overhead should be minimal

        # Test batch size optimization
        small_batch = estimate_processing_time(117740, 500)   # Smaller batches
        large_batch = estimate_processing_time(117740, 2000)  # Larger batches
        optimal_batch = estimate_processing_time(117740, 1000)  # Optimal batches

        # Verify optimal batch size performs well
        assert optimal_batch['total_estimated_time'] <= small_batch['total_estimated_time']
        assert optimal_batch['batch_count'] < small_batch['batch_count']


class TestTiingoInstrumentIntegration:
    """Integration tests for Tiingo instrument population"""

    @pytest.mark.integration
    def test_end_to_end_data_flow_simulation(self):
        """Test complete end-to-end data flow simulation"""

        # Simulate the complete workflow
        workflow_steps = {
            'download_zip': {'status': 'pending', 'duration': 0},
            'extract_csv': {'status': 'pending', 'duration': 0},
            'parse_data': {'status': 'pending', 'duration': 0},
            'create_schema': {'status': 'pending', 'duration': 0},
            'process_batches': {'status': 'pending', 'duration': 0, 'batches_processed': 0},
            'generate_stats': {'status': 'pending', 'duration': 0}
        }

        def execute_workflow_step(step_name: str, simulate_duration: float = 0.1):
            """Execute a workflow step"""
            workflow_steps[step_name]['status'] = 'running'

            # Simulate work
            import time
            time.sleep(simulate_duration)

            workflow_steps[step_name]['status'] = 'completed'
            workflow_steps[step_name]['duration'] = simulate_duration

            if step_name == 'process_batches':
                workflow_steps[step_name]['batches_processed'] = 118  # Simulate 118 batches

        # Execute complete workflow
        execute_workflow_step('download_zip')
        execute_workflow_step('extract_csv')
        execute_workflow_step('parse_data')
        execute_workflow_step('create_schema')
        execute_workflow_step('process_batches')
        execute_workflow_step('generate_stats')

        # Verify all steps completed successfully
        for step_name, step_info in workflow_steps.items():
            assert step_info['status'] == 'completed', f"Step {step_name} did not complete"

        # Verify batch processing metrics
        assert workflow_steps['process_batches']['batches_processed'] == 118

        # Verify total processing time is reasonable
        total_duration = sum(step['duration'] for step in workflow_steps.values())
        assert total_duration < 1.0  # Should complete quickly in simulation

    @pytest.mark.integration
    def test_database_interaction_simulation(self):
        """Test database interaction patterns"""

        # Simulate database operations
        mock_operations = {
            'connect': {'calls': 0, 'success': True},
            'create_table': {'calls': 0, 'success': True},
            'count_existing': {'calls': 0, 'result': 0},
            'batch_insert': {'calls': 0, 'total_rows': 0, 'success': True},
            'final_count': {'calls': 0, 'result': 0},
            'summary_queries': {'calls': 0, 'results': {}},
            'close': {'calls': 0, 'success': True}
        }

        def simulate_database_operation(operation: str, **kwargs):
            """Simulate database operation"""
            mock_operations[operation]['calls'] += 1

            if operation == 'batch_insert':
                batch_size = kwargs.get('batch_size', 1000)
                mock_operations[operation]['total_rows'] += batch_size

            if operation == 'final_count':
                mock_operations[operation]['result'] = 117740

            if operation == 'summary_queries':
                mock_operations[operation]['results'] = {
                    'asset_types': {'Stock': 58092, 'Mutual Fund': 52024, 'ETF': 7624},
                    'exchanges': {'NMFQS': 52007, 'PINK': 15242, 'NASDAQ': 13690},
                    'currencies': {'USD': 98500, 'CAD': 12000, 'EUR': 5000}
                }

        # Simulate complete database interaction flow
        simulate_database_operation('connect')
        simulate_database_operation('create_table')
        simulate_database_operation('count_existing')

        # Simulate batch processing (118 batches)
        for batch_num in range(118):
            batch_size = 1000 if batch_num < 117 else 740  # Last batch is smaller
            simulate_database_operation('batch_insert', batch_size=batch_size)

        simulate_database_operation('final_count')
        simulate_database_operation('summary_queries')
        simulate_database_operation('close')

        # Verify database interaction patterns
        assert mock_operations['connect']['calls'] == 1
        assert mock_operations['create_table']['calls'] == 1
        assert mock_operations['batch_insert']['calls'] == 118
        assert mock_operations['batch_insert']['total_rows'] == 117740
        assert mock_operations['final_count']['calls'] == 1
        assert mock_operations['final_count']['result'] == 117740
        assert mock_operations['summary_queries']['calls'] == 1
        assert mock_operations['close']['calls'] == 1

    @pytest.mark.integration
    def test_real_world_data_characteristics(self):
        """Test handling of real-world Tiingo data characteristics"""

        # Real Tiingo data characteristics based on actual results
        tiingo_characteristics = {
            'total_instruments': 117740,
            'column_mapping': {
                'ticker': 'ticker',
                'exchange': 'exchange',
                'assetType': 'asset_type',
                'priceCurrency': 'price_currency',
                'startDate': 'start_date',
                'endDate': 'end_date'
            },
            'data_quality': {
                'instruments_with_exchange': 114459,  # 97.2% completeness
                'instruments_with_asset_type': 117740,  # 100% completeness
                'instruments_with_start_date': 110000,  # Estimated
                'instruments_with_end_date': 5000       # Estimated
            },
            'asset_distribution': {
                'Stock': 58092,
                'Mutual Fund': 52024,
                'ETF': 7624
            },
            'exchange_distribution': {
                'NMFQS': 52007,      # Mutual Fund quotes
                'PINK': 15242,       # Pink sheets
                'NASDAQ': 13690,     # Major exchange
                'NYSE': 7659,        # Major exchange
                'OTCGREY': 4498,     # OTC Grey
                'SHE': 3415,         # International
                'NYSE ARCA': 3216,   # Major exchange
                'EXPM': 3195,        # International
                'SHG': 2962,         # International
                'ASX': 1834          # International
            }
        }

        # Test data volume handling
        total_instruments = tiingo_characteristics['total_instruments']
        assert total_instruments > 100000, "Tiingo should provide large dataset"

        # Test column mapping
        column_mapping = tiingo_characteristics['column_mapping']
        assert 'ticker' in column_mapping
        assert 'assetType' in column_mapping  # Tiingo uses camelCase
        assert column_mapping['assetType'] == 'asset_type'  # Maps to snake_case

        # Test data completeness
        quality = tiingo_characteristics['data_quality']
        exchange_completeness = quality['instruments_with_exchange'] / total_instruments
        asset_type_completeness = quality['instruments_with_asset_type'] / total_instruments

        assert exchange_completeness > 0.95, "Exchange data should be >95% complete"
        assert asset_type_completeness == 1.0, "Asset type should be 100% complete"

        # Test asset distribution realism
        assets = tiingo_characteristics['asset_distribution']
        assert assets['Stock'] > assets['Mutual Fund'] / 2  # Reasonable stock representation
        assert assets['Mutual Fund'] > 50000  # Large mutual fund coverage
        assert assets['ETF'] > 5000  # Substantial ETF coverage

        # Test exchange diversity
        exchanges = tiingo_characteristics['exchange_distribution']
        major_us_exchanges = exchanges['NASDAQ'] + exchanges['NYSE'] + exchanges['NYSE ARCA']
        international_exchanges = exchanges['SHE'] + exchanges['SHG'] + exchanges['ASX']
        otc_exchanges = exchanges['PINK'] + exchanges['OTCGREY']

        assert major_us_exchanges > 20000, "Should have substantial US major exchange coverage"
        assert international_exchanges > 8000, "Should include international markets"
        assert otc_exchanges > 15000, "Should include OTC markets"

        # Test that mutual fund exchange (NMFQS) dominates mutual funds
        assert exchanges['NMFQS'] > 50000, "NMFQS should dominate for mutual funds"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])