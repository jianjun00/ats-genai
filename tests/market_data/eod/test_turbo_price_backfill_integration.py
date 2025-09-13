import pytest
import asyncio
from datetime import date
from shared.utils.environment import Environment, EnvironmentType

from domains.market_data.services.eod.turbo_price_backfill import (
    TurboDatabaseInserter
)


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_end_to_end_polygon_backfill(unit_test_db):
    """Test end-to-end Polygon price backfill with real database."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Setup test data: create test instrument
    from domains.instruments.repositories.instruments_dao import InstrumentsDAO
    from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
    from dao.vendors_dao import VendorsDAO

    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    vendors_dao = VendorsDAO(env)

    # Create Polygon vendor if it doesn't exist
    polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    if not polygon_vendor:
        polygon_vendor_id = await vendors_core.dao.create_vendor(
            "polygon",
            description="Polygon.io",
            api_key_env_var="POLYGON_API_KEY"
        )
    else:
        polygon_vendor_id = polygon_vendor["id"]

    # Create test instrument
    test_instrument_id = await instruments_core.dao.create_instrument(
        symbol="TESTPOLY",
        name="Test Polygon Corp",
        exchange="NASDAQ",
        type_="CS",
        currency="USD",
        list_date=date(2020, 1, 1)
    )

    # Create instrument cross-reference
    await xrefs_core.dao.create_xref(
        instrument_id=test_instrument_id,
        vendor_id=polygon_vendor_id,
        symbol="TESTPOLY",
        start_at=date(2020, 1, 1)
    )

    # Mock API response data
    sample_data = [
        {
            'date': date(2024, 8, 1),
            'instrument_id': test_instrument_id,
            'open': 150.0,
            'high': 155.0,
            'low': 149.0,
            'close': 153.0,
            'volume': 1000000
        },
        {
            'date': date(2024, 8, 2),
            'instrument_id': test_instrument_id,
            'open': 153.0,
            'high': 158.0,
            'low': 152.0,
            'close': 157.0,
            'volume': 1200000
        }
    ]

    # Test database insertion
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert sample data
        inserted_count = await db_inserter.bulk_insert_polygon(sample_data)
        assert inserted_count == 2

        # Verify data was inserted correctly
        from vendor.polygon.core.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
        prices_dao = DailyPricesPolygonDAO(env)

        # Get inserted prices
        prices = await prices_core.dao.list_prices(test_instrument_id)
        assert len(prices) == 2

        # Verify price data
        price_by_date = {p['date']: p for p in prices}

        assert price_by_date[date(2024, 8, 1)]['open'] == 150.0
        assert price_by_date[date(2024, 8, 1)]['close'] == 153.0
        assert price_by_date[date(2024, 8, 1)]['volume'] == 1000000

        assert price_by_date[date(2024, 8, 2)]['open'] == 153.0
        assert price_by_date[date(2024, 8, 2)]['close'] == 157.0
        assert price_by_date[date(2024, 8, 2)]['volume'] == 1200000


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_end_to_end_tiingo_backfill(unit_test_db):
    """Test end-to-end Tiingo price backfill with real database."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Setup test data: create test instrument
    from domains.instruments.repositories.instruments_dao import InstrumentsDAO
    from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
    from dao.vendors_dao import VendorsDAO

    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    vendors_dao = VendorsDAO(env)

    # Create Tiingo vendor if it doesn't exist
    tiingo_vendor = await vendors_core.dao.get_vendor_by_name("tiingo")
    if not tiingo_vendor:
        tiingo_vendor_id = await vendors_core.dao.create_vendor(
            "tiingo",
            description="Tiingo.com",
            api_key_env_var="TIINGO_API_KEY"
        )
    else:
        tiingo_vendor_id = tiingo_vendor["id"]

    # Create test instrument
    test_instrument_id = await instruments_core.dao.create_instrument(
        symbol="TESTTIINGO",
        name="Test Tiingo Corp",
        exchange="NYSE",
        type_="CS",
        currency="USD",
        list_date=date(2020, 1, 1)
    )

    # Create instrument cross-reference
    await xrefs_core.dao.create_xref(
        instrument_id=test_instrument_id,
        vendor_id=tiingo_vendor_id,
        symbol="TESTTIINGO",
        start_at=date(2020, 1, 1)
    )

    # Mock API response data
    sample_data = [
        {
            'date': date(2024, 8, 1),
            'instrument_id': test_instrument_id,
            'open': 100.0,
            'high': 105.0,
            'low': 99.0,
            'close': 103.0,
            'volume': 500000
        }
    ]

    # Test database insertion
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert sample data
        inserted_count = await db_inserter.bulk_insert_tiingo(sample_data)
        assert inserted_count == 1

        # Verify data was inserted correctly
        from vendor.tiingo.core.dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
        prices_dao = DailyPricesTiingoDAO(env)

        # Get inserted prices
        prices = await prices_core.dao.list_prices(test_instrument_id)
        assert len(prices) == 1

        # Verify price data
        price = prices[0]
        assert price['date'] == date(2024, 8, 1)
        assert price['open'] == 100.0
        assert price['close'] == 103.0
        assert price['volume'] == 500000


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_duplicate_handling(unit_test_db):
    """Test that duplicate price data is handled correctly."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Setup test data
    from domains.instruments.repositories.instruments_dao import InstrumentsDAO
    from dao.vendors_dao import VendorsDAO

    instruments_dao = InstrumentsDAO(env)
    vendors_dao = VendorsDAO(env)

    # Create Polygon vendor if it doesn't exist
    polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    if not polygon_vendor:
        polygon_vendor_id = await vendors_core.dao.create_vendor(
            "polygon",
            description="Polygon.io",
            api_key_env_var="POLYGON_API_KEY"
        )
    else:
        polygon_vendor_id = polygon_vendor["id"]

    # Create test instrument
    test_instrument_id = await instruments_core.dao.create_instrument(
        symbol="TESTDUP",
        name="Test Duplicate Corp",
        exchange="NYSE",
        type_="CS",
        currency="USD",
        list_date=date(2020, 1, 1)
    )

    # Sample data with potential duplicate
    sample_data = [
        {
            'date': date(2024, 8, 1),
            'instrument_id': test_instrument_id,
            'open': 150.0,
            'high': 155.0,
            'low': 149.0,
            'close': 153.0,
            'volume': 1000000
        }
    ]

    # Database config
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert data first time
        first_insert = await db_inserter.bulk_insert_polygon(sample_data)
        assert first_insert == 1

        # Insert same data again (should handle duplicates)
        second_insert = await db_inserter.bulk_insert_polygon(sample_data)
        # Should still report 1 (attempted insert) but no actual duplicates
        assert second_insert == 1

        # Verify only one record exists
        from vendor.polygon.core.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
        prices_dao = DailyPricesPolygonDAO(env)

        prices = await prices_core.dao.list_prices(test_instrument_id)
        assert len(prices) == 1  # Should still be only 1 record


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_concurrent_database_operations(unit_test_db):
    """Test concurrent database insertions work correctly."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Setup test data
    from domains.instruments.repositories.instruments_dao import InstrumentsDAO
    from dao.vendors_dao import VendorsDAO

    instruments_dao = InstrumentsDAO(env)
    vendors_dao = VendorsDAO(env)

    # Create vendors if they don't exist
    polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    if not polygon_vendor:
        polygon_vendor_id = await vendors_core.dao.create_vendor(
            "polygon",
            description="Polygon.io",
            api_key_env_var="POLYGON_API_KEY"
        )
    else:
        polygon_vendor_id = polygon_vendor["id"]

    # Create test instruments
    instrument_ids = []
    for i in range(3):
        instrument_id = await instruments_core.dao.create_instrument(
            symbol=f"TESTCONC{i}",
            name=f"Test Concurrent Corp {i}",
            exchange="NYSE",
            type_="CS",
            currency="USD",
            list_date=date(2020, 1, 1)
        )
        instrument_ids.append(instrument_id)

    # Create sample data for each instrument
    all_sample_data = []
    for i, instrument_id in enumerate(instrument_ids):
        sample_data = [
            {
                'date': date(2024, 8, 1),
                'instrument_id': instrument_id,
                'open': 100.0 + i * 10,
                'high': 105.0 + i * 10,
                'low': 99.0 + i * 10,
                'close': 103.0 + i * 10,
                'volume': 1000000 + i * 100000
            }
        ]
        all_sample_data.append(sample_data)

    # Database config
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboDatabaseInserter(db_config, pool_size=3) as db_inserter:
        # Execute concurrent insertions
        tasks = [
            db_inserter.bulk_insert_polygon(data)
            for data in all_sample_data
        ]

        results = await asyncio.gather(*tasks)

        # Verify all insertions succeeded
        assert all(result == 1 for result in results)

        # Verify data was inserted correctly for each instrument
        from vendor.polygon.core.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
        prices_dao = DailyPricesPolygonDAO(env)

        for i, instrument_id in enumerate(instrument_ids):
            prices = await prices_core.dao.list_prices(instrument_id)
            assert len(prices) == 1

            price = prices[0]
            assert price['open'] == 100.0 + i * 10
            assert price['close'] == 103.0 + i * 10
            assert price['volume'] == 1000000 + i * 100000


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_large_batch_processing(unit_test_db):
    """Test processing of large batches of price data."""
    # Setup test environment
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Setup test data
    from domains.instruments.repositories.instruments_dao import InstrumentsDAO
    from dao.vendors_dao import VendorsDAO

    instruments_dao = InstrumentsDAO(env)
    vendors_dao = VendorsDAO(env)

    # Create Polygon vendor if it doesn't exist
    polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    if not polygon_vendor:
        polygon_vendor_id = await vendors_core.dao.create_vendor(
            "polygon",
            description="Polygon.io",
            api_key_env_var="POLYGON_API_KEY"
        )
    else:
        polygon_vendor_id = polygon_vendor["id"]

    # Create test instrument
    test_instrument_id = await instruments_core.dao.create_instrument(
        symbol="TESTBATCH",
        name="Test Batch Corp",
        exchange="NYSE",
        type_="CS",
        currency="USD",
        list_date=date(2020, 1, 1)
    )

    # Create large batch of sample data (100 days)
    from datetime import timedelta

    large_batch = []
    base_date = date(2024, 1, 1)

    for i in range(100):
        sample_record = {
            'date': base_date + timedelta(days=i),
            'instrument_id': test_instrument_id,
            'open': 100.0 + i * 0.1,
            'high': 105.0 + i * 0.1,
            'low': 99.0 + i * 0.1,
            'close': 103.0 + i * 0.1,
            'volume': 1000000 + i * 1000
        }
        large_batch.append(sample_record)

    # Database config
    db_config = {
        'host': env.get_database_config()['host'],
        'port': env.get_database_config()['port'],
        'user': env.get_database_config()['user'],
        'password': env.get_database_config()['password'],
        'database': env.get_database_config()['database']
    }

    async with TurboDatabaseInserter(db_config, pool_size=1) as db_inserter:
        # Insert large batch
        inserted_count = await db_inserter.bulk_insert_polygon(large_batch)
        assert inserted_count == 100

        # Verify all data was inserted
        from vendor.polygon.core.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
        prices_dao = DailyPricesPolygonDAO(env)

        prices = await prices_core.dao.list_prices(test_instrument_id)
        assert len(prices) == 100

        # Verify data integrity (check first and last records)
        prices_by_date = {p['date']: p for p in prices}

        first_record = prices_by_date[base_date]
        assert first_record['open'] == 100.0
        assert first_record['close'] == 103.0
        assert first_record['volume'] == 1000000

        last_record = prices_by_date[base_date + timedelta(days=99)]
        assert last_record['open'] == 100.0 + 99 * 0.1
        assert last_record['close'] == 103.0 + 99 * 0.1
        assert last_record['volume'] == 1000000 + 99 * 1000