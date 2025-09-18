import pytest
from db.migration_manager import MigrationManager

@pytest.mark.unit
def test_select_statement_prefixing():
    """Test that SELECT statements are correctly prefixed."""
    manager = MigrationManager("postgresql://postgres:password@localhost:5432/test_db")

    # Test simple SELECT statement
    sql = "SELECT * FROM daily_price_polygon;"
    prefixed_sql = manager._apply_table_prefixes(sql)
    assert "SELECT * FROM test_daily_price_polygon" in prefixed_sql

    # Test SELECT with WHERE clause
    sql = "SELECT * FROM daily_price_polygon WHERE date = '2025-01-01';"
    prefixed_sql = manager._apply_table_prefixes(sql)
    assert "SELECT * FROM test_daily_price_polygon WHERE date = '2025-01-01'" in prefixed_sql

    # Test SELECT with JOIN
    sql = "SELECT * FROM daily_price_polygon JOIN instruments ON daily_price_polygon.instrument_id = instruments.id;"
    prefixed_sql = manager._apply_table_prefixes(sql)
    assert "SELECT * FROM test_daily_price_polygon JOIN test_instruments" in prefixed_sql
    assert "test_daily_price_polygon.instrument_id = test_instruments.id" in prefixed_sql

@pytest.mark.unit
def test_foreign_key_references_prefixing():
    """Test that REFERENCES in foreign key constraints are correctly prefixed."""
    manager = MigrationManager("postgresql://postgres:password@localhost:5432/test_db")

    # Test REFERENCES in CREATE TABLE
    sql = """
    CREATE TABLE daily_price_polygon (
        id SERIAL PRIMARY KEY,
        instrument_id INTEGER REFERENCES instruments(id),
        date DATE NOT NULL
    );
    """
    prefixed_sql = manager._apply_table_prefixes(sql)
    assert "CREATE TABLE test_daily_price_polygon" in prefixed_sql
    assert "REFERENCES test_instruments(id)" in prefixed_sql

    # Test REFERENCES with ON DELETE CASCADE
    sql = """
    CREATE TABLE universe_membership (
        universe_id INTEGER REFERENCES universe(id) ON DELETE CASCADE,
        instrument_id INTEGER REFERENCES instruments(id) ON DELETE CASCADE,
        PRIMARY KEY (universe_id, instrument_id)
    );
    """
    prefixed_sql = manager._apply_table_prefixes(sql)
    assert "REFERENCES test_universe(id) ON DELETE CASCADE" in prefixed_sql
    assert "REFERENCES test_instruments(id) ON DELETE CASCADE" in prefixed_sql

@pytest.mark.unit
def test_regclass_cast_prefixing():
    """Test that regclass casts are correctly prefixed."""
    manager = MigrationManager("postgresql://postgres:password@localhost:5432/test_db")

    # Test regclass cast in DO block
    sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE
            conrelid = 'universe_state_interval'::regclass AND
            conname = 'universe_state_interval_pkey'
        ) THEN
            ALTER TABLE universe_state_interval ADD PRIMARY KEY (universe_id, date);
        END IF;
    END
    $$;
    """
    prefixed_sql = manager._apply_table_prefixes(sql)
    assert "'test_universe_state_interval'::regclass" in prefixed_sql
    assert "ALTER TABLE test_universe_state_interval" in prefixed_sql

@pytest.mark.unit
def test_no_double_prefixing():
    """Test that tables are not double-prefixed."""
    manager = MigrationManager("postgresql://postgres:password@localhost:5432/test_db")

    # Test already prefixed table name
    sql = """
    CREATE TABLE test_events (id SERIAL PRIMARY KEY);
    INSERT INTO test_events (id) VALUES (1);
    """
    prefixed_sql = manager._apply_table_prefixes(sql)
    assert "CREATE TABLE test_events" in prefixed_sql
    assert "INSERT INTO test_events" in prefixed_sql
    assert "test_test_events" not in prefixed_sql

@pytest.mark.unit
def test_complex_sql_prefixing():
    """Test prefixing in complex SQL with multiple statements and comments."""
    manager = MigrationManager("postgresql://postgres:password@localhost:5432/test_db")

    sql = """
    -- Create tables
    CREATE TABLE instruments (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    );

    /*
     * Create daily_price_polygon table with foreign key
     */
    CREATE TABLE daily_price_polygon (
        id SERIAL PRIMARY KEY,
        instrument_id INTEGER REFERENCES instruments(id),
        date DATE NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT
    );

    -- Add index
    CREATE INDEX idx_daily_price_polygon_instrument_date ON daily_price_polygon(instrument_id, date);

    -- Insert some data
    INSERT INTO instruments (name) VALUES ('AAPL'), ('TSLA');
    """

    prefixed_sql = manager._apply_table_prefixes(sql)

    # Check table creation
    assert "CREATE TABLE test_instruments" in prefixed_sql
    assert "CREATE TABLE test_daily_price_polygon" in prefixed_sql

    # Check foreign key reference
    assert "REFERENCES test_instruments(id)" in prefixed_sql

    # Check index creation
    assert "CREATE INDEX idx_daily_price_polygon_instrument_date ON test_daily_price_polygon" in prefixed_sql

    # Check insert statement
    assert "INSERT INTO test_instruments" in prefixed_sql
