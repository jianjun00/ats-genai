"""
Domain entity schema definitions for ATS platform.

Contains complete type definitions for financial entities including
instruments, exchanges, prices, and cross-references.
"""

from .types import FieldDefinition, FieldType, FieldSemantics, EntitySchema

# =============================================================================
# ENUM VALUE DEFINITIONS (Central source of truth)
# =============================================================================

EXCHANGE_VALUES = [
    "NYSE",      # New York Stock Exchange
    "NASDAQ",    # NASDAQ Global Market
    "AMEX",      # NYSE American
    "LSE",       # London Stock Exchange
    "TSE",       # Tokyo Stock Exchange
    "XETRA",     # Deutsche Börse XETRA
    "TSX",       # Toronto Stock Exchange
    "EURONEXT",  # Euronext
    "OTC",       # Over-the-Counter
    "PINK",      # OTC Pink Market
    "CBOE",      # Chicago Board Options Exchange
    "BATS",      # CBOE BZX Exchange
    "IEX",       # Investors Exchange
]

INSTRUMENT_TYPE_VALUES = [
    "STOCK",        # Common stock
    "ETF",          # Exchange-traded fund
    "MUTUAL_FUND",  # Mutual fund
    "BOND",         # Corporate/government bond
    "OPTION",       # Options contract
    "FUTURE",       # Futures contract
    "FOREX",        # Foreign exchange
    "CRYPTO",       # Cryptocurrency
    "INDEX",        # Market index
    "ADR",          # American Depositary Receipt
    "WARRANT",      # Warrant
    "RIGHT",        # Rights offering
]

CURRENCY_VALUES = [
    "USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "HKD", "SGD"
]

# =============================================================================
# INSTRUMENT SCHEMA
# =============================================================================

INSTRUMENT_SCHEMA = EntitySchema(
    entity_name="instrument",
    table_name="dev_instruments",
    description="Master data for financial instruments",
    primary_key=["id"],
    indexes=[["symbol"], ["exchange"], ["type"], ["active"]],
    fields={
        "id": FieldDefinition(
            name="id",
            field_type=FieldType.INTEGER,
            semantics=FieldSemantics.READONLY,
            nullable=False,
            description="Unique identifier for instrument",
            ui_label="ID",
            eda_priority=0,
            eda_default_visible=False
        ),

        "symbol": FieldDefinition(
            name="symbol",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            nullable=False,
            max_length=20,
            validation_regex=r"^[A-Z0-9\.\-\/]{1,20}$",
            description="Ticker symbol for the instrument",
            ui_label="Symbol",
            ui_placeholder="AAPL, BRK.A, etc.",
            ui_help_text="Stock ticker symbol - supports partial search",
            ui_group="Basic Info",
            eda_priority=10,  # Highest priority for filtering
            eda_default_visible=True
        ),

        "name": FieldDefinition(
            name="name",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            nullable=True,
            max_length=255,
            description="Full company or instrument name",
            ui_label="Company Name",
            ui_placeholder="Apple Inc., Berkshire Hathaway Inc., etc.",
            ui_help_text="Full company name - supports partial search",
            ui_group="Basic Info",
            eda_priority=8,
            eda_default_visible=True
        ),

        "exchange": FieldDefinition(
            name="exchange",
            field_type=FieldType.ENUM,
            semantics=FieldSemantics.CATEGORICAL,
            nullable=True,
            enum_values=EXCHANGE_VALUES,
            description="Primary trading exchange for the instrument",
            ui_label="Exchange",
            ui_help_text="Stock exchange where instrument is primarily traded",
            ui_group="Trading Info",
            eda_priority=9,  # Very high priority
            eda_default_visible=True
        ),

        "type": FieldDefinition(
            name="type",
            field_type=FieldType.ENUM,
            semantics=FieldSemantics.CATEGORICAL,
            nullable=True,
            enum_values=INSTRUMENT_TYPE_VALUES,
            description="Type of financial instrument",
            ui_label="Instrument Type",
            ui_help_text="Category of financial instrument",
            ui_group="Basic Info",
            eda_priority=7,
            eda_default_visible=True
        ),

        "currency": FieldDefinition(
            name="currency",
            field_type=FieldType.ENUM,
            semantics=FieldSemantics.CATEGORICAL,
            nullable=True,
            enum_values=CURRENCY_VALUES,
            description="Trading currency for the instrument",
            ui_label="Currency",
            ui_help_text="Currency denomination for prices",
            ui_group="Trading Info",
            eda_priority=5,
            eda_default_visible=False
        ),

        "active": FieldDefinition(
            name="active",
            field_type=FieldType.BOOLEAN,
            semantics=FieldSemantics.BOOLEAN,
            nullable=False,
            description="Whether instrument is currently active/trading",
            ui_label="Active",
            ui_help_text="Currently active and trading",
            ui_group="Status",
            eda_priority=8,
            eda_default_visible=True
        ),

        "list_date": FieldDefinition(
            name="list_date",
            field_type=FieldType.DATE,
            semantics=FieldSemantics.DATE_RANGE,
            nullable=True,
            description="Date when instrument first started trading",
            ui_label="Listing Date",
            ui_help_text="Date of initial public offering or listing",
            ui_group="Dates",
            eda_priority=4,
            eda_default_visible=False
        ),

        "delist_date": FieldDefinition(
            name="delist_date",
            field_type=FieldType.DATE,
            semantics=FieldSemantics.DATE_RANGE,
            nullable=True,
            description="Date when instrument was delisted (if applicable)",
            ui_label="Delisting Date",
            ui_help_text="Date instrument stopped trading",
            ui_group="Dates",
            eda_priority=3,
            eda_default_visible=False
        ),

        "sector": FieldDefinition(
            name="sector",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.CATEGORICAL,
            nullable=True,
            max_length=100,
            description="Business sector classification",
            ui_label="Sector",
            ui_help_text="Industry sector (Technology, Healthcare, etc.)",
            ui_group="Classification",
            eda_priority=6,
            eda_default_visible=False
        ),

        "created_at": FieldDefinition(
            name="created_at",
            field_type=FieldType.DATETIME,
            semantics=FieldSemantics.READONLY,
            nullable=True,
            description="Record creation timestamp",
            ui_label="Created At",
            ui_group="Metadata",
            eda_priority=0,
            eda_default_visible=False
        ),

        "updated_at": FieldDefinition(
            name="updated_at",
            field_type=FieldType.DATETIME,
            semantics=FieldSemantics.READONLY,
            nullable=True,
            description="Record last update timestamp",
            ui_label="Updated At",
            ui_group="Metadata",
            eda_priority=0,
            eda_default_visible=False
        )
    }
)

# =============================================================================
# DAILY PRICE SCHEMA
# =============================================================================

PRICE_SCHEMA = EntitySchema(
    entity_name="daily_price",
    table_name="dev_daily_price_polygon",
    description="Daily OHLCV price data for instruments",
    primary_key=["symbol", "date"],
    indexes=[["symbol"], ["date"], ["symbol", "date"]],
    fields={
        "symbol": FieldDefinition(
            name="symbol",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.CATEGORICAL,  # For price data, symbol is categorical filter
            nullable=False,
            max_length=20,
            description="Instrument symbol",
            ui_label="Symbol",
            ui_help_text="Stock symbol for filtering",
            ui_group="Instrument",
            eda_priority=10,
            eda_default_visible=True
        ),

        "date": FieldDefinition(
            name="date",
            field_type=FieldType.DATE,
            semantics=FieldSemantics.DATE_RANGE,
            nullable=False,
            description="Trading date for price data",
            ui_label="Date",
            ui_help_text="Date range for price data",
            ui_group="Time",
            eda_priority=9,
            eda_default_visible=True
        ),

        "open": FieldDefinition(
            name="open",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            min_value=0,
            description="Opening price for the trading day",
            ui_label="Open Price ($)",
            ui_help_text="Opening price range filter",
            ui_group="OHLC",
            eda_priority=6,
            eda_default_visible=True
        ),

        "high": FieldDefinition(
            name="high",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            min_value=0,
            description="Highest price during trading day",
            ui_label="High Price ($)",
            ui_help_text="High price range filter",
            ui_group="OHLC",
            eda_priority=5,
            eda_default_visible=True
        ),

        "low": FieldDefinition(
            name="low",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            min_value=0,
            description="Lowest price during trading day",
            ui_label="Low Price ($)",
            ui_help_text="Low price range filter",
            ui_group="OHLC",
            eda_priority=5,
            eda_default_visible=True
        ),

        "close": FieldDefinition(
            name="close",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            min_value=0,
            description="Closing price for the trading day",
            ui_label="Close Price ($)",
            ui_help_text="Closing price range filter",
            ui_group="OHLC",
            eda_priority=8,  # Close price very important
            eda_default_visible=True
        ),

        "volume": FieldDefinition(
            name="volume",
            field_type=FieldType.INTEGER,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            min_value=0,
            description="Trading volume for the day",
            ui_label="Volume",
            ui_help_text="Trading volume range",
            ui_group="Volume",
            eda_priority=7,
            eda_default_visible=True
        )
    }
)

# =============================================================================
# INSTRUMENT CROSS-REFERENCE SCHEMA
# =============================================================================

INSTRUMENT_XREF_SCHEMA = EntitySchema(
    entity_name="instrument_xref",
    table_name="instrument_xrefs",
    description="Cross-reference mappings between internal and external instrument identifiers",
    primary_key=["id"],
    indexes=[["instrument_id"], ["vendor_id"], ["external_symbol"]],
    fields={
        "id": FieldDefinition(
            name="id",
            field_type=FieldType.INTEGER,
            semantics=FieldSemantics.READONLY,
            nullable=False,
            description="Unique identifier for cross-reference",
            ui_label="ID",
            eda_priority=0,
            eda_default_visible=False
        ),

        "instrument_id": FieldDefinition(
            name="instrument_id",
            field_type=FieldType.INTEGER,
            semantics=FieldSemantics.CATEGORICAL,
            nullable=False,
            description="Internal instrument ID reference",
            ui_label="Instrument ID",
            ui_help_text="Internal instrument identifier",
            ui_group="References",
            eda_priority=3,
            eda_default_visible=False
        ),

        "vendor_id": FieldDefinition(
            name="vendor_id",
            field_type=FieldType.INTEGER,
            semantics=FieldSemantics.CATEGORICAL,
            nullable=False,
            description="Data vendor ID reference",
            ui_label="Vendor ID",
            ui_help_text="Data provider identifier",
            ui_group="References",
            eda_priority=5,
            eda_default_visible=True
        ),

        "external_symbol": FieldDefinition(
            name="external_symbol",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            nullable=False,
            max_length=50,
            description="External vendor-specific symbol",
            ui_label="External Symbol",
            ui_placeholder="AAPL, BRK-A, etc.",
            ui_help_text="Vendor-specific symbol - supports search",
            ui_group="Symbols",
            eda_priority=8,
            eda_default_visible=True
        ),

        "start_date": FieldDefinition(
            name="start_date",
            field_type=FieldType.DATE,
            semantics=FieldSemantics.DATE_RANGE,
            nullable=False,
            description="Date when mapping became effective",
            ui_label="Start Date",
            ui_help_text="When this mapping started",
            ui_group="Dates",
            eda_priority=4,
            eda_default_visible=False
        ),

        "end_date": FieldDefinition(
            name="end_date",
            field_type=FieldType.DATE,
            semantics=FieldSemantics.DATE_RANGE,
            nullable=True,
            description="Date when mapping expired (NULL if still active)",
            ui_label="End Date",
            ui_help_text="When this mapping ended",
            ui_group="Dates",
            eda_priority=3,
            eda_default_visible=False
        )
    }
)

# =============================================================================
# SCHEMA REGISTRY ENTRIES
# =============================================================================

ALL_SCHEMAS = {
    "instrument": INSTRUMENT_SCHEMA,
    "daily_price": PRICE_SCHEMA,
    "instrument_xref": INSTRUMENT_XREF_SCHEMA
}

# Additional table mappings for legacy tables
TABLE_SCHEMA_MAPPING = {
    "dev_instruments": INSTRUMENT_SCHEMA,
    "dev_instrument_tiingo": INSTRUMENT_SCHEMA,  # Same structure
    "dev_instrument_polygon": INSTRUMENT_SCHEMA,  # Same structure
    "dev_daily_price_polygon": PRICE_SCHEMA,
    "dev_daily_price_tiingo": PRICE_SCHEMA,
    "dev_daily_price_eodhd": PRICE_SCHEMA,
    "instrument_xrefs": INSTRUMENT_XREF_SCHEMA
}