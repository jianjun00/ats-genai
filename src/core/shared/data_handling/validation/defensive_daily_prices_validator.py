#!/usr/bin/env python3
"""
Defensive Daily Prices Validation System

Enhanced validation for daily price data with comprehensive defensive coding:
- Input sanitization and type safety for all financial data
- SQL injection prevention for all database operations
- Circuit breaker protection for external services
- Resource management with connection pooling and timeouts
- Secure error handling with audit logging
- Rate limiting for validation operations
- Memory usage monitoring and cleanup

This validator implements security-first principles for financial data validation.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from decimal import Decimal
import json

# Defensive imports with graceful degradation
try:
    from core.platform.config_env.environment import Environment
except ImportError:
    try:
        from core.platform.config_env.environment import Environment
    except ImportError:
        # Emergency environment class for system stability
        class Environment:
            def get_table_name(self, name: str) -> str:
                return f"dev_{name}"

# Import our defensive components
from core.defensive import (
    DefensiveFinancialValidator,
    SecurityLevel,
    SecureErrorHandler,
    ErrorCategory,
    ErrorSeverity,
    DefensiveResourceManager,
    ResourceType,
    ResourceLimits,
    secure_financial_operation,
    with_validation_error_handling
)

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Validation issue severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    SECURITY = "security"  # New: Security-related validation issues


class ValidationRule(Enum):
    """Types of validation rules"""
    MISSING_DATA = "missing_data"
    NULL_VALUES = "null_values"
    NEGATIVE_PRICES = "negative_prices"
    ZERO_PRICES = "zero_prices"
    EXTREME_PRICES = "extreme_prices"
    OHLC_CONSISTENCY = "ohlc_consistency"
    VOLUME_ANOMALY = "volume_anomaly"
    PRICE_GAPS = "price_gaps"
    STALE_DATA = "stale_data"
    CROSS_VENDOR_MISMATCH = "cross_vendor_mismatch"
    TRADING_HALT = "trading_halt"
    SPLIT_ADJUSTMENT = "split_adjustment"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"  # New: Security validation
    DATA_INTEGRITY = "data_integrity"  # New: Data integrity checks


@dataclass
class DefensiveValidationIssue:
    """Secure validation issue with audit trail"""
    rule: ValidationRule
    severity: ValidationSeverity
    instrument_symbol: str
    instrument_id: int
    date: date
    vendor: str
    message: str  # Sanitized message safe for display
    technical_details: str  # Full details for audit only
    detected_at: datetime = field(default_factory=datetime.utcnow)
    resolved: bool = False
    resolution_action: Optional[str] = None
    audit_hash: str = field(default="")
    user_id_hash: Optional[str] = None

    def __post_init__(self):
        if not self.audit_hash:
            import hashlib
            audit_data = f"{self.rule.value}:{self.instrument_symbol}:{self.date}:{self.detected_at}"
            self.audit_hash = hashlib.sha256(audit_data.encode()).hexdigest()[:16]


@dataclass
class DefensivePriceRecord:
    """Defensively validated price record"""
    instrument_id: int
    symbol: str
    date: date
    open: Optional[Decimal]
    high: Optional[Decimal]
    low: Optional[Decimal]
    close: Optional[Decimal]
    volume: Optional[int]
    vendor: str
    adjclose: Optional[Decimal] = None
    validation_passed: bool = False
    validation_errors: List[str] = field(default_factory=list)
    sanitized: bool = False


class DefensiveDailyPricesValidator:
    """
    Comprehensive defensive validator for daily prices data.

    Implements all defensive coding principles:
    - Never trust input data - validate everything
    - Use parameterized queries to prevent SQL injection
    - Sanitize all output for logging
    - Implement circuit breakers for database operations
    - Use resource management for connections
    - Provide comprehensive audit logging
    """

    def __init__(self, database_url: str, env: Environment = None):
        self.database_url = database_url
        self.env = env or Environment()

        # Defensive components
        self.financial_validator = DefensiveFinancialValidator(SecurityLevel.CRITICAL)
        self.error_handler = SecureErrorHandler("price_validator")
        self.resource_manager = DefensiveResourceManager(
            ResourceType.DATABASE,
            ResourceLimits(
                max_connections=5,
                connection_timeout=30.0,
                query_timeout=120.0,
                max_retries=3
            )
        )

        # Validation thresholds with defensive limits
        self.thresholds = {
            "max_price_change_pct": 50.0,
            "min_volume": 100,
            "max_volume_multiplier": 50.0,
            "min_price": Decimal('0.01'),
            "max_price": Decimal('10000.0'),
            "stale_data_days": 5,
            "cross_vendor_tolerance_pct": 5.0,
            "ohlc_tolerance_pct": 0.1,
            "max_records_per_batch": 10000,  # Prevent memory exhaustion
            "max_validation_time": 300.0  # 5 minute timeout
        }

        # Tables with defensive naming
        self.issues_table = self.env.get_table_name("price_validation_issues")
        self.audit_table = self.env.get_table_name("validation_audit_log")

        logger.info("Defensive daily prices validator initialized with security controls")

    @secure_financial_operation(ErrorCategory.VALIDATION)
    async def initialize(self):
        """Initialize validation system with defensive database setup"""
        try:
            async with self.resource_manager.defensive_database_connection(self.database_url) as conn:
                await self._create_validation_tables_secure(conn)
                await self._create_audit_table_secure(conn)
                logger.info("Defensive validation system initialized successfully")
        except Exception as e:
            self.error_handler.handle_error(
                e, ErrorCategory.DATABASE, ErrorSeverity.CRITICAL,
                "Failed to initialize validation system"
            )
            raise

    async def _create_validation_tables_secure(self, conn: asyncpg.Connection):
        """Create validation tables with defensive SQL"""
        # Using parameterized table name is not possible, but we sanitize it
        table_name = self._sanitize_table_name(self.issues_table)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            rule_type VARCHAR(50) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            instrument_symbol VARCHAR(10) NOT NULL,
            instrument_id INTEGER NOT NULL,
            validation_date DATE NOT NULL,
            vendor VARCHAR(50) NOT NULL,
            message TEXT NOT NULL,
            technical_details TEXT,
            detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            resolved BOOLEAN DEFAULT FALSE,
            resolution_action TEXT,
            audit_hash VARCHAR(32) NOT NULL,
            user_id_hash VARCHAR(32),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_validation_issues_symbol_date
        ON {table_name} (instrument_symbol, validation_date);

        CREATE INDEX IF NOT EXISTS idx_validation_issues_severity
        ON {table_name} (severity, detected_at);

        CREATE INDEX IF NOT EXISTS idx_validation_audit_hash
        ON {table_name} (audit_hash);
        """

        await conn.execute(create_sql)

    async def _create_audit_table_secure(self, conn: asyncpg.Connection):
        """Create audit table for compliance logging"""
        table_name = self._sanitize_table_name(self.audit_table)

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            operation_type VARCHAR(50) NOT NULL,
            operation_details JSONB NOT NULL,
            user_id_hash VARCHAR(32),
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            audit_hash VARCHAR(64) NOT NULL,
            ip_address VARCHAR(15),
            session_id VARCHAR(64)
        );

        CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp
        ON {table_name} (timestamp);

        CREATE INDEX IF NOT EXISTS idx_audit_log_operation
        ON {table_name} (operation_type);
        """

        await conn.execute(create_sql)

    def _sanitize_table_name(self, table_name: str) -> str:
        """Sanitize table name to prevent SQL injection"""
        import re
        # Only allow alphanumeric, underscore, and dots
        sanitized = re.sub(r'[^a-zA-Z0-9_.]', '', table_name)
        if not sanitized or len(sanitized) > 63:  # PostgreSQL limit
            raise ValueError(f"Invalid table name: {table_name}")
        return sanitized

    @with_validation_error_handling
    def validate_price_record_defensive(self, raw_record: Dict[str, Any]) -> DefensivePriceRecord:
        """
        Defensively validate a single price record.

        Args:
            raw_record: Raw price data dictionary (untrusted)

        Returns:
            Validated and sanitized price record
        """
        validation_errors = []

        # Step 1: Defensive type checking and conversion
        try:
            # Validate required fields exist
            required_fields = ['symbol', 'date', 'vendor']
            for field in required_fields:
                if field not in raw_record:
                    validation_errors.append(f"Missing required field: {field}")

            if validation_errors:
                raise ValueError(f"Missing required fields: {validation_errors}")

            # Defensive symbol validation
            symbol_validation = self.financial_validator.validate_symbol(raw_record.get('symbol'))
            if not symbol_validation.is_valid:
                validation_errors.append(f"Symbol validation failed: {symbol_validation.message}")

            # Defensive date validation
            date_validation = self.financial_validator.validate_trading_date(raw_record.get('date'))
            if not date_validation.is_valid:
                validation_errors.append(f"Date validation failed: {date_validation.message}")

            # Defensive price validations
            price_fields = ['open', 'high', 'low', 'close', 'adjclose']
            validated_prices = {}

            for field in price_fields:
                if field in raw_record and raw_record[field] is not None:
                    price_validation = self.financial_validator.validate_price(
                        raw_record[field], field_name=field
                    )
                    if not price_validation.is_valid:
                        validation_errors.append(f"{field} validation failed: {price_validation.message}")
                    else:
                        validated_prices[field] = Decimal(str(raw_record[field]))

            # OHLC consistency validation if we have all required prices
            if all(field in validated_prices for field in ['open', 'high', 'low', 'close']):
                ohlc_validation = self.financial_validator.validate_ohlc_consistency(
                    validated_prices['open'],
                    validated_prices['high'],
                    validated_prices['low'],
                    validated_prices['close']
                )
                if not ohlc_validation.is_valid:
                    validation_errors.append(f"OHLC consistency failed: {ohlc_validation.message}")

            # Volume validation
            volume = None
            if 'volume' in raw_record and raw_record['volume'] is not None:
                try:
                    volume = int(raw_record['volume'])
                    if volume < 0:
                        validation_errors.append("Volume cannot be negative")
                except (ValueError, TypeError):
                    validation_errors.append("Invalid volume format")

            # Create validated record
            record = DefensivePriceRecord(
                instrument_id=int(raw_record.get('instrument_id', 0)),
                symbol=symbol_validation.input_value if symbol_validation.is_valid else '',
                date=datetime.strptime(str(raw_record.get('date')), '%Y-%m-%d').date() if date_validation.is_valid else date.today(),
                open=validated_prices.get('open'),
                high=validated_prices.get('high'),
                low=validated_prices.get('low'),
                close=validated_prices.get('close'),
                adjclose=validated_prices.get('adjclose'),
                volume=volume,
                vendor=str(raw_record.get('vendor', '')),
                validation_passed=len(validation_errors) == 0,
                validation_errors=validation_errors,
                sanitized=True
            )

            return record

        except Exception as e:
            # Create error record for failed validation
            return DefensivePriceRecord(
                instrument_id=0,
                symbol="INVALID",
                date=date.today(),
                open=None,
                high=None,
                low=None,
                close=None,
                volume=None,
                vendor="unknown",
                validation_passed=False,
                validation_errors=[f"Validation exception: {str(e)}"],
                sanitized=False
            )

    @secure_financial_operation(ErrorCategory.VALIDATION)
    async def validate_batch_defensive(self,
                                     raw_records: List[Dict[str, Any]],
                                     batch_id: str = None) -> Tuple[List[DefensivePriceRecord], List[DefensiveValidationIssue]]:
        """
        Defensively validate a batch of price records.

        Args:
            raw_records: List of raw price data dictionaries
            batch_id: Optional batch identifier for audit

        Returns:
            Tuple of (validated_records, validation_issues)
        """
        if not isinstance(raw_records, list):
            raise ValueError("raw_records must be a list")

        # Defensive batch size check
        if len(raw_records) > self.thresholds['max_records_per_batch']:
            raise ValueError(f"Batch too large: {len(raw_records)} > {self.thresholds['max_records_per_batch']}")

        validated_records = []
        validation_issues = []

        start_time = datetime.utcnow()

        try:
            # Process each record with defensive validation
            for i, raw_record in enumerate(raw_records):
                # Timeout check to prevent infinite processing
                if (datetime.utcnow() - start_time).total_seconds() > self.thresholds['max_validation_time']:
                    raise TimeoutError(f"Validation timeout after {self.thresholds['max_validation_time']}s")

                try:
                    validated_record = self.validate_price_record_defensive(raw_record)
                    validated_records.append(validated_record)

                    # Create validation issues for failed validations
                    if not validated_record.validation_passed:
                        for error in validated_record.validation_errors:
                            issue = DefensiveValidationIssue(
                                rule=ValidationRule.DATA_INTEGRITY,
                                severity=ValidationSeverity.ERROR,
                                instrument_symbol=validated_record.symbol,
                                instrument_id=validated_record.instrument_id,
                                date=validated_record.date,
                                vendor=validated_record.vendor,
                                message=self._sanitize_message(error),
                                technical_details=f"Record {i}: {error}"
                            )
                            validation_issues.append(issue)

                except Exception as e:
                    # Handle individual record validation failure
                    self.error_handler.handle_error(
                        e, ErrorCategory.VALIDATION, ErrorSeverity.HIGH,
                        f"Failed to validate record {i}",
                        should_raise=False
                    )

                    # Create error issue
                    issue = DefensiveValidationIssue(
                        rule=ValidationRule.SUSPICIOUS_ACTIVITY,
                        severity=ValidationSeverity.CRITICAL,
                        instrument_symbol="UNKNOWN",
                        instrument_id=0,
                        date=date.today(),
                        vendor="unknown",
                        message="Record validation failed with exception",
                        technical_details=f"Record {i} validation exception: {type(e).__name__}"
                    )
                    validation_issues.append(issue)

            # Audit log the batch validation
            await self._audit_log_batch_validation(batch_id, len(raw_records), len(validation_issues))

            logger.info(f"Batch validation complete: {len(validated_records)} records, {len(validation_issues)} issues")

            return validated_records, validation_issues

        except Exception as e:
            self.error_handler.handle_error(
                e, ErrorCategory.VALIDATION, ErrorSeverity.CRITICAL,
                f"Batch validation failed for batch {batch_id}"
            )
            raise

    def _sanitize_message(self, message: str) -> str:
        """Sanitize message for safe display"""
        if not message:
            return "Unknown validation error"

        # Remove potential sensitive information
        import re
        sanitized = str(message)

        # Remove potential PII patterns
        sanitized = re.sub(r'\b\d{3}-?\d{2}-?\d{4}\b', '[SSN]', sanitized)
        sanitized = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', sanitized)

        # Truncate if too long
        if len(sanitized) > 200:
            sanitized = sanitized[:197] + "..."

        return sanitized

    async def _audit_log_batch_validation(self, batch_id: str, record_count: int, issue_count: int):
        """Log batch validation for audit compliance"""
        try:
            async with self.resource_manager.defensive_database_connection(self.database_url) as conn:
                audit_data = {
                    "batch_id": batch_id or "unknown",
                    "record_count": record_count,
                    "issue_count": issue_count,
                    "timestamp": datetime.utcnow().isoformat()
                }

                # Defensive SQL with parameters
                query = f"""
                INSERT INTO {self._sanitize_table_name(self.audit_table)}
                (operation_type, operation_details, audit_hash, timestamp)
                VALUES ($1, $2, $3, $4)
                """

                import hashlib
                audit_hash = hashlib.sha256(json.dumps(audit_data).encode()).hexdigest()

                await conn.execute(
                    query,
                    "batch_validation",
                    json.dumps(audit_data),
                    audit_hash,
                    datetime.utcnow()
                )

        except Exception as e:
            self.error_handler.handle_error(
                e, ErrorCategory.DATABASE, ErrorSeverity.HIGH,
                "Audit logging failed",
                should_raise=False
            )

    @secure_financial_operation(ErrorCategory.DATABASE)
    async def get_validation_issues(self,
                                  symbol: str = None,
                                  start_date: date = None,
                                  end_date: date = None,
                                  severity: ValidationSeverity = None) -> List[DefensiveValidationIssue]:
        """
        Get validation issues with defensive query construction.

        Args:
            symbol: Filter by symbol (will be sanitized)
            start_date: Filter by start date
            end_date: Filter by end date
            severity: Filter by severity level

        Returns:
            List of validation issues
        """
        try:
            async with self.resource_manager.defensive_database_connection(self.database_url) as conn:
                # Build defensive parameterized query
                where_conditions = []
                params = []
                param_counter = 1

                if symbol:
                    # Validate symbol defensively
                    symbol_validation = self.financial_validator.validate_symbol(symbol)
                    if not symbol_validation.is_valid:
                        raise ValueError(f"Invalid symbol: {symbol}")

                    where_conditions.append(f"instrument_symbol = ${param_counter}")
                    params.append(symbol_validation.input_value)
                    param_counter += 1

                if start_date:
                    where_conditions.append(f"validation_date >= ${param_counter}")
                    params.append(start_date)
                    param_counter += 1

                if end_date:
                    where_conditions.append(f"validation_date <= ${param_counter}")
                    params.append(end_date)
                    param_counter += 1

                if severity:
                    where_conditions.append(f"severity = ${param_counter}")
                    params.append(severity.value)
                    param_counter += 1

                # Construct query with defensive WHERE clause
                where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""

                query = f"""
                SELECT rule_type, severity, instrument_symbol, instrument_id,
                       validation_date, vendor, message, technical_details,
                       detected_at, resolved, resolution_action, audit_hash
                FROM {self._sanitize_table_name(self.issues_table)}
                {where_clause}
                ORDER BY detected_at DESC
                LIMIT 1000
                """

                rows = await conn.fetch(query, *params)

                # Convert rows to defensive validation issues
                issues = []
                for row in rows:
                    try:
                        issue = DefensiveValidationIssue(
                            rule=ValidationRule(row['rule_type']),
                            severity=ValidationSeverity(row['severity']),
                            instrument_symbol=row['instrument_symbol'],
                            instrument_id=row['instrument_id'],
                            date=row['validation_date'],
                            vendor=row['vendor'],
                            message=row['message'],
                            technical_details=row['technical_details'] or '',
                            detected_at=row['detected_at'],
                            resolved=row['resolved'],
                            resolution_action=row['resolution_action'],
                            audit_hash=row['audit_hash']
                        )
                        issues.append(issue)
                    except Exception as e:
                        logger.warning(f"Failed to parse validation issue row: {e}")

                return issues

        except Exception as e:
            self.error_handler.handle_error(
                e, ErrorCategory.DATABASE, ErrorSeverity.HIGH,
                "Failed to retrieve validation issues"
            )
            raise

    def get_health_status(self) -> Dict[str, Any]:
        """Get validator health status"""
        return {
            "validator_type": "defensive_daily_price_polygon",
            "resource_manager_health": self.resource_manager.get_resource_health(),
            "thresholds": self.thresholds,
            "components": {
                "financial_validator": "active",
                "error_handler": "active",
                "resource_manager": "active"
            }
        }

    async def shutdown(self):
        """Shutdown validator and cleanup resources"""
        logger.info("Shutting down defensive daily prices validator")
        self.resource_manager.shutdown()


# Example usage and testing
if __name__ == "__main__":
    import asyncio

    async def test_defensive_validator():
        # Test data with various defensive scenarios
        test_records = [
            # Valid record
            {
                "symbol": "AAPL",
                "date": "2023-12-01",
                "open": 150.00,
                "high": 155.00,
                "low": 149.00,
                "close": 154.50,
                "volume": 1000000,
                "vendor": "test",
                "instrument_id": 1
            },
            # Invalid record with security issue
            {
                "symbol": "'; DROP TABLE prices; --",
                "date": "2023-12-01",
                "open": -100.00,  # Negative price
                "high": 50.00,
                "low": 200.00,   # Low > High (inconsistent)
                "close": 75.00,
                "volume": "invalid_volume",
                "vendor": "malicious",
                "instrument_id": "not_a_number"
            }
        ]

        # Test database URL for testing
        database_url = "postgresql://test:test@localhost:5432/test_db"

        validator = DefensiveDailyPricesValidator(database_url)

        # Test individual record validation
        print("Testing individual record validation:")
        for i, record in enumerate(test_records):
            validated = validator.validate_price_record_defensive(record)
            print(f"Record {i}: Valid={validated.validation_passed}, Errors={validated.validation_errors}")

        # Test batch validation
        print("\nTesting batch validation:")
        try:
            validated_records, issues = await validator.validate_batch_defensive(test_records, "test_batch_1")
            print(f"Batch validation: {len(validated_records)} records, {len(issues)} issues")

            for issue in issues:
                print(f"  Issue: {issue.severity.value} - {issue.message}")

        except Exception as e:
            print(f"Batch validation failed: {e}")

    # Run test
    asyncio.run(test_defensive_validator())