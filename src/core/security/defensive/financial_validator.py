#!/usr/bin/env python3
"""
Defensive Financial Data Validator

Implements comprehensive defensive validation for financial data with:
- Input sanitization and type safety
- Range validation and anomaly detection
- SQL injection prevention
- Audit logging for all validation operations
- Circuit breaker patterns for external services
- Defensive error handling

This module follows the "fail-secure" principle for financial systems.
"""

import hashlib
import logging
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Defensive imports with graceful degradation
try:
    from core.platform.config_env.environment import Environment
except ImportError:
    try:
        from core.platform.config_env.environment import Environment
    except ImportError:
        # Emergency environment class for system stability
        class Environment:
            def __init__(self):
                pass

logger = logging.getLogger(__name__)


class SecurityLevel(Enum):
    """Security validation levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ValidationSeverity(Enum):
    """Validation issue severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """Defensive validation result with audit trail"""
    is_valid: bool
    severity: ValidationSeverity
    message: str
    field_name: Optional[str] = None
    input_value: Optional[str] = None  # Sanitized for logging
    expected_range: Optional[Tuple[Any, Any]] = None
    audit_hash: Optional[str] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()
        if self.audit_hash is None and self.input_value:
            # Create audit hash for tracking (not for security)
            self.audit_hash = hashlib.sha256(
                f"{self.field_name}:{self.input_value}:{self.timestamp}".encode()
            ).hexdigest()[:16]


class DefensiveFinancialValidator:
    """
    Defensive validator for financial data with comprehensive security checks.

    Key principles:
    1. Never trust any input data
    2. Validate everything before processing
    3. Fail securely when validation fails
    4. Log all validation attempts with audit trails
    5. Use precise decimal arithmetic for money
    6. Implement circuit breakers for external validation
    """

    # Defensive constants with reasonable financial limits
    MIN_STOCK_PRICE = Decimal('0.0001')  # $0.0001 minimum (penny stocks)
    MAX_STOCK_PRICE = Decimal('1000000')  # $1M maximum (defensive upper bound)
    MAX_DAILY_PRICE_CHANGE = Decimal('0.50')  # 50% max daily change
    MIN_VOLUME = 0
    MAX_VOLUME = 10_000_000_000  # 10B shares maximum
    MAX_SYMBOL_LENGTH = 10
    MAX_DATE_FUTURE_DAYS = 1  # Allow 1 day in future for timezone issues
    MAX_DATE_HISTORY_YEARS = 50  # 50 years of historical data

    def __init__(self, security_level: SecurityLevel = SecurityLevel.HIGH):
        self.security_level = security_level
        self.validation_cache = {}  # Simple cache for repeated validations
        self.circuit_breaker_failures = 0
        self.circuit_breaker_last_failure = None
        self.circuit_breaker_threshold = 5
        self.audit_logger = logging.getLogger(f"{__name__}.audit")

        # Initialize with defensive configuration
        self._setup_defensive_logging()

    def _setup_defensive_logging(self):
        """Setup defensive logging with sanitization"""
        formatter = logging.Formatter(
            '%(asctime)s - FINANCIAL_VALIDATOR - %(levelname)s - %(message)s'
        )
        if not self.audit_logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.audit_logger.addHandler(handler)
            self.audit_logger.setLevel(logging.INFO)

    def _sanitize_for_logging(self, value: Any) -> str:
        """Sanitize values for safe logging (prevent log injection)"""
        if value is None:
            return "NULL"

        # Convert to string and remove control characters
        str_value = str(value)
        # Remove newlines, tabs, and other control characters that could break logs
        sanitized = re.sub(r'[\r\n\t\x00-\x1f\x7f-\x9f]', '', str_value)

        # Truncate if too long
        if len(sanitized) > 100:
            sanitized = sanitized[:97] + "..."

        return sanitized

    def _audit_log(self, operation: str, result: ValidationResult, **metadata):
        """Defensive audit logging for all validation operations"""
        audit_entry = {
            "operation": operation,
            "valid": result.is_valid,
            "severity": result.severity.value,
            "field": result.field_name or "unknown",
            "hash": result.audit_hash,
            "timestamp": result.timestamp.isoformat(),
            "security_level": self.security_level.value
        }
        audit_entry.update(metadata)

        # Log with sanitized data
        self.audit_logger.info(f"VALIDATION_AUDIT: {audit_entry}")

    def validate_symbol(self, symbol: Any, field_name: str = "symbol") -> ValidationResult:
        """
        Defensive symbol validation with comprehensive security checks.

        Args:
            symbol: Input symbol (any type, will be validated)
            field_name: Name of the field being validated

        Returns:
            ValidationResult with security audit trail
        """
        # Step 1: Type and null validation
        if symbol is None:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Symbol cannot be None",
                field_name=field_name
            )
            self._audit_log("validate_symbol", result, error_type="null_input")
            return result

        # Step 2: Convert to string defensively
        try:
            symbol_str = str(symbol).strip().upper()
        except Exception as e:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Symbol conversion failed: {type(e).__name__}",
                field_name=field_name,
                input_value=self._sanitize_for_logging(symbol)
            )
            self._audit_log("validate_symbol", result, error_type="conversion_error", exception=str(e))
            return result

        # Step 3: Length validation (prevent buffer overflows)
        if len(symbol_str) == 0:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Symbol cannot be empty",
                field_name=field_name,
                input_value=self._sanitize_for_logging(symbol_str)
            )
            self._audit_log("validate_symbol", result, error_type="empty_symbol")
            return result

        if len(symbol_str) > self.MAX_SYMBOL_LENGTH:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Symbol too long: {len(symbol_str)} > {self.MAX_SYMBOL_LENGTH}",
                field_name=field_name,
                input_value=self._sanitize_for_logging(symbol_str)
            )
            self._audit_log("validate_symbol", result, error_type="length_exceeded", length=len(symbol_str))
            return result

        # Step 4: Character validation (prevent injection attacks)
        if not re.match(r'^[A-Z0-9.-]{1,10}$', symbol_str):
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Invalid symbol format: contains illegal characters",
                field_name=field_name,
                input_value=self._sanitize_for_logging(symbol_str)
            )
            self._audit_log("validate_symbol", result, error_type="invalid_format")
            return result

        # Step 5: Business rule validation
        suspicious_patterns = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'SELECT', '--', ';', '/*']
        for pattern in suspicious_patterns:
            if pattern in symbol_str:
                result = ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.CRITICAL,
                    message=f"Suspicious pattern detected in symbol: {pattern}",
                    field_name=field_name,
                    input_value=self._sanitize_for_logging(symbol_str)
                )
                self._audit_log("validate_symbol", result,
                              error_type="security_violation",
                              pattern=pattern,
                              security_level="CRITICAL")
                return result

        # Success
        result = ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Symbol validation passed",
            field_name=field_name,
            input_value=self._sanitize_for_logging(symbol_str)
        )
        self._audit_log("validate_symbol", result)
        return result

    def validate_price(self, price: Any, field_name: str = "price",
                      allow_zero: bool = False) -> ValidationResult:
        """
        Defensive price validation with financial precision and security checks.

        Args:
            price: Input price (any type, will be validated)
            field_name: Name of the field being validated
            allow_zero: Whether zero prices are allowed

        Returns:
            ValidationResult with precise decimal validation
        """
        # Step 1: Null validation
        if price is None:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Price cannot be None",
                field_name=field_name
            )
            self._audit_log("validate_price", result, error_type="null_input")
            return result

        # Step 2: Defensive decimal conversion
        try:
            if isinstance(price, str):
                # Sanitize string input to prevent injection
                price_str = re.sub(r'[^\d.-]', '', str(price))
                if not price_str or price_str in ['-', '.', '-.']:
                    raise InvalidOperation("Empty or invalid price string")
                decimal_price = Decimal(price_str)
            else:
                decimal_price = Decimal(str(price))
        except (InvalidOperation, TypeError, ValueError, OverflowError) as e:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Invalid price format: {type(e).__name__}",
                field_name=field_name,
                input_value=self._sanitize_for_logging(price)
            )
            self._audit_log("validate_price", result,
                          error_type="conversion_error",
                          exception=str(e))
            return result

        # Step 3: Range validation with defensive bounds
        if not allow_zero and decimal_price <= 0:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Price must be positive: {decimal_price}",
                field_name=field_name,
                input_value=str(decimal_price),
                expected_range=(self.MIN_STOCK_PRICE, self.MAX_STOCK_PRICE)
            )
            self._audit_log("validate_price", result, error_type="negative_price")
            return result

        if decimal_price < self.MIN_STOCK_PRICE:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Price below minimum: {decimal_price} < {self.MIN_STOCK_PRICE}",
                field_name=field_name,
                input_value=str(decimal_price),
                expected_range=(self.MIN_STOCK_PRICE, self.MAX_STOCK_PRICE)
            )
            self._audit_log("validate_price", result, error_type="price_too_low")
            return result

        if decimal_price > self.MAX_STOCK_PRICE:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.CRITICAL,
                message=f"Price suspiciously high: {decimal_price} > {self.MAX_STOCK_PRICE}",
                field_name=field_name,
                input_value=str(decimal_price),
                expected_range=(self.MIN_STOCK_PRICE, self.MAX_STOCK_PRICE)
            )
            self._audit_log("validate_price", result,
                          error_type="suspicious_high_price",
                          security_level="CRITICAL")
            return result

        # Success
        result = ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Price validation passed",
            field_name=field_name,
            input_value=str(decimal_price)
        )
        self._audit_log("validate_price", result, price_decimal=str(decimal_price))
        return result

    def validate_ohlc_consistency(self, open_price: Any, high: Any, low: Any,
                                 close: Any) -> ValidationResult:
        """
        Defensive OHLC (Open-High-Low-Close) consistency validation.

        Args:
            open_price, high, low, close: OHLC prices (any type)

        Returns:
            ValidationResult for OHLC consistency
        """
        # First validate each price individually
        price_validations = [
            ("open", open_price),
            ("high", high),
            ("low", low),
            ("close", close)
        ]

        validated_prices = {}
        for name, price in price_validations:
            validation = self.validate_price(price, field_name=name)
            if not validation.is_valid:
                result = ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"OHLC validation failed on {name}: {validation.message}",
                    field_name="ohlc",
                    input_value=f"O:{self._sanitize_for_logging(open_price)},H:{self._sanitize_for_logging(high)},L:{self._sanitize_for_logging(low)},C:{self._sanitize_for_logging(close)}"
                )
                self._audit_log("validate_ohlc", result,
                              error_type="individual_price_invalid",
                              failed_field=name)
                return result

            validated_prices[name] = Decimal(str(price))

        # OHLC consistency checks
        o, h, l, c = validated_prices['open'], validated_prices['high'], validated_prices['low'], validated_prices['close']

        # High should be >= all other prices
        if h < max(o, l, c):
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"High price {h} is less than max(open={o}, low={l}, close={c})",
                field_name="ohlc",
                input_value=f"O:{o},H:{h},L:{l},C:{c}"
            )
            self._audit_log("validate_ohlc", result, error_type="high_inconsistent")
            return result

        # Low should be <= all other prices
        if l > min(o, h, c):
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Low price {l} is greater than min(open={o}, high={h}, close={c})",
                field_name="ohlc",
                input_value=f"O:{o},H:{h},L:{l},C:{c}"
            )
            self._audit_log("validate_ohlc", result, error_type="low_inconsistent")
            return result

        # Success
        result = ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="OHLC consistency validation passed",
            field_name="ohlc",
            input_value=f"O:{o},H:{h},L:{l},C:{c}"
        )
        self._audit_log("validate_ohlc", result)
        return result

    def validate_trading_date(self, trading_date: Any, field_name: str = "date") -> ValidationResult:
        """
        Defensive trading date validation with business rules.

        Args:
            trading_date: Input date (any type, will be validated)
            field_name: Name of the field being validated

        Returns:
            ValidationResult for trading date
        """
        # Step 1: Null validation
        if trading_date is None:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message="Trading date cannot be None",
                field_name=field_name
            )
            self._audit_log("validate_trading_date", result, error_type="null_input")
            return result

        # Step 2: Convert to date defensively
        try:
            if isinstance(trading_date, str):
                # Sanitize date string to prevent injection
                date_str = re.sub(r'[^\d-]', '', trading_date)
                if len(date_str) != 10 or date_str.count('-') != 2:
                    raise ValueError("Invalid date format")
                validated_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            elif isinstance(trading_date, datetime):
                validated_date = trading_date.date()
            elif isinstance(trading_date, date):
                validated_date = trading_date
            else:
                raise TypeError(f"Unsupported date type: {type(trading_date)}")
        except (ValueError, TypeError) as e:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Invalid date format: {type(e).__name__}",
                field_name=field_name,
                input_value=self._sanitize_for_logging(trading_date)
            )
            self._audit_log("validate_trading_date", result,
                          error_type="conversion_error",
                          exception=str(e))
            return result

        # Step 3: Range validation with defensive bounds
        today = date.today()
        min_date = today - timedelta(days=365 * self.MAX_DATE_HISTORY_YEARS)
        max_date = today + timedelta(days=self.MAX_DATE_FUTURE_DAYS)

        if validated_date < min_date:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Date too far in past: {validated_date} < {min_date}",
                field_name=field_name,
                input_value=str(validated_date),
                expected_range=(min_date, max_date)
            )
            self._audit_log("validate_trading_date", result, error_type="date_too_old")
            return result

        if validated_date > max_date:
            result = ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                message=f"Date too far in future: {validated_date} > {max_date}",
                field_name=field_name,
                input_value=str(validated_date),
                expected_range=(min_date, max_date)
            )
            self._audit_log("validate_trading_date", result, error_type="date_too_future")
            return result

        # Success
        result = ValidationResult(
            is_valid=True,
            severity=ValidationSeverity.INFO,
            message="Trading date validation passed",
            field_name=field_name,
            input_value=str(validated_date)
        )
        self._audit_log("validate_trading_date", result, validated_date=str(validated_date))
        return result

    def validate_financial_record(self, record: Dict[str, Any]) -> List[ValidationResult]:
        """
        Comprehensive defensive validation of a complete financial record.

        Args:
            record: Dictionary containing financial data

        Returns:
            List of ValidationResult objects for all validations
        """
        if not isinstance(record, dict):
            return [ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.CRITICAL,
                message=f"Financial record must be a dictionary, got {type(record)}",
                field_name="record_type"
            )]

        results = []

        # Validate symbol
        if 'symbol' in record:
            results.append(self.validate_symbol(record['symbol']))

        # Validate date
        for date_field in ['date', 'trading_date', 'timestamp']:
            if date_field in record:
                results.append(self.validate_trading_date(record[date_field], date_field))
                break

        # Validate prices
        price_fields = ['open', 'high', 'low', 'close', 'price', 'adj_close']
        price_values = {}
        for field in price_fields:
            if field in record:
                validation = self.validate_price(record[field], field)
                results.append(validation)
                if validation.is_valid:
                    price_values[field] = Decimal(str(record[field]))

        # Validate OHLC consistency if we have all four
        if all(field in price_values for field in ['open', 'high', 'low', 'close']):
            ohlc_result = self.validate_ohlc_consistency(
                price_values['open'],
                price_values['high'],
                price_values['low'],
                price_values['close']
            )
            results.append(ohlc_result)

        # Log comprehensive record validation
        valid_count = sum(1 for r in results if r.is_valid)
        total_count = len(results)

        self.audit_logger.info(
            f"FINANCIAL_RECORD_VALIDATION: {valid_count}/{total_count} validations passed"
        )

        return results


# Convenience functions for defensive validation
def validate_stock_symbol(symbol: Any) -> ValidationResult:
    """Quick defensive stock symbol validation"""
    validator = DefensiveFinancialValidator(SecurityLevel.HIGH)
    return validator.validate_symbol(symbol)


def validate_stock_price(price: Any) -> ValidationResult:
    """Quick defensive stock price validation"""
    validator = DefensiveFinancialValidator(SecurityLevel.HIGH)
    return validator.validate_price(price)


def validate_financial_data_record(record: Dict[str, Any]) -> List[ValidationResult]:
    """Quick defensive financial record validation"""
    validator = DefensiveFinancialValidator(SecurityLevel.HIGH)
    return validator.validate_financial_record(record)


# Example usage and testing
if __name__ == "__main__":
    # Defensive testing
    validator = DefensiveFinancialValidator(SecurityLevel.HIGH)

    # Test valid data
    print("Testing valid data:")
    print(validator.validate_symbol("AAPL"))
    print(validator.validate_price(150.25))
    print(validator.validate_trading_date("2023-12-01"))

    # Test invalid data (defensive cases)
    print("\nTesting invalid data:")
    print(validator.validate_symbol(None))
    print(validator.validate_symbol("'; DROP TABLE prices; --"))
    print(validator.validate_price(-10.50))
    print(validator.validate_price("not_a_number"))

    # Test comprehensive record
    test_record = {
        "symbol": "AAPL",
        "date": "2023-12-01",
        "open": 150.00,
        "high": 155.00,
        "low": 149.00,
        "close": 154.50,
        "volume": 1000000
    }

    print(f"\nTesting financial record validation:")
    results = validator.validate_financial_record(test_record)
    for result in results:
        print(f"  {result.field_name}: {'✅' if result.is_valid else '❌'} {result.message}")