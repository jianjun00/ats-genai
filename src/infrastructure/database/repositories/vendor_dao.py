"""
Base vendor DAO class for vendor-specific operations.

This module provides the foundation for vendor-specific DAOs, standardizing
the interface for different data providers while eliminating code duplication.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
from enum import Enum

from infrastructure.database.repositories.base.base_dao import BaseDAO
from core.logging.logger_config import get_logger
from shared.exceptions.custom_exceptions import DataValidationError


class VendorType(str, Enum):
    """Supported data vendors."""
    POLYGON = "polygon"
    TIINGO = "tiingo"
    ALPHA_VANTAGE = "alpha_vantage"
    FINNHUB = "finnhub"
    FMP = "fmp"
    IEX = "iex"


class BaseVendorDAO(BaseDAO, ABC):
    """
    Base class for vendor-specific DAOs.
    
    Provides standardized interface for vendor data operations while
    maintaining vendor-specific customizations.
    """
    
    def __init__(self, table_name: str, vendor_type: VendorType):
        """
        Initialize vendor DAO.
        
        Args:
            table_name: Base table name
            vendor_type: Type of vendor
        """
        # Create vendor-specific table name
        vendor_table_name = f"{table_name}_{vendor_type.value}"
        super().__init__(vendor_table_name)
        
        self.vendor_type = vendor_type
        self.logger = get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")
    
    @abstractmethod
    def get_vendor_config(self) -> Dict[str, Any]:
        """Get vendor-specific configuration."""
    
    def validate_vendor_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate vendor-specific data.
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid
        """
        # Base validation - can be overridden by subclasses
        required_fields = self.get_required_fields()
        
        for field in required_fields:
            if field not in data or data[field] is None:
                self.logger.error(f"Missing required field: {field}")
                return False
        
        return True
    
    @abstractmethod
    def get_required_fields(self) -> List[str]:
        """Get list of required fields for this vendor."""
    
    def transform_vendor_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform vendor-specific data to standardized format.
        
        Args:
            raw_data: Raw vendor data
            
        Returns:
            Standardized data
        """
        # Default implementation - can be overridden
        return raw_data
    
    def add_vendor_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add vendor-specific metadata to data.
        
        Args:
            data: Record data
            
        Returns:
            Data with vendor metadata
        """
        enhanced_data = data.copy()
        enhanced_data.update({
            "vendor": self.vendor_type.value,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        })
        return enhanced_data
    
    def create_with_vendor_metadata(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Create record with vendor metadata.
        
        Args:
            data: Record data
            
        Returns:
            Created record ID
        """
        # Validate vendor-specific data
        if not self.validate_vendor_data(data):
            raise DataValidationError(f"Vendor data validation failed for {self.vendor_type.value}")
        
        # Transform data if needed
        transformed_data = self.transform_vendor_data(data)
        
        # Add vendor metadata
        enhanced_data = self.add_vendor_metadata(transformed_data)
        
        return self.create(enhanced_data)
    
    def bulk_insert_with_vendor_metadata(self, records: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """
        Bulk insert with vendor metadata.
        
        Args:
            records: List of records
            batch_size: Batch size for insertion
            
        Returns:
            Number of records inserted
        """
        # Process all records
        processed_records = []
        for record in records:
            if not self.validate_vendor_data(record):
                self.logger.warning(f"Skipping invalid record: {record}")
                continue
            
            transformed = self.transform_vendor_data(record)
            enhanced = self.add_vendor_metadata(transformed)
            processed_records.append(enhanced)
        
        return self.bulk_insert(processed_records, batch_size)
    
    def get_vendor_info(self) -> Dict[str, Any]:
        """Get vendor information."""
        return {
            "vendor_type": self.vendor_type.value,
            "table_name": self.table_name,
            "base_table_name": self.base_table_name,
            "config": self.get_vendor_config(),
            "required_fields": self.get_required_fields()
        }


class MarketDataVendorDAO(BaseVendorDAO, ABC):
    """
    Base class for market data vendor DAOs.
    
    Provides common functionality for market data vendors like Polygon, Tiingo, etc.
    """
    
    def __init__(self, table_name: str, vendor_type: VendorType):
        super().__init__(table_name, vendor_type)
    
    def validate_price_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate price data common to all market data vendors.
        
        Args:
            data: Price data to validate
            
        Returns:
            True if valid
        """
        # Common price data validation
        required_price_fields = ["symbol", "date", "open", "high", "low", "close", "volume"]
        
        for field in required_price_fields:
            if field not in data:
                self.logger.error(f"Missing required price field: {field}")
                return False
        
        # Validate OHLC consistency
        try:
            open_price = float(data["open"])
            high_price = float(data["high"])
            low_price = float(data["low"])
            close_price = float(data["close"])
            volume = int(data["volume"])
            
            # High should be >= Open, Low, Close
            if high_price < max(open_price, low_price, close_price):
                self.logger.error("High price is less than open, low, or close")
                return False
            
            # Low should be <= Open, High, Close
            if low_price > min(open_price, high_price, close_price):
                self.logger.error("Low price is greater than open, high, or close")
                return False
            
            # Volume should be non-negative
            if volume < 0:
                self.logger.error("Volume cannot be negative")
                return False
            
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid price data types: {e}")
            return False
        
        return True
    
    def standardize_symbol(self, symbol: str) -> str:
        """
        Standardize symbol format across vendors.
        
        Args:
            symbol: Raw symbol
            
        Returns:
            Standardized symbol
        """
        if not symbol:
            return ""
        
        # Remove whitespace and convert to uppercase
        standardized = symbol.strip().upper()
        
        # Vendor-specific symbol transformations can be added here
        return standardized
    
    def transform_price_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform price data to standardized format.
        
        Args:
            raw_data: Raw price data
            
        Returns:
            Standardized price data
        """
        transformed = raw_data.copy()
        
        # Standardize symbol
        if "symbol" in transformed:
            transformed["symbol"] = self.standardize_symbol(transformed["symbol"])
        
        # Ensure date is proper format
        if "date" in transformed and isinstance(transformed["date"], str):
            try:
                transformed["date"] = datetime.strptime(transformed["date"], "%Y-%m-%d").date()
            except ValueError:
                # Try other common formats
                try:
                    transformed["date"] = datetime.fromisoformat(transformed["date"]).date()
                except ValueError:
                    pass  # Keep original format
        
        return transformed
    
    def get_price_history(
        self, 
        symbol: str, 
        start_date: Union[date, datetime], 
        end_date: Union[date, datetime]
    ) -> List[Dict[str, Any]]:
        """
        Get price history for a symbol.
        
        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            
        Returns:
            List of price records
        """
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = %(symbol)s
            AND date >= %(start_date)s
            AND date <= %(end_date)s
            ORDER BY date
        """
        
        params = {
            "symbol": self.standardize_symbol(symbol),
            "start_date": start_date,
            "end_date": end_date
        }
        
        return self.execute_query(query, params)
    
    def get_latest_price(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get latest price for a symbol.
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Latest price record or None
        """
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = %(symbol)s
            ORDER BY date DESC
            LIMIT 1
        """
        
        params = {"symbol": self.standardize_symbol(symbol)}
        results = self.execute_query(query, params)
        
        return results[0] if results else None