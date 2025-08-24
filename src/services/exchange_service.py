"""
Exchange Service for business logic operations.

Provides high-level business operations for exchange vendor system,
using DAOs for data access and implementing complex business rules.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import date, datetime, timedelta

from dao.exchange_dao import ExchangeDAO
from dao.instrument_xref_dao import InstrumentXrefDAO
from dao.vendor_dao import VendorDAO
from core.logging.logger_config import get_logger
from core.exceptions.custom_exceptions import DatabaseError, DataValidationError


class ExchangeService:
    """
    Business service for exchange vendor operations.
    
    Implements business logic for exchange history, migrations, and analysis
    while maintaining separation from data access concerns.
    """
    
    def __init__(self):
        self.exchange_dao = ExchangeDAO()
        self.instrument_xref_dao = InstrumentXrefDAO()
        self.vendor_dao = VendorDAO()
        self.logger = get_logger(__name__)
    
    def get_exchange_vendor_id(self) -> int:
        """Get the exchange vendor ID, raising error if not found."""
        vendor_id = self.vendor_dao.get_exchange_vendor_id()
        if not vendor_id:
            raise DatabaseError("Exchange vendor not found. Please run system setup first.")
        return vendor_id
    
    def get_current_exchange_for_instrument(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get current exchange for an instrument by symbol.
        
        Args:
            symbol: Instrument symbol
            
        Returns:
            Current exchange information or None if not found
        """
        try:
            # First get instrument ID (this would need an instrument DAO)
            instrument = self._get_instrument_by_symbol(symbol)
            if not instrument:
                return None
            
            exchange_vendor_id = self.get_exchange_vendor_id()
            return self.instrument_xref_dao.get_current_exchange(
                instrument['id'], exchange_vendor_id
            )
            
        except Exception as e:
            self.logger.error(f"Error getting current exchange for {symbol}: {e}")
            raise
    
    def get_exchange_history_for_instrument(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Get complete exchange history for an instrument.
        
        Args:
            symbol: Instrument symbol
            
        Returns:
            List of exchange history entries
        """
        try:
            instrument = self._get_instrument_by_symbol(symbol)
            if not instrument:
                return []
            
            exchange_vendor_id = self.get_exchange_vendor_id()
            return self.instrument_xref_dao.get_exchange_history(
                instrument['id'], exchange_vendor_id
            )
            
        except Exception as e:
            self.logger.error(f"Error getting exchange history for {symbol}: {e}")
            raise
    
    def record_exchange_migration(self, symbol: str, from_exchange: str, 
                                to_exchange: str, migration_date: date) -> bool:
        """
        Record an exchange migration for an instrument.
        
        Args:
            symbol: Instrument symbol
            from_exchange: Source exchange code
            to_exchange: Destination exchange code  
            migration_date: Date of migration
            
        Returns:
            True if migration recorded successfully
        """
        try:
            instrument = self._get_instrument_by_symbol(symbol)
            if not instrument:
                raise DataValidationError(f"Instrument {symbol} not found")
            
            exchange_vendor_id = self.get_exchange_vendor_id()
            
            # Close the current exchange entry
            close_success = self.instrument_xref_dao.close_exchange_entry(
                instrument['id'], exchange_vendor_id, from_exchange, migration_date
            )
            
            if not close_success:
                self.logger.warning(f"No active entry found to close for {symbol} on {from_exchange}")
            
            # Create new exchange entry
            new_entry_id = self.instrument_xref_dao.create_exchange_entry(
                instrument['id'], exchange_vendor_id, to_exchange, migration_date
            )
            
            if new_entry_id:
                self.logger.info(f"Recorded migration: {symbol} {from_exchange} → {to_exchange} on {migration_date}")
                return True
            else:
                raise DatabaseError("Failed to create new exchange entry")
                
        except Exception as e:
            self.logger.error(f"Error recording migration for {symbol}: {e}")
            raise
    
    def analyze_exchange_migrations(self, days_back: int = 365) -> Dict[str, Any]:
        """
        Analyze exchange migrations over a time period.
        
        Args:
            days_back: Number of days to analyze
            
        Returns:
            Migration analysis results
        """
        try:
            exchange_vendor_id = self.get_exchange_vendor_id()
            migrations = self.instrument_xref_dao.find_exchange_migrations(exchange_vendor_id)
            
            # Filter by date range
            cutoff_date = date.today() - timedelta(days=days_back)
            recent_migrations = [
                m for m in migrations 
                if m['transition_date'] >= cutoff_date
            ]
            
            # Analyze patterns
            analysis = {
                'total_migrations': len(recent_migrations),
                'date_range': {
                    'from': cutoff_date,
                    'to': date.today()
                },
                'migration_patterns': {},
                'major_to_otc_count': 0,
                'otc_to_major_count': 0,
                'major_to_major_count': 0
            }
            
            # Count migration patterns
            for migration in recent_migrations:
                pattern = f"{migration['from_exchange']} → {migration['to_exchange']}"
                analysis['migration_patterns'][pattern] = analysis['migration_patterns'].get(pattern, 0) + 1
                
                # Categorize migrations
                if migration['from_major'] and migration['to_otc']:
                    analysis['major_to_otc_count'] += 1
                elif not migration['from_major'] and not migration['to_otc']:
                    analysis['otc_to_major_count'] += 1
                elif migration['from_major'] and not migration['to_otc']:
                    analysis['major_to_major_count'] += 1
            
            # Most common patterns
            analysis['top_patterns'] = sorted(
                analysis['migration_patterns'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing migrations: {e}")
            raise
    
    def get_instruments_on_exchange(self, exchange_code: str, 
                                   as_of_date: Optional[date] = None) -> List[Dict[str, Any]]:
        """
        Get instruments trading on a specific exchange as of a date.
        
        Args:
            exchange_code: Exchange code (e.g., 'NYSE', 'NASDAQ')
            as_of_date: Date to check (defaults to today)
            
        Returns:
            List of instruments on the exchange
        """
        try:
            exchange_vendor_id = self.get_exchange_vendor_id()
            return self.instrument_xref_dao.get_instruments_on_exchange(
                exchange_code, exchange_vendor_id, as_of_date
            )
            
        except Exception as e:
            self.logger.error(f"Error getting instruments on {exchange_code}: {e}")
            raise
    
    def detect_delisting_risk(self, symbol: str, risk_threshold_days: int = 90) -> Dict[str, Any]:
        """
        Detect delisting risk based on exchange migration patterns.
        
        Args:
            symbol: Instrument symbol
            risk_threshold_days: Days to look back for risk patterns
            
        Returns:
            Delisting risk analysis
        """
        try:
            history = self.get_exchange_history_for_instrument(symbol)
            if not history:
                return {'risk_level': 'unknown', 'reason': 'No exchange history found'}
            
            current_exchange = next((h for h in history if h['end_date'] is None), None)
            if not current_exchange:
                return {'risk_level': 'high', 'reason': 'No current exchange found'}
            
            risk_analysis = {
                'symbol': symbol,
                'current_exchange': current_exchange['external_symbol'],
                'risk_level': 'low',
                'risk_factors': [],
                'days_on_current_exchange': (date.today() - current_exchange['start_date']).days,
                'migration_count': len(history),
                'otc_history': False
            }
            
            # Check risk factors
            if current_exchange['external_symbol'] == 'OTC':
                risk_analysis['risk_level'] = 'high'
                risk_analysis['risk_factors'].append('Currently trading OTC')
            
            # Check for recent migrations to OTC
            recent_otc = any(
                h['external_symbol'] == 'OTC' and 
                h['start_date'] >= date.today() - timedelta(days=risk_threshold_days)
                for h in history
            )
            
            if recent_otc:
                risk_analysis['risk_level'] = 'high'
                risk_analysis['risk_factors'].append(f'Recent OTC migration within {risk_threshold_days} days')
            
            # Check for frequent migrations
            if len(history) > 3:
                risk_analysis['risk_level'] = 'medium' if risk_analysis['risk_level'] == 'low' else risk_analysis['risk_level']
                risk_analysis['risk_factors'].append(f'Frequent exchange changes ({len(history)} total)')
            
            # Check for any OTC history
            risk_analysis['otc_history'] = any(h['external_symbol'] == 'OTC' for h in history)
            if risk_analysis['otc_history'] and risk_analysis['risk_level'] == 'low':
                risk_analysis['risk_level'] = 'medium'
                risk_analysis['risk_factors'].append('Previous OTC trading history')
            
            return risk_analysis
            
        except Exception as e:
            self.logger.error(f"Error detecting delisting risk for {symbol}: {e}")
            raise
    
    def _get_instrument_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Helper method to get instrument by symbol.
        This would use an InstrumentDAO in a complete implementation.
        """
        try:
            # For now, query directly - in full implementation this would use InstrumentDAO
            query = "SELECT * FROM dev_instruments WHERE symbol = %(symbol)s AND is_active = true"
            results = self.exchange_dao.execute_query(query, {"symbol": symbol})
            return results[0] if results else None
            
        except Exception as e:
            self.logger.error(f"Error getting instrument {symbol}: {e}")
            raise
    
    def validate_exchange_system(self) -> Dict[str, Any]:
        """
        Validate the exchange vendor system is properly configured.
        
        Returns:
            Validation results with system status
        """
        try:
            validation = {
                'system_status': 'healthy',
                'checks': {},
                'errors': [],
                'warnings': []
            }
            
            # Check exchange vendor exists
            try:
                exchange_vendor_id = self.get_exchange_vendor_id()
                validation['checks']['exchange_vendor'] = {
                    'status': 'pass',
                    'vendor_id': exchange_vendor_id
                }
            except Exception as e:
                validation['checks']['exchange_vendor'] = {
                    'status': 'fail',
                    'error': str(e)
                }
                validation['errors'].append('Exchange vendor not found')
            
            # Check exchanges table
            try:
                exchanges = self.exchange_dao.list_active_exchanges()
                validation['checks']['exchanges'] = {
                    'status': 'pass',
                    'count': len(exchanges)
                }
                
                if len(exchanges) < 3:  # Should have at least NYSE, NASDAQ, OTC
                    validation['warnings'].append(f'Only {len(exchanges)} exchanges configured')
                    
            except Exception as e:
                validation['checks']['exchanges'] = {
                    'status': 'fail',
                    'error': str(e)
                }
                validation['errors'].append('Cannot access exchanges table')
            
            # Check instrument_xrefs table
            try:
                xref_count = self.instrument_xref_dao.count()
                validation['checks']['instrument_xrefs'] = {
                    'status': 'pass',
                    'count': xref_count
                }
                
                if xref_count == 0:
                    validation['warnings'].append('No instrument exchange mappings found')
                    
            except Exception as e:
                validation['checks']['instrument_xrefs'] = {
                    'status': 'fail',
                    'error': str(e)
                }
                validation['errors'].append('Cannot access instrument_xrefs table')
            
            # Set overall status
            if validation['errors']:
                validation['system_status'] = 'error'
            elif validation['warnings']:
                validation['system_status'] = 'warning'
            
            return validation
            
        except Exception as e:
            self.logger.error(f"Error validating exchange system: {e}")
            raise