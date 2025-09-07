"""
OHLCV to InstrumentInterval Adapter for Indicator System Integration.

This adapter converts pandas DataFrame OHLCV data to InstrumentInterval objects
so that the existing indicator system can process multi-timeframe market data.
"""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from state.instrument_interval import InstrumentInterval


class OHLCVToIntervalAdapter:
    """
    Adapter to convert OHLCV pandas DataFrames to InstrumentInterval objects
    for use with the existing indicator system.
    """
    
    def __init__(self, instrument_id: int = 1):
        self.instrument_id = instrument_id
    
    def convert_dataframe_to_intervals(self, df: pd.DataFrame, symbol: str = "UNKNOWN") -> List[InstrumentInterval]:
        """
        Convert OHLCV DataFrame to list of InstrumentInterval objects.
        
        Args:
            df: DataFrame with OHLCV data (columns: open, high, low, close, volume)
            symbol: Symbol name for the intervals
            
        Returns:
            List of InstrumentInterval objects
        """
        intervals = []
        
        for idx, row in df.iterrows():
            # Get timestamp
            if hasattr(idx, 'to_pydatetime'):
                timestamp = idx.to_pydatetime()
            elif isinstance(idx, datetime):
                timestamp = idx
            else:
                # Fallback to current time if no timestamp
                timestamp = datetime.now()
            
            # Create InstrumentInterval
            interval = InstrumentInterval(
                instrument_id=self.instrument_id,
                start_date_time=timestamp,
                end_date_time=timestamp,  # For simplicity, same as start
                symbol=symbol,
                open=float(row.get('open', 0.0)),
                high=float(row.get('high', 0.0)),
                low=float(row.get('low', 0.0)),
                close=float(row.get('close', 0.0)),
                volume=int(row.get('volume', 0)),
                traded_volume=int(row.get('volume', 0)),
                status='ok'
            )
            
            intervals.append(interval)
        
        return intervals
    
    def convert_multi_timeframe_data(
        self, 
        multi_timeframe_data: Dict[str, pd.DataFrame], 
        symbol: str = "UNKNOWN"
    ) -> Dict[str, List[InstrumentInterval]]:
        """
        Convert multi-timeframe OHLCV data to InstrumentInterval objects.
        
        Args:
            multi_timeframe_data: Dict mapping timeframes to OHLCV DataFrames
            symbol: Symbol name for the intervals
            
        Returns:
            Dict mapping timeframes to lists of InstrumentInterval objects
        """
        result = {}
        
        for timeframe, df in multi_timeframe_data.items():
            if df is not None and not df.empty:
                result[timeframe] = self.convert_dataframe_to_intervals(df, symbol)
            else:
                result[timeframe] = []
        
        return result
    
    def compute_indicators_for_timeframe(
        self, 
        df: pd.DataFrame, 
        indicator_config, 
        symbol: str = "UNKNOWN"
    ) -> Dict[str, Any]:
        """
        Compute indicators for a single timeframe using the existing IndicatorBuilder.
        
        Args:
            df: OHLCV DataFrame for the timeframe
            indicator_config: IndicatorConfig with indicators to compute
            symbol: Symbol name
            
        Returns:
            Dict of indicator names to values
        """
        from signals.indicator_builder import IndicatorBuilder
        
        # Convert DataFrame to InstrumentIntervals
        intervals = self.convert_dataframe_to_intervals(df, symbol)
        
        if not intervals:
            return {}
        
        # Create rolling cache format expected by IndicatorBuilder
        # Use instrument_id as key, intervals as rolling window
        rolling_cache = {self.instrument_id: intervals}
        
        # Get start and end times from the data
        start_time = intervals[0].start_date_time if intervals else datetime.now()
        end_time = intervals[-1].end_date_time if intervals else datetime.now()
        
        try:
            # Use the existing IndicatorBuilder class
            indicator_builder = IndicatorBuilder(indicator_config)
            indicator_intervals = indicator_builder.build_indicator_intervals(
                rolling_cache, start_time, end_time
            )
            
            # Extract indicator values from IndicatorInterval
            results = {}
            if self.instrument_id in indicator_intervals:
                indicator_interval = indicator_intervals[self.instrument_id]
                
                # Get all indicators from the IndicatorInterval
                for indicator_name, indicator_data in indicator_interval.indicators.items():
                    results[indicator_name] = {
                        'value': indicator_data.get('value'),
                        'status': indicator_data.get('status', 'ok'),
                        'update_at': indicator_data.get('update_at')
                    }
            
            return results
            
        except Exception as e:
            # Fallback to empty results on error
            return {}
    
    def compute_indicators_multi_timeframe(
        self,
        multi_timeframe_data: Dict[str, pd.DataFrame],
        indicator_config,
        symbol: str = "UNKNOWN"
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compute indicators for multiple timeframes.
        
        Args:
            multi_timeframe_data: Dict mapping timeframes to OHLCV DataFrames
            indicator_config: IndicatorConfig with indicators to compute
            symbol: Symbol name
            
        Returns:
            Dict mapping timeframes to indicator results
        """
        results = {}
        
        for timeframe, df in multi_timeframe_data.items():
            if df is not None and not df.empty:
                results[timeframe] = self.compute_indicators_for_timeframe(df, indicator_config, symbol)
            else:
                results[timeframe] = {}
        
        return results
    
    def extract_indicator_values(
        self, 
        indicator_results: Dict[str, Dict[str, Any]], 
        flatten: bool = True
    ) -> Dict[str, float]:
        """
        Extract indicator values from results structure.
        
        Args:
            indicator_results: Results from compute_indicators_multi_timeframe
            flatten: If True, flatten timeframe_indicator_name structure
            
        Returns:
            Dict of indicator names/values suitable for feature matrices
        """
        if not flatten:
            return indicator_results
        
        flattened = {}
        
        for timeframe, indicators in indicator_results.items():
            for indicator_name, result in indicators.items():
                if isinstance(result, dict) and 'value' in result:
                    value = result['value']
                    status = result.get('status', 'unknown')
                    
                    # Only include valid indicators
                    if status == 'ok' and value is not None:
                        flattened[f'{timeframe}_{indicator_name}'] = float(value)
                else:
                    # Handle simple value
                    if result is not None:
                        flattened[f'{timeframe}_{indicator_name}'] = float(result)
        
        return flattened


def create_multi_timeframe_features(
    multi_timeframe_data: Dict[str, pd.DataFrame],
    indicator_config,
    symbol: str = "UNKNOWN",
    instrument_id: int = 1
) -> pd.DataFrame:
    """
    Convenience function to create multi-timeframe features DataFrame.
    
    Args:
        multi_timeframe_data: Dict mapping timeframes to OHLCV DataFrames
        indicator_config: IndicatorConfig with indicators to compute
        symbol: Symbol name
        instrument_id: Instrument ID for intervals
        
    Returns:
        DataFrame with multi-timeframe features
    """
    adapter = OHLCVToIntervalAdapter(instrument_id)
    
    # Compute indicators for all timeframes
    indicator_results = adapter.compute_indicators_multi_timeframe(
        multi_timeframe_data, indicator_config, symbol
    )
    
    # Extract flattened values
    features = adapter.extract_indicator_values(indicator_results, flatten=True)
    
    # Create DataFrame with one row
    if features:
        return pd.DataFrame([features])
    else:
        return pd.DataFrame()


def validate_ohlcv_dataframe(df: pd.DataFrame) -> bool:
    """
    Validate that DataFrame has required OHLCV columns and data quality.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_columns = ['open', 'high', 'low', 'close', 'volume']
    
    # Check required columns exist
    if not all(col in df.columns for col in required_columns):
        return False
    
    # Check for empty DataFrame
    if df.empty:
        return False
    
    # Check OHLC relationships
    try:
        if not (df['high'] >= df['low']).all():
            return False
        if not (df['high'] >= df['open']).all():
            return False
        if not (df['high'] >= df['close']).all():
            return False
        if not (df['low'] <= df['open']).all():
            return False
        if not (df['low'] <= df['close']).all():
            return False
        if not (df['volume'] >= 0).all():
            return False
    except Exception:
        return False
    
    return True