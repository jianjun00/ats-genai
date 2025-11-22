"""
Feature Service for locating and reading ArrayRecord files.

Given symbol, datetime, and feature lists, finds the latest feature extraction runs
and locates the right ArrayRecord files to extract OHLC and sequence data.
"""

import asyncio
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
import json

try:
    from array_record.python import array_record_module
    ARRAYRECORD_AVAILABLE = True
except ImportError:
    ARRAYRECORD_AVAILABLE = False
    print("Warning: array_record not available, training will fail")


@dataclass
class FeatureRequest:
    """Request for feature data."""
    symbol: str
    target_datetime: datetime
    feature_groups: List[str]
    timeframe: str = "60m"  # Start with 1h


@dataclass
class FeatureDataResult:
    """Result containing OHLC and sequence data."""
    symbol: str
    datetime: datetime
    timeframe: str
    ohlc: Dict[str, float]  # {open, high, low, close, volume}
    features: Dict[str, Any]  # Feature group data
    sequence_data: List[Dict[str, Any]]  # Time series sequence
    file_paths: List[str]  # Source ArrayRecord files


class FeatureService:
    """Service for locating and reading feature data from ArrayRecord files."""
    
    def __init__(self, base_path: str = "/mnt/d/ats-data/training_data"):
        # Try container path first, then host path
        container_path = Path("/data/training_data")
        host_path = Path(base_path)
        
        if container_path.exists():
            self.base_path = container_path
        elif host_path.exists():
            self.base_path = host_path
        else:
            self.base_path = Path(base_path)  # Default to provided path
        
    async def get_feature_data(self, request: FeatureRequest) -> FeatureDataResult:
        """
        Main entry point: get feature data for symbol at datetime.
        
        Args:
            request: FeatureRequest with symbol, datetime, features
            
        Returns:
            FeatureDataResult with OHLC and sequence data
        """
        # Step 1: Find the best dataset for this symbol/datetime
        dataset_id = await self._find_best_dataset(request.symbol, request.target_datetime)
        
        if not dataset_id:
            raise ValueError(f"No dataset found for {request.symbol} at {request.target_datetime}")
            
        # Step 2: Locate ArrayRecord files for requested features
        file_paths = await self._locate_arrayrecord_files(dataset_id, request)
        
        # Step 3: Read ArrayRecord files and extract data
        feature_data = await self._read_arrayrecord_files(file_paths, request)
        
        # Step 4: Extract OHLC and sequence data
        ohlc_data = self._extract_ohlc_data(feature_data, request)
        sequence_data = self._build_sequence_data(feature_data, request)
        
        return FeatureDataResult(
            symbol=request.symbol,
            datetime=request.target_datetime,
            timeframe=request.timeframe,
            ohlc=ohlc_data,
            features=feature_data,
            sequence_data=sequence_data,
            file_paths=file_paths
        )
        
    async def _find_best_dataset(self, symbol: str, target_datetime: datetime) -> Optional[str]:
        """Find the latest production dataset that contains data for symbol around target_datetime."""
        
        # Convert datetime to year_month format for matching
        year_month = target_datetime.strftime("%Y_%m")
        target_year = int(year_month[:4])
        
        try:
            # Query database for production datasets that might contain this symbol/date
            import asyncpg
            import os
            
            # Determine environment from base_path
            if "/data/training_data" in str(self.base_path):
                # Container environment - try integration first
                environments = [
                    ('intg', "postgresql://postgres:intg_password@localhost:4432/intg_db"),
                    ('dev', "postgresql://postgres:dev_password@localhost:3432/dev_db")
                ]
            else:
                # Host environment
                environments = [
                    ('dev', "postgresql://postgres:dev_password@localhost:5432/dev_db"),
                    ('intg', "postgresql://postgres:intg_password@localhost:5432/intg_db")
                ]
            
            available_datasets = []
            
            for env_name, db_url in environments:
                try:
                    conn = await asyncpg.connect(db_url)
                    try:
                        # Query for production feature group files
                        query = f"""
                        SELECT fgf.dataset_id, fgf.symbol, fgf.year_month, fgf.feature_group,
                               fgf.timeframes, fgf.file_paths, fgf.created_at, fgf.status
                        FROM {env_name}_feature_group_files fgf
                        WHERE fgf.status = 'prod'
                          AND fgf.symbol = $1
                          AND fgf.year_month = $2
                        ORDER BY fgf.created_at DESC
                        """
                        
                        target_year_month = f"{target_year}_{target_datetime.month:02d}"
                        rows = await conn.fetch(query, symbol, target_year_month)
                        
                        for row in rows:
                            dataset_id = row['dataset_id']
                            feature_group = row['feature_group']
                            symbol_from_db = row['symbol']
                            year_month = row['year_month']
                            
                            # Verify the dataset directory exists
                            dataset_dir = self.base_path / dataset_id
                            if dataset_dir.exists() and dataset_dir.is_dir():
                                # Check if the specific symbol directory exists
                                symbol_period = f"{symbol_from_db}_{year_month}"
                                feature_dir = dataset_dir / feature_group / symbol_period
                                if feature_dir.exists():
                                    available_datasets.append({
                                        'dataset_id': dataset_id,
                                        'path': dataset_dir,
                                        'symbol_dir': symbol_period,
                                        'feature_group': feature_group,
                                        'created': row['created_at'],
                                        'status': row['status'],
                                        'environment': env_name
                                    })
                    
                    finally:
                        await conn.close()
                        
                except Exception as e:
                    # Log database connection error but continue with other environments
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.debug(f"Could not connect to {env_name} database: {e}")
                    continue
            
            if not available_datasets:
                # No production datasets found - fail rather than using unvalidated data
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"No production datasets found in database for {symbol} {year_month}. Only production-tagged datasets are allowed.")
                return None
                
            # Return the most recently created production dataset
            best_dataset = max(available_datasets, key=lambda x: x['created'])
            
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"✅ Using production dataset: {best_dataset['dataset_id']} "
                       f"(status: {best_dataset['status']}, env: {best_dataset['environment']})")
            
            return best_dataset['dataset_id']
            
        except Exception as e:
            # If database query fails, let it fail - no unvalidated data allowed
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Database query failed for production datasets: {e}")
            raise
        
    async def _locate_arrayrecord_files(self, dataset_id: str, request: FeatureRequest) -> List[str]:
        """Locate ArrayRecord files for the requested features."""
        
        dataset_path = self.base_path / dataset_id
        year_month = request.target_datetime.strftime("%Y_%m")
        symbol_period = f"{request.symbol}_{year_month[2:]}"  # TSLA_25_07
        
        # Updated to match actual file naming: TSLA_2025_07
        symbol_period = f"{request.symbol}_{year_month[:4]}_{year_month[5:]}"  # TSLA_2025_07
        
        file_paths = []
        
        for feature_group in request.feature_groups:
            # Path: dataset_id/feature_group/SYMBOL_YEAR_MONTH/timeframe/
            feature_dir = dataset_path / feature_group / symbol_period / request.timeframe
            
            if feature_dir.exists():
                # Expected filename: SYMBOL_YEAR_MONTH_feature_group.arrayrecord
                expected_file = feature_dir / f"{symbol_period}_{feature_group}.arrayrecord"
                
                if expected_file.exists():
                    file_paths.append(str(expected_file))
                    
        return file_paths
        
    async def _read_arrayrecord_files(self, file_paths: List[str], request: FeatureRequest) -> Dict[str, Any]:
        """Read ArrayRecord files and return feature data."""
        
        feature_data = {}
        
        for file_path in file_paths:
            try:
                path_obj = Path(file_path)
                feature_group = path_obj.parent.parent.parent.name
                
                feature_data[feature_group] = {
                    'file_path': file_path,
                    'file_size': path_obj.stat().st_size,
                    'exists': True,
                    'records': []
                }
                
                # Read actual ArrayRecord data
                if ARRAYRECORD_AVAILABLE:
                    print(f"📖 Reading ArrayRecord: {file_path}")
                    
                    # Read ArrayRecord file
                    reader = array_record_module.ArrayRecordReader(file_path)
                    records = []
                    
                    try:
                        # Get number of records first
                        num_records = reader.num_records()
                        print(f"   📊 File contains {num_records} records")
                        
                        if num_records > 0:
                            # Read all records (or first few for testing)
                            max_records = min(num_records, 10)  # Limit for testing
                            
                            for i in range(max_records):
                                try:
                                    # Seek to record and read it
                                    reader.seek(i)
                                    record_bytes = reader.read()
                                    
                                    if record_bytes:
                                        # Parse binary format: indicator_count(2) + timestamp(8) + symbol_len(4) + symbol + OHLCV(20) + indicators
                                        try:
                                            import struct
                                            
                                            # Read indicator count (2 bytes, big-endian unsigned short)
                                            indicator_count = struct.unpack('>H', record_bytes[0:2])[0]
                                            
                                            # Read core data: timestamp(8) + symbol_len(4) + symbol + OHLCV(20)
                                            offset = 2
                                            timestamp, symbol_len = struct.unpack('>dI', record_bytes[offset:offset+12])
                                            offset += 12
                                            
                                            # Read symbol
                                            symbol = record_bytes[offset:offset+symbol_len].decode('utf-8')
                                            offset += symbol_len
                                            
                                            # Read OHLCV data (5 floats = 20 bytes, not 40 bytes)
                                            ohlcv = struct.unpack('>fffff', record_bytes[offset:offset+20])
                                            offset += 20
                                            
                                            # Parse OHLCV
                                            open_price, high_price, low_price, close_price, volume = ohlcv
                                            
                                            # Convert timestamp to datetime
                                            from datetime import datetime
                                            dt = datetime.fromtimestamp(timestamp)
                                            
                                            # Create record
                                            record_data = {
                                                'timestamp': dt.isoformat(),
                                                'symbol': symbol,
                                                'open': float(open_price),
                                                'high': float(high_price), 
                                                'low': float(low_price),
                                                'close': float(close_price),
                                                'volume': int(volume),
                                                'indicator_count': indicator_count
                                            }
                                            
                                            # TODO: Parse indicators if needed
                                            records.append(record_data)
                                            print(f"   ✅ Parsed record {i}: {symbol} {dt.strftime('%Y-%m-%d %H:%M')} O:{open_price:.2f} H:{high_price:.2f} L:{low_price:.2f} C:{close_price:.2f}")
                                            
                                        except Exception as parse_error:
                                            # Log parse error for debugging
                                            print(f"   ⚠️  Binary parse error for record {i}: {parse_error}")
                                            records.append({
                                                'raw_bytes_length': len(record_bytes),
                                                'record_index': i,
                                                'parse_error': str(parse_error)
                                            })
                                            
                                except Exception as e:
                                    print(f"   ⚠️  Error reading record {i}: {e}")
                                    continue
                                    
                    finally:
                        reader.close()
                                
                    feature_data[feature_group]['records'] = records
                    feature_data[feature_group]['record_count'] = len(records)
                    
                    print(f"   ✅ Read {len(records)} records from {feature_group}")
                    
                    # Extract sample data for OHLC if available
                    if records and feature_group == 'ohlcv_basic':
                        sample_record = records[0]  # Use first record
                        feature_data[feature_group]['sample_data'] = self._extract_ohlc_from_record(sample_record)
                            
                else:
                    # No valid records available - let caller handle the missing data
                    raise ValueError(f"No valid {feature_group} records found for {request.symbol} at {request.target_datetime}")
                    
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")
                feature_data[feature_group] = {
                    'file_path': file_path,
                    'error': str(e),
                    'exists': False
                }
                
        return feature_data
        
    def _extract_ohlc_from_record(self, record: Dict[str, Any]) -> Dict[str, float]:
        """Extract OHLC data from a record."""
        
        # Try different possible field names for OHLC data
        ohlc_data = {}
        
        # Look for standard OHLC fields
        field_mappings = {
            'open': ['open', 'Open', 'OPEN'],
            'high': ['high', 'High', 'HIGH'],
            'low': ['low', 'Low', 'LOW'],
            'close': ['close', 'Close', 'CLOSE'],
            'volume': ['volume', 'Volume', 'VOLUME', 'vol']
        }
        
        for ohlc_field, possible_names in field_mappings.items():
            value = None
            for name in possible_names:
                if name in record:
                    value = record[name]
                    break
                    
            if value is not None:
                try:
                    ohlc_data[ohlc_field] = float(value)
                except (ValueError, TypeError):
                    ohlc_data[ohlc_field] = 0.0
            else:
                ohlc_data[ohlc_field] = 0.0
                
        return ohlc_data
        
    def _extract_ohlc_data(self, feature_data: Dict[str, Any], request: FeatureRequest) -> Dict[str, float]:
        """Extract OHLC data from feature data."""
        
        if 'ohlcv_basic' in feature_data and 'sample_data' in feature_data['ohlcv_basic']:
            return feature_data['ohlcv_basic']['sample_data']
            
        # Default empty OHLC
        return {
            'open': 0.0,
            'high': 0.0,
            'low': 0.0,
            'close': 0.0,
            'volume': 0
        }
        
    def _build_sequence_data(self, feature_data: Dict[str, Any], request: FeatureRequest) -> List[Dict[str, Any]]:
        """Build time series sequence data from features."""
        
        sequence = []
        
        # Get actual OHLC records from feature data
        ohlcv_records = []
        if 'ohlcv_basic' in feature_data and 'records' in feature_data['ohlcv_basic']:
            ohlcv_records = feature_data['ohlcv_basic']['records']
        
        if ohlcv_records:
            # Use real data from ArrayRecord files
            print(f"   📊 Building sequence from {len(ohlcv_records)} real OHLC records")
            
            for i, record in enumerate(ohlcv_records):
                if isinstance(record, dict) and 'timestamp' in record:
                    # Use real OHLC data
                    bar_data = {
                        'index': i,
                        'timestamp': record['timestamp'],
                        'symbol': record['symbol'],
                        'timeframe': request.timeframe,
                        'open': record.get('open', 0.0),
                        'high': record.get('high', 0.0),
                        'low': record.get('low', 0.0),
                        'close': record.get('close', 0.0),
                        'volume': record.get('volume', 0)
                    }
                else:
                    # Record parsing failed - raise error instead of providing defaults
                    raise ValueError(f"Could not parse record {i} for {request.symbol} at {request.target_datetime}")
                
                sequence.append(bar_data)
        else:
            # No real OHLC records available - let caller handle the missing data
            raise ValueError(f"No OHLC records found for {request.symbol} at {request.target_datetime}")
            
        return sequence


async def main():
    """Test the feature service."""
    
    # Test with dataset 97 data
    service = FeatureService()
    
    # Test request for TSLA on July 17, 2025 at 2:30 PM
    request = FeatureRequest(
        symbol="TSLA",
        target_datetime=datetime(2025, 7, 17, 14, 30, 0),
        feature_groups=["ohlcv_basic", "technical_momentum"],
        timeframe="60m"
    )
    
    print(f"🎯 Testing Feature Service")
    print(f"Symbol: {request.symbol}")
    print(f"DateTime: {request.target_datetime}")
    print(f"Features: {request.feature_groups}")
    print(f"Timeframe: {request.timeframe}")
    
    try:
        result = await service.get_feature_data(request)
        
        print(f"\n✅ SUCCESS!")
        print(f"Dataset found with OHLC: {result.ohlc}")
        print(f"Features loaded: {list(result.features.keys())}")
        print(f"Sequence length: {len(result.sequence_data)}")
        print(f"Source files: {len(result.file_paths)}")
        
        for file_path in result.file_paths:
            print(f"  📄 {file_path}")
            
    except Exception as e:
        print(f"❌ ERROR: {e}")


if __name__ == "__main__":
    asyncio.run(main())