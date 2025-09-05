#!/usr/bin/env python3
"""
File-Based Minute Data Storage Manager

Manages 1-minute financial data storage using monthly Parquet files instead of database.
Handles overlapping data, missing files, and comprehensive edge cases.

Key Features:
- Monthly file organization (SYMBOL/YEAR/MONTH/)
- Overlap detection and resolution
- Missing file handling
- Data deduplication
- Atomic file operations with backup/restore
- Comprehensive error handling
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple, Set
from pathlib import Path
import logging
from dataclasses import dataclass, field
import shutil
from concurrent.futures import ThreadPoolExecutor
import json
import hashlib
from contextlib import asynccontextmanager

# Optional pyarrow import for Parquet support
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    pq = None
    PYARROW_AVAILABLE = False

logger = logging.getLogger(__name__)

@dataclass
class MinuteBar:
    """Standardized minute bar data structure."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: Optional[float] = None
    trade_count: Optional[int] = None
    vendor: str = 'unknown'
    quality_score: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame."""
        return {
            'timestamp': self.timestamp,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'vwap': self.vwap,
            'trade_count': self.trade_count,
            'vendor': self.vendor,
            'quality_score': self.quality_score
        }

@dataclass
class FileMetadata:
    """Metadata for monthly data files."""
    file_path: Path
    symbol: str
    year: int
    month: int
    record_count: int
    date_range: Tuple[datetime, datetime]
    file_size: int
    checksum: str
    last_modified: datetime
    vendors: Set[str] = field(default_factory=set)

@dataclass
class OverlapInfo:
    """Information about overlapping data between files and new data."""
    overlapping_timestamps: Set[datetime]
    new_timestamps: Set[datetime]
    existing_timestamps: Set[datetime]
    resolution_strategy: str = 'merge'  # 'merge', 'replace', 'skip'

class FileBasedMinuteManager:
    """
    File-based storage manager for 1-minute financial data using monthly Parquet files.
    
    File Structure:
    base_path/
    ├── AAPL/
    │   ├── 2024/
    │   │   ├── 01/
    │   │   │   ├── AAPL_2024_01.parquet
    │   │   │   └── .AAPL_2024_01.metadata.json
    │   │   ├── 02/
    │   │   │   ├── AAPL_2024_02.parquet
    │   │   │   └── .AAPL_2024_02.metadata.json
    """
    
    def __init__(
        self,
        base_path: str = "/home/jianjun/ats-data/minute-files",
        max_concurrent_operations: int = 4,
        backup_enabled: bool = True,
        compression: str = "snappy"
    ):
        self.base_path = Path(base_path)
        self.max_concurrent_operations = max_concurrent_operations
        self.backup_enabled = backup_enabled
        self.compression = compression
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_operations)
        
        # Ensure base directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        # Create backup directory
        if self.backup_enabled:
            self.backup_path = self.base_path / '.backups'
            self.backup_path.mkdir(exist_ok=True)
        
        logger.info(f"FileBasedMinuteManager initialized at {self.base_path}")
    
    def _get_monthly_file_path(self, symbol: str, year: int, month: int) -> Path:
        """Get path for monthly data file. Try firstrate structure first, then fallback to standard."""
        # Try firstrate directory structure first: /base_path/firstrate/[FIRST_LETTER]/SYMBOL/YEAR/MONTH/
        first_letter = symbol[0].upper()
        firstrate_path = self.base_path / "firstrate" / first_letter / symbol / str(year) / f"{month:02d}" / f"{symbol}_{year}_{month:02d}.parquet"
        
        if firstrate_path.exists():
            return firstrate_path
        
        # Fallback to standard structure: /base_path/SYMBOL/YEAR/MONTH/
        symbol_dir = self.base_path / symbol / str(year) / f"{month:02d}"
        symbol_dir.mkdir(parents=True, exist_ok=True)
        return symbol_dir / f"{symbol}_{year}_{month:02d}.parquet"
    
    def _get_metadata_path(self, file_path: Path) -> Path:
        """Get path for metadata file."""
        return file_path.parent / f".{file_path.stem}.metadata.json"
    
    def _get_backup_path(self, file_path: Path) -> Path:
        """Get path for backup file."""
        if not self.backup_enabled:
            return None
        
        relative_path = file_path.relative_to(self.base_path)
        backup_path = self.backup_path / relative_path
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        return backup_path.with_suffix(f".{datetime.now().strftime('%Y%m%d_%H%M%S')}.backup")
    
    async def store_minute_data(
        self,
        symbol: str,
        bars: List[MinuteBar],
        overlap_strategy: str = 'merge'
    ) -> Dict[str, Any]:
        """
        Store minute bars in monthly files with overlap handling.
        
        Args:
            symbol: Stock symbol
            bars: List of minute bars
            overlap_strategy: How to handle overlapping data
                - 'merge': Combine new and existing data
                - 'replace': Replace existing data with new data
                - 'skip': Skip overlapping timestamps
        
        Returns:
            Storage statistics
        """
        if not bars:
            return {'stored': 0, 'skipped': 0, 'updated': 0}
        
        logger.info(f"Storing {len(bars)} minute bars for {symbol}")
        
        # Group bars by month
        monthly_groups = self._group_bars_by_month(bars)
        
        results = {
            'stored': 0,
            'skipped': 0, 
            'updated': 0,
            'files_created': 0,
            'files_updated': 0,
            'errors': []
        }
        
        # Process each month
        for (year, month), month_bars in monthly_groups.items():
            try:
                month_result = await self._store_monthly_data(
                    symbol, year, month, month_bars, overlap_strategy
                )
                
                # Aggregate results
                for key in ['stored', 'skipped', 'updated']:
                    results[key] += month_result.get(key, 0)
                
                if month_result.get('file_created'):
                    results['files_created'] += 1
                elif month_result.get('file_updated'):
                    results['files_updated'] += 1
                    
            except Exception as e:
                error_msg = f"Error storing {symbol} {year}-{month:02d}: {e}"
                logger.error(error_msg)
                results['errors'].append(error_msg)
        
        logger.info(f"Storage complete for {symbol}: {results}")
        return results
    
    def _group_bars_by_month(self, bars: List[MinuteBar]) -> Dict[Tuple[int, int], List[MinuteBar]]:
        """Group bars by year and month."""
        groups = {}
        for bar in bars:
            key = (bar.timestamp.year, bar.timestamp.month)
            if key not in groups:
                groups[key] = []
            groups[key].append(bar)
        return groups
    
    async def _store_monthly_data(
        self,
        symbol: str,
        year: int,
        month: int,
        bars: List[MinuteBar],
        overlap_strategy: str
    ) -> Dict[str, Any]:
        """Store data for a specific month with overlap handling."""
        file_path = self._get_monthly_file_path(symbol, year, month)
        
        # Check if file exists
        file_exists = file_path.exists()
        
        if file_exists:
            # Handle overlapping data
            return await self._handle_overlapping_data(
                file_path, bars, overlap_strategy
            )
        else:
            # Create new file
            return await self._create_new_monthly_file(
                file_path, symbol, year, month, bars
            )
    
    async def _handle_overlapping_data(
        self,
        file_path: Path,
        new_bars: List[MinuteBar],
        overlap_strategy: str
    ) -> Dict[str, Any]:
        """Handle overlapping data between existing file and new data."""
        logger.debug(f"Handling overlapping data for {file_path}")
        
        try:
            # Read existing data
            existing_df = await self._read_parquet_async(file_path)
            if existing_df.empty:
                # File exists but is empty - treat as new file
                return await self._create_new_monthly_file(
                    file_path, new_bars[0].symbol, 
                    new_bars[0].timestamp.year, 
                    new_bars[0].timestamp.month, 
                    new_bars
                )
            
            # Convert new bars to DataFrame
            new_df = pd.DataFrame([bar.to_dict() for bar in new_bars])
            new_df['timestamp'] = pd.to_datetime(new_df['timestamp'])
            
            # Analyze overlap
            overlap_info = self._analyze_overlap(existing_df, new_df)
            
            # Apply overlap strategy
            combined_df = self._apply_overlap_strategy(
                existing_df, new_df, overlap_info, overlap_strategy
            )
            
            # Create backup if enabled
            backup_path = None
            if self.backup_enabled:
                backup_path = self._get_backup_path(file_path)
                shutil.copy2(file_path, backup_path)
                logger.debug(f"Backup created: {backup_path}")
            
            # Write updated file atomically
            await self._write_parquet_atomic(file_path, combined_df)
            
            # Update metadata
            await self._update_metadata(file_path, combined_df)
            
            # Calculate correct counts based on strategy
            if overlap_strategy == 'skip':
                result = {
                    'stored': len(overlap_info.new_timestamps),
                    'updated': 0,
                    'skipped': len(overlap_info.overlapping_timestamps),
                    'file_updated': True,
                    'backup_created': backup_path is not None
                }
            else:
                result = {
                    'stored': len(overlap_info.new_timestamps),
                    'updated': len(overlap_info.overlapping_timestamps),
                    'skipped': 0,
                    'file_updated': True,
                    'backup_created': backup_path is not None
                }
            
            logger.debug(f"Overlap handling complete: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error handling overlapping data for {file_path}: {e}")
            raise
    
    def _analyze_overlap(self, existing_df: pd.DataFrame, new_df: pd.DataFrame) -> OverlapInfo:
        """Analyze overlap between existing and new data."""
        existing_timestamps = set(existing_df['timestamp'])
        new_timestamps = set(new_df['timestamp'])
        
        overlapping_timestamps = existing_timestamps.intersection(new_timestamps)
        
        return OverlapInfo(
            overlapping_timestamps=overlapping_timestamps,
            new_timestamps=new_timestamps - overlapping_timestamps,
            existing_timestamps=existing_timestamps - overlapping_timestamps
        )
    
    def _apply_overlap_strategy(
        self,
        existing_df: pd.DataFrame,
        new_df: pd.DataFrame,
        overlap_info: OverlapInfo,
        strategy: str
    ) -> pd.DataFrame:
        """Apply overlap resolution strategy."""
        if strategy == 'skip':
            # Skip overlapping timestamps - only add truly new data
            filtered_new_df = new_df[~new_df['timestamp'].isin(overlap_info.overlapping_timestamps)]
            combined_df = pd.concat([existing_df, filtered_new_df], ignore_index=True)
            
        elif strategy == 'replace':
            # Replace overlapping timestamps with new data
            filtered_existing_df = existing_df[~existing_df['timestamp'].isin(overlap_info.overlapping_timestamps)]
            combined_df = pd.concat([filtered_existing_df, new_df], ignore_index=True)
            
        else:  # 'merge' - default
            # Merge data, preferring new data for overlapping timestamps
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
        
        # Sort by timestamp
        combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
        
        return combined_df
    
    async def _create_new_monthly_file(
        self,
        file_path: Path,
        symbol: str,
        year: int,
        month: int,
        bars: List[MinuteBar]
    ) -> Dict[str, Any]:
        """Create a new monthly data file."""
        logger.debug(f"Creating new monthly file: {file_path}")
        
        try:
            # Convert bars to DataFrame
            df = pd.DataFrame([bar.to_dict() for bar in bars])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # Write file atomically
            await self._write_parquet_atomic(file_path, df)
            
            # Create metadata
            await self._create_metadata(file_path, df)
            
            result = {
                'stored': len(bars),
                'updated': 0,
                'skipped': 0,
                'file_created': True
            }
            
            logger.debug(f"New file created: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error creating new file {file_path}: {e}")
            raise
    
    async def _write_parquet_atomic(self, file_path: Path, df: pd.DataFrame):
        """Write Parquet file atomically using temporary file."""
        temp_file = file_path.with_suffix('.tmp')
        
        try:
            # Write to temporary file
            await asyncio.get_event_loop().run_in_executor(
                self.executor,
                self._write_parquet_sync,
                df, temp_file
            )
            
            # Atomic move to final location
            temp_file.replace(file_path)
            
        except Exception as e:
            # Clean up temp file on error
            if temp_file.exists():
                temp_file.unlink()
            raise e
    
    def _write_parquet_sync(self, df: pd.DataFrame, file_path: Path):
        """Synchronous Parquet write operation."""
        df.to_parquet(
            file_path,
            engine='pyarrow',
            compression=self.compression,
            index=False
        )
    
    async def _read_parquet_async(self, file_path: Path) -> pd.DataFrame:
        """Read Parquet file asynchronously."""
        return await asyncio.get_event_loop().run_in_executor(
            self.executor,
            pd.read_parquet,
            file_path
        )
    
    async def _create_metadata(self, file_path: Path, df: pd.DataFrame):
        """Create metadata file for monthly data."""
        metadata = FileMetadata(
            file_path=file_path,
            symbol=df['timestamp'].iloc[0] if not df.empty else 'unknown',  # Will be overridden
            year=df['timestamp'].iloc[0].year if not df.empty else 0,
            month=df['timestamp'].iloc[0].month if not df.empty else 0,
            record_count=len(df),
            date_range=(df['timestamp'].min(), df['timestamp'].max()) if not df.empty else (None, None),
            file_size=file_path.stat().st_size if file_path.exists() else 0,
            checksum=self._calculate_checksum(file_path),
            last_modified=datetime.now(),
            vendors=set(df['vendor'].unique()) if 'vendor' in df.columns else set()
        )
        
        # Extract symbol from file path
        metadata.symbol = file_path.stem.split('_')[0]
        
        await self._save_metadata(file_path, metadata)
    
    async def _update_metadata(self, file_path: Path, df: pd.DataFrame):
        """Update existing metadata file."""
        await self._create_metadata(file_path, df)
    
    async def _save_metadata(self, file_path: Path, metadata: FileMetadata):
        """Save metadata to JSON file."""
        metadata_path = self._get_metadata_path(file_path)
        
        metadata_dict = {
            'symbol': metadata.symbol,
            'year': metadata.year,
            'month': metadata.month,
            'record_count': metadata.record_count,
            'date_range': [
                metadata.date_range[0].isoformat() if metadata.date_range[0] else None,
                metadata.date_range[1].isoformat() if metadata.date_range[1] else None
            ],
            'file_size': metadata.file_size,
            'checksum': metadata.checksum,
            'last_modified': metadata.last_modified.isoformat(),
            'vendors': list(metadata.vendors)
        }
        
        await asyncio.get_event_loop().run_in_executor(
            self.executor,
            self._write_json_sync,
            metadata_dict, metadata_path
        )
    
    def _write_json_sync(self, data: Dict, file_path: Path):
        """Synchronous JSON write operation."""
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate MD5 checksum of file."""
        if not file_path.exists():
            return ""
        
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    async def query_minute_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Query minute data from monthly files.
        
        Handles missing files gracefully and combines data from multiple months.
        """
        logger.debug(f"Querying {symbol} from {start_date} to {end_date}")
        
        # Find relevant monthly files
        file_paths = self._find_relevant_monthly_files(symbol, start_date, end_date)
        
        if not file_paths:
            logger.warning(f"No files found for {symbol} in date range")
            return pd.DataFrame()
        
        # Read files concurrently
        tasks = [self._read_and_filter_file(fp, start_date, end_date, columns) 
                for fp in file_paths]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combine results, handling exceptions
        valid_dfs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to read {file_paths[i]}: {result}")
                continue
            if result is not None and not result.empty:
                valid_dfs.append(result)
        
        if not valid_dfs:
            logger.warning(f"No valid data found for {symbol} in date range")
            return pd.DataFrame()
        
        # Combine and deduplicate
        combined_df = pd.concat(valid_dfs, ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
        combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
        
        logger.debug(f"Query complete: {len(combined_df)} records returned")
        return combined_df
    
    def _find_relevant_monthly_files(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Path]:
        """Find monthly files that might contain data in the date range."""
        files = []
        
        # Generate list of months in date range
        current_date = start_date.replace(day=1)
        end_month = end_date.replace(day=1)
        
        while current_date <= end_month:
            file_path = self._get_monthly_file_path(
                symbol, current_date.year, current_date.month
            )
            
            if file_path.exists():
                files.append(file_path)
            else:
                logger.debug(f"Missing file: {file_path}")
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        return files
    
    async def _read_and_filter_file(
        self,
        file_path: Path,
        start_date: datetime,
        end_date: datetime,
        columns: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """Read and filter a single file."""
        try:
            df = await self._read_parquet_async(file_path)
            
            if df.empty:
                return None
            
            # Convert timestamp column
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Ensure timezone compatibility for date filtering
            if df['timestamp'].dt.tz is not None:
                # Data has timezone, convert filter dates to same timezone
                if hasattr(start_date, 'tz_localize'):
                    # If start_date is timezone-naive, localize to UTC
                    start_date_tz = start_date.tz_localize('UTC') if start_date.tzinfo is None else start_date
                    end_date_tz = end_date.tz_localize('UTC') if end_date.tzinfo is None else end_date
                else:
                    # Convert datetime to pandas timestamp with UTC
                    start_date_tz = pd.Timestamp(start_date).tz_localize('UTC') if start_date.tzinfo is None else pd.Timestamp(start_date)
                    end_date_tz = pd.Timestamp(end_date).tz_localize('UTC') if end_date.tzinfo is None else pd.Timestamp(end_date)
            else:
                # Data is timezone-naive, use dates as-is
                start_date_tz = start_date
                end_date_tz = end_date
            
            # Filter by date range
            mask = (df['timestamp'] >= start_date_tz) & (df['timestamp'] <= end_date_tz)
            filtered_df = df[mask]
            
            # Select specific columns if requested
            if columns and not filtered_df.empty:
                available_cols = [col for col in columns if col in filtered_df.columns]
                if available_cols:
                    filtered_df = filtered_df[available_cols]
            
            return filtered_df if not filtered_df.empty else None
            
        except Exception as e:
            logger.error(f"Error reading/filtering {file_path}: {e}")
            raise
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get comprehensive storage statistics."""
        stats = {
            'symbols': 0,
            'files': 0,
            'total_records': 0,
            'total_size_bytes': 0,
            'date_range': {'min': None, 'max': None},
            'vendors': set(),
            'symbols_detail': {}
        }
        
        # Walk through all symbol directories
        for symbol_dir in self.base_path.iterdir():
            if not symbol_dir.is_dir() or symbol_dir.name.startswith('.'):
                continue
                
            symbol = symbol_dir.name
            symbol_stats = {
                'files': 0,
                'records': 0,
                'size_bytes': 0,
                'date_range': {'min': None, 'max': None},
                'vendors': set()
            }
            
            # Walk through year/month directories
            for parquet_file in symbol_dir.rglob("*.parquet"):
                try:
                    # File statistics
                    file_stat = parquet_file.stat()
                    symbol_stats['files'] += 1
                    symbol_stats['size_bytes'] += file_stat.st_size
                    
                    # Try to read metadata first
                    metadata_path = self._get_metadata_path(parquet_file)
                    if metadata_path.exists():
                        with open(metadata_path, 'r') as f:
                            metadata = json.load(f)
                            symbol_stats['records'] += metadata.get('record_count', 0)
                            symbol_stats['vendors'].update(metadata.get('vendors', []))
                            
                            # Update date range
                            if metadata.get('date_range'):
                                file_min = datetime.fromisoformat(metadata['date_range'][0]) if metadata['date_range'][0] else None
                                file_max = datetime.fromisoformat(metadata['date_range'][1]) if metadata['date_range'][1] else None
                                
                                if file_min:
                                    if symbol_stats['date_range']['min'] is None or file_min < symbol_stats['date_range']['min']:
                                        symbol_stats['date_range']['min'] = file_min
                                    
                                if file_max:
                                    if symbol_stats['date_range']['max'] is None or file_max > symbol_stats['date_range']['max']:
                                        symbol_stats['date_range']['max'] = file_max
                                    
                except Exception as e:
                    logger.warning(f"Error reading stats for {parquet_file}: {e}")
                    continue
            
            if symbol_stats['files'] > 0:
                stats['symbols'] += 1
                stats['files'] += symbol_stats['files']
                stats['total_records'] += symbol_stats['records']
                stats['total_size_bytes'] += symbol_stats['size_bytes']
                stats['vendors'].update(symbol_stats['vendors'])
                stats['symbols_detail'][symbol] = symbol_stats
                
                # Update global date range
                if symbol_stats['date_range']['min']:
                    if stats['date_range']['min'] is None or symbol_stats['date_range']['min'] < stats['date_range']['min']:
                        stats['date_range']['min'] = symbol_stats['date_range']['min']
                        
                if symbol_stats['date_range']['max']:
                    if stats['date_range']['max'] is None or symbol_stats['date_range']['max'] > stats['date_range']['max']:
                        stats['date_range']['max'] = symbol_stats['date_range']['max']
        
        # Convert sets to lists for JSON serialization
        stats['vendors'] = list(stats['vendors'])
        for symbol_detail in stats['symbols_detail'].values():
            symbol_detail['vendors'] = list(symbol_detail['vendors'])
        
        # Add derived metrics
        stats['total_size_mb'] = stats['total_size_bytes'] / (1024 * 1024)
        stats['total_size_gb'] = stats['total_size_mb'] / 1024
        stats['avg_file_size_mb'] = stats['total_size_mb'] / stats['files'] if stats['files'] > 0 else 0
        
        return stats
    
    async def verify_data_integrity(self, symbol: str = None) -> Dict[str, Any]:
        """
        Verify data integrity across files.
        
        Checks for:
        - File corruption
        - Missing timestamps within files
        - Metadata consistency
        - Checksum validation
        """
        verification_results = {
            'verified_files': 0,
            'corrupt_files': 0,
            'missing_metadata': 0,
            'checksum_mismatches': 0,
            'timestamp_gaps': 0,
            'issues': []
        }
        
        # Get files to verify
        if symbol:
            search_pattern = self.base_path / symbol / "**" / "*.parquet"
        else:
            search_pattern = self.base_path / "**" / "*.parquet"
        
        files_to_verify = list(self.base_path.glob(str(search_pattern.relative_to(self.base_path))))
        
        logger.info(f"Verifying {len(files_to_verify)} files")
        
        # Verify files concurrently
        tasks = [self._verify_single_file(fp) for fp in files_to_verify]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Aggregate results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                verification_results['corrupt_files'] += 1
                verification_results['issues'].append({
                    'file': str(files_to_verify[i]),
                    'issue': 'verification_error',
                    'details': str(result)
                })
                continue
                
            verification_results['verified_files'] += 1
            
            # Aggregate specific issues
            for issue_type in ['missing_metadata', 'checksum_mismatch', 'timestamp_gaps']:
                if result.get(issue_type):
                    verification_results[f"{issue_type}s"] += 1
                    verification_results['issues'].append({
                        'file': str(files_to_verify[i]),
                        'issue': issue_type,
                        'details': result.get(f'{issue_type}_details', '')
                    })
        
        logger.info(f"Verification complete: {verification_results}")
        return verification_results
    
    async def _verify_single_file(self, file_path: Path) -> Dict[str, Any]:
        """Verify a single file's integrity."""
        result = {
            'missing_metadata': False,
            'checksum_mismatch': False,
            'timestamp_gaps': False
        }
        
        # Check metadata exists
        metadata_path = self._get_metadata_path(file_path)
        if not metadata_path.exists():
            result['missing_metadata'] = True
            result['missing_metadata_details'] = f"Metadata file not found: {metadata_path}"
            return result
        
        # Verify checksum
        try:
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            
            expected_checksum = metadata.get('checksum', '')
            actual_checksum = self._calculate_checksum(file_path)
            
            if expected_checksum != actual_checksum:
                result['checksum_mismatch'] = True
                result['checksum_mismatch_details'] = f"Expected: {expected_checksum}, Actual: {actual_checksum}"
                
        except Exception as e:
            result['checksum_mismatch'] = True
            result['checksum_mismatch_details'] = f"Error reading metadata: {e}"
        
        # Check for timestamp gaps (basic check)
        try:
            df = await self._read_parquet_async(file_path)
            if not df.empty:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp')
                
                # Check for large gaps (more than 5 minutes)
                time_diffs = df['timestamp'].diff()
                large_gaps = time_diffs > pd.Timedelta(minutes=5)
                
                if large_gaps.any():
                    result['timestamp_gaps'] = True
                    result['timestamp_gaps_details'] = f"Found {large_gaps.sum()} gaps > 5 minutes"
                    
        except Exception as e:
            # Don't fail verification for timestamp analysis errors
            logger.warning(f"Could not analyze timestamps for {file_path}: {e}")
        
        return result
    
    @asynccontextmanager
    async def transaction(self):
        """Context manager for transactional operations (future enhancement)."""
        # For now, this is a simple passthrough
        # Could be enhanced to track operations and rollback on failure
        yield self
    
    async def cleanup_old_backups(self, days_old: int = 7) -> int:
        """Clean up old backup files."""
        if not self.backup_enabled:
            return 0
        
        cutoff_date = datetime.now() - timedelta(days=days_old)
        cleaned_count = 0
        
        for backup_file in self.backup_path.rglob("*.backup"):
            try:
                file_mtime = datetime.fromtimestamp(backup_file.stat().st_mtime)
                if file_mtime < cutoff_date:
                    backup_file.unlink()
                    cleaned_count += 1
            except Exception as e:
                logger.warning(f"Error cleaning backup {backup_file}: {e}")
        
        logger.info(f"Cleaned {cleaned_count} old backup files")
        return cleaned_count
    
    async def close(self):
        """Clean up resources."""
        self.executor.shutdown(wait=True)
        logger.info("FileBasedMinuteManager closed")


# Convenience functions
async def create_file_manager(
    base_path: str = None,
    **kwargs
) -> FileBasedMinuteManager:
    """Create and initialize a file-based minute manager."""
    if base_path is None:
        base_path = "/home/jianjun/ats-data/minute-files"
    
    manager = FileBasedMinuteManager(base_path, **kwargs)
    return manager


# Example usage and testing utilities
if __name__ == "__main__":
    import random
    
    async def example_usage():
        """Example usage of the file-based minute manager."""
        
        # Create manager
        manager = await create_file_manager()
        
        # Create sample data
        sample_bars = []
        base_time = datetime(2024, 1, 15, 9, 30)  # Market open
        
        for i in range(100):
            bar = MinuteBar(
                symbol='AAPL',
                timestamp=base_time + timedelta(minutes=i),
                open=150.0 + random.uniform(-1, 1),
                high=150.0 + random.uniform(0, 2),
                low=150.0 + random.uniform(-2, 0),
                close=150.0 + random.uniform(-1, 1),
                volume=random.randint(1000, 10000),
                vendor='test'
            )
            sample_bars.append(bar)
        
        # Store data
        print("Storing sample data...")
        result = await manager.store_minute_data('AAPL', sample_bars)
        print(f"Storage result: {result}")
        
        # Query data
        print("Querying data...")
        query_result = await manager.query_minute_data(
            'AAPL',
            datetime(2024, 1, 15, 9, 30),
            datetime(2024, 1, 15, 11, 30)
        )
        print(f"Query returned {len(query_result)} records")
        
        # Get stats
        stats = await manager.get_storage_stats()
        print(f"Storage stats: {stats}")
        
        # Verify integrity
        integrity = await manager.verify_data_integrity('AAPL')
        print(f"Integrity check: {integrity}")
        
        await manager.close()
    
    # Run example
    asyncio.run(example_usage())