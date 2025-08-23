#!/usr/bin/env python3
"""
Data Integrity Validator for File-Based Migration

Comprehensive validation suite to ensure data integrity during and after
the migration from database storage to file-based storage.

Validation Types:
1. Record Count Validation - Compare total records between DB and files
2. Data Content Validation - Compare actual values for sample records
3. Date Range Validation - Ensure complete date coverage
4. File Format Validation - Verify binary format integrity
5. Metadata Validation - Check file metadata consistency
6. Performance Validation - Compare query performance between systems

Usage:
    # Validate specific date range
    python data_integrity_validator.py --start-date 2024-01-01 --end-date 2024-01-31
    
    # Comprehensive validation
    python data_integrity_validator.py --full-validation --sample-size 1000
    
    # Performance comparison
    python data_integrity_validator.py --performance-test --instruments 10
"""

import asyncio
import asyncpg
import logging
import os
import json
import argparse
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple, Any, Set
from dataclasses import dataclass, asdict
from pathlib import Path
import sys
import random
import statistics
from collections import defaultdict

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from storage.time_series_file_manager import (
    TimeSeriesFileManager,
    MinuteRecord,
    TimeSeriesQueryEngine,
    FileMetadata
)

@dataclass
class ValidationConfig:
    """Configuration for validation tests"""
    # Database connection
    db_host: str = "postgres-simple"
    db_password: str = "dev_password" 
    db_name: str = "dev_db"
    
    # File storage
    file_base_path: str = "/data/monthly/interval"
    
    # Validation parameters
    sample_size: int = 100              # Records to sample for content validation
    tolerance: float = 0.001            # Tolerance for float comparisons
    max_date_gaps: int = 5              # Max acceptable missing days
    
    # Source tables to validate against
    source_tables: List[str] = None
    
    # Performance test parameters
    performance_test_instruments: int = 10
    performance_test_days: int = 30
    
    def __post_init__(self):
        if self.source_tables is None:
            self.source_tables = [
                'dev_minute_prices_fmp',
                'dev_minute_prices_polygon',
                'dev_minute_prices_tiingo'
            ]

@dataclass
class ValidationResult:
    """Result of a validation test"""
    test_name: str
    success: bool
    details: Dict[str, Any] = None
    errors: List[str] = None
    warnings: List[str] = None
    execution_time: float = 0.0
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []

@dataclass
class ValidationSummary:
    """Overall validation summary"""
    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    warnings_count: int = 0
    total_execution_time: float = 0.0
    results: List[ValidationResult] = None
    
    def __post_init__(self):
        if self.results is None:
            self.results = []

class DataIntegrityValidator:
    """Comprehensive data integrity validator"""
    
    def __init__(self, config: ValidationConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Database connection
        self.db_url = f"postgresql://postgres:{config.db_password}@{config.db_host}:5432/{config.db_name}"
        
        # File manager
        self.file_manager = TimeSeriesFileManager(config.file_base_path)
        self.query_engine = TimeSeriesQueryEngine(self.file_manager)
        
        # Validation summary
        self.summary = ValidationSummary()
    
    async def run_all_validations(self, 
                                start_date: date = None, 
                                end_date: date = None,
                                instrument_ids: List[int] = None) -> ValidationSummary:
        """Run comprehensive validation suite"""
        
        self.logger.info("🔍 Starting Comprehensive Data Integrity Validation")
        
        if start_date is None:
            start_date = date(2024, 1, 1)
        if end_date is None:
            end_date = date.today()
        
        self.logger.info(f"📅 Validation period: {start_date} to {end_date}")
        
        validation_tests = [
            ("Record Count Validation", self.validate_record_counts),
            ("Date Range Validation", self.validate_date_ranges), 
            ("File Format Validation", self.validate_file_formats),
            ("Metadata Consistency", self.validate_metadata_consistency),
            ("Sample Data Content", self.validate_sample_data_content),
            ("Storage Statistics", self.validate_storage_statistics)
        ]
        
        # Add performance test if requested
        if self.config.performance_test_instruments > 0:
            validation_tests.append(("Performance Comparison", self.validate_performance))
        
        for test_name, test_func in validation_tests:
            self.logger.info(f"🧪 Running {test_name}...")
            
            start_time = datetime.now()
            
            try:
                if test_name == "Performance Comparison":
                    result = await test_func()
                elif test_name in ["Sample Data Content", "Record Count Validation"]:
                    result = await test_func(start_date, end_date, instrument_ids)
                else:
                    result = await test_func()
                
                result.execution_time = (datetime.now() - start_time).total_seconds()
                
                self.summary.results.append(result)
                self.summary.total_tests += 1
                self.summary.total_execution_time += result.execution_time
                
                if result.success:
                    self.summary.passed_tests += 1
                    self.logger.info(f"✅ {test_name} passed ({result.execution_time:.2f}s)")
                else:
                    self.summary.failed_tests += 1
                    self.logger.error(f"❌ {test_name} failed ({result.execution_time:.2f}s)")
                    for error in result.errors:
                        self.logger.error(f"   Error: {error}")
                
                if result.warnings:
                    self.summary.warnings_count += len(result.warnings)
                    for warning in result.warnings:
                        self.logger.warning(f"   Warning: {warning}")
            
            except Exception as e:
                error_result = ValidationResult(
                    test_name=test_name,
                    success=False,
                    errors=[f"Test execution failed: {str(e)}"],
                    execution_time=(datetime.now() - start_time).total_seconds()
                )
                
                self.summary.results.append(error_result)
                self.summary.total_tests += 1
                self.summary.failed_tests += 1
                
                self.logger.error(f"💥 {test_name} crashed: {e}")
        
        # Generate final report
        await self.generate_validation_report()
        
        return self.summary
    
    async def validate_record_counts(self, 
                                   start_date: date = None, 
                                   end_date: date = None,
                                   instrument_ids: List[int] = None) -> ValidationResult:
        """Validate record counts between database and files"""
        
        result = ValidationResult(
            test_name="Record Count Validation",
            success=True
        )
        
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=3)
        
        try:
            async with pool.acquire() as conn:
                # Get instruments to validate
                if instrument_ids is None:
                    instrument_rows = await conn.fetch(
                        "SELECT id FROM dev_instruments WHERE symbol IS NOT NULL LIMIT 50"
                    )
                    instrument_ids = [row['id'] for row in instrument_rows]
                
                total_db_records = 0
                total_file_records = 0
                mismatched_instruments = []
                
                for instrument_id in instrument_ids:
                    # Count DB records
                    db_count = 0
                    for table in self.config.source_tables:
                        try:
                            table_exists = await conn.fetchval(
                                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1",
                                table
                            )
                            
                            if table_exists:
                                count = await conn.fetchval(f"""
                                    SELECT COUNT(*) FROM {table} 
                                    WHERE instrument_id = $1
                                    {f"AND timestamp::date BETWEEN '{start_date}' AND '{end_date}'" if start_date and end_date else ""}
                                """, instrument_id)
                                db_count += count or 0
                        
                        except Exception as e:
                            result.warnings.append(f"Could not query table {table}: {e}")
                    
                    # Count file records
                    file_count = 0
                    try:
                        if start_date and end_date:
                            # Query specific date range
                            file_data = await self.query_engine.query_range(
                                [instrument_id],
                                datetime.combine(start_date, datetime.min.time()),
                                datetime.combine(end_date, datetime.max.time())
                            )
                            file_count = len(file_data.get(instrument_id, []))
                        else:
                            # Count all available files
                            available_months = await self.file_manager.list_available_data(instrument_id)
                            for year, month in available_months:
                                metadata = await self.file_manager.get_file_metadata(instrument_id, year, month)
                                if metadata:
                                    file_count += metadata.record_count
                    
                    except Exception as e:
                        result.warnings.append(f"Could not count files for instrument {instrument_id}: {e}")
                    
                    total_db_records += db_count
                    total_file_records += file_count
                    
                    # Check for significant mismatches
                    if abs(db_count - file_count) > max(db_count * 0.01, 10):  # >1% or >10 records
                        mismatched_instruments.append({
                            'instrument_id': instrument_id,
                            'db_records': db_count,
                            'file_records': file_count,
                            'difference': abs(db_count - file_count)
                        })
                
                result.details = {
                    'total_db_records': total_db_records,
                    'total_file_records': total_file_records,
                    'total_difference': abs(total_db_records - total_file_records),
                    'instruments_checked': len(instrument_ids),
                    'mismatched_instruments': len(mismatched_instruments),
                    'match_percentage': (total_file_records / total_db_records * 100) if total_db_records > 0 else 0
                }
                
                # Determine success
                if total_db_records == 0:
                    result.success = False
                    result.errors.append("No database records found")
                elif abs(total_db_records - total_file_records) > total_db_records * 0.05:  # >5% difference
                    result.success = False
                    result.errors.append(f"Significant record count mismatch: DB={total_db_records:,}, Files={total_file_records:,}")
                
                if mismatched_instruments:
                    result.warnings.append(f"{len(mismatched_instruments)} instruments have mismatched record counts")
        
        finally:
            await pool.close()
        
        return result
    
    async def validate_date_ranges(self) -> ValidationResult:
        """Validate date range completeness in file storage"""
        
        result = ValidationResult(
            test_name="Date Range Validation",
            success=True
        )
        
        try:
            # Get storage statistics to analyze date coverage
            stats = await self.file_manager.get_storage_stats()
            
            if stats['total_files'] == 0:
                result.success = False
                result.errors.append("No files found in storage")
                return result
            
            # Analyze monthly coverage gaps
            all_months = set()
            current_date = date(2005, 1, 1)
            end_date = date.today()
            
            while current_date <= end_date:
                all_months.add((current_date.year, current_date.month))
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            
            # Sample some instruments to check coverage
            pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=2)
            
            try:
                async with pool.acquire() as conn:
                    sample_instruments = await conn.fetch(
                        "SELECT id FROM dev_instruments WHERE symbol IS NOT NULL LIMIT 10"
                    )
                
                coverage_gaps = []
                for instrument_row in sample_instruments:
                    instrument_id = instrument_row['id']
                    available_months = await self.file_manager.list_available_data(instrument_id)
                    available_set = set(available_months)
                    
                    missing_months = all_months - available_set
                    if len(missing_months) > self.config.max_date_gaps:
                        coverage_gaps.append({
                            'instrument_id': instrument_id,
                            'missing_months': len(missing_months),
                            'total_months': len(all_months),
                            'coverage_percentage': (len(available_set) / len(all_months)) * 100
                        })
            
            finally:
                await pool.close()
            
            result.details = {
                'total_files': stats['total_files'],
                'years_covered': stats['years_covered'],
                'instruments_with_gaps': len(coverage_gaps),
                'average_coverage': statistics.mean([gap['coverage_percentage'] for gap in coverage_gaps]) if coverage_gaps else 100
            }
            
            if coverage_gaps:
                avg_coverage = statistics.mean([gap['coverage_percentage'] for gap in coverage_gaps])
                if avg_coverage < 80:  # Less than 80% coverage
                    result.success = False
                    result.errors.append(f"Poor date coverage: {avg_coverage:.1f}% average")
                else:
                    result.warnings.append(f"Some date gaps found, average coverage: {avg_coverage:.1f}%")
        
        except Exception as e:
            result.success = False
            result.errors.append(f"Date range validation failed: {e}")
        
        return result
    
    async def validate_file_formats(self) -> ValidationResult:
        """Validate binary file format integrity"""
        
        result = ValidationResult(
            test_name="File Format Validation",
            success=True
        )
        
        try:
            # Get sample of files to validate
            stats = await self.file_manager.get_storage_stats()
            
            if stats['total_files'] == 0:
                result.success = False
                result.errors.append("No files found to validate")
                return result
            
            # Test file reading and format validation
            pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=2)
            
            try:
                async with pool.acquire() as conn:
                    sample_instruments = await conn.fetch(
                        "SELECT id FROM dev_instruments WHERE symbol IS NOT NULL LIMIT 5"
                    )
                
                format_errors = []
                successful_reads = 0
                total_files_tested = 0
                
                for instrument_row in sample_instruments:
                    instrument_id = instrument_row['id']
                    available_months = await self.file_manager.list_available_data(instrument_id)
                    
                    # Test a few files for this instrument
                    for year, month in available_months[:3]:  # Test first 3 months
                        total_files_tested += 1
                        
                        try:
                            # Test metadata reading
                            metadata = await self.file_manager.get_file_metadata(instrument_id, year, month)
                            if not metadata:
                                format_errors.append(f"Could not read metadata for {instrument_id}_{year}_{month:02d}")
                                continue
                            
                            # Test record reading
                            records = await self.file_manager.read_monthly_file(instrument_id, year, month)
                            if not records:
                                result.warnings.append(f"No records in file {instrument_id}_{year}_{month:02d}")
                            elif len(records) != metadata.record_count:
                                format_errors.append(f"Record count mismatch in {instrument_id}_{year}_{month:02d}: metadata says {metadata.record_count}, read {len(records)}")
                            else:
                                successful_reads += 1
                                
                                # Validate record format
                                sample_record = records[0]
                                if not isinstance(sample_record.timestamp, datetime):
                                    format_errors.append(f"Invalid timestamp format in {instrument_id}_{year}_{month:02d}")
                                if not (0 <= sample_record.open_price <= 100000):  # Reasonable price range
                                    result.warnings.append(f"Suspicious price values in {instrument_id}_{year}_{month:02d}")
                        
                        except Exception as e:
                            format_errors.append(f"Format error in {instrument_id}_{year}_{month:02d}: {e}")
            
            finally:
                await pool.close()
            
            result.details = {
                'files_tested': total_files_tested,
                'successful_reads': successful_reads,
                'format_errors': len(format_errors),
                'success_rate': (successful_reads / total_files_tested * 100) if total_files_tested > 0 else 0
            }
            
            if format_errors:
                if len(format_errors) > total_files_tested * 0.1:  # >10% error rate
                    result.success = False
                    result.errors.extend(format_errors[:5])  # Show first 5 errors
                else:
                    result.warnings.extend(format_errors[:3])  # Show first 3 warnings
        
        except Exception as e:
            result.success = False
            result.errors.append(f"File format validation failed: {e}")
        
        return result
    
    async def validate_metadata_consistency(self) -> ValidationResult:
        """Validate metadata consistency across files"""
        
        result = ValidationResult(
            test_name="Metadata Consistency",
            success=True
        )
        
        try:
            pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=2)
            
            try:
                async with pool.acquire() as conn:
                    sample_instruments = await conn.fetch(
                        "SELECT id FROM dev_instruments WHERE symbol IS NOT NULL LIMIT 10"
                    )
                
                metadata_issues = []
                total_metadata_checked = 0
                
                for instrument_row in sample_instruments:
                    instrument_id = instrument_row['id']
                    available_months = await self.file_manager.list_available_data(instrument_id)
                    
                    for year, month in available_months:
                        total_metadata_checked += 1
                        metadata = await self.file_manager.get_file_metadata(instrument_id, year, month)
                        
                        if not metadata:
                            metadata_issues.append(f"No metadata for {instrument_id}_{year}_{month:02d}")
                            continue
                        
                        # Validate metadata consistency
                        if metadata.instrument_id != instrument_id:
                            metadata_issues.append(f"Instrument ID mismatch in {instrument_id}_{year}_{month:02d}")
                        
                        if metadata.year != year or metadata.month != month:
                            metadata_issues.append(f"Date mismatch in {instrument_id}_{year}_{month:02d}")
                        
                        if metadata.record_count <= 0:
                            metadata_issues.append(f"Invalid record count in {instrument_id}_{year}_{month:02d}")
                        
                        # Validate timestamp range
                        expected_start = datetime(year, month, 1)
                        expected_end = datetime(year, month + 1, 1) if month < 12 else datetime(year + 1, 1, 1)
                        
                        if metadata.first_timestamp < expected_start or metadata.last_timestamp >= expected_end:
                            result.warnings.append(f"Timestamp range outside expected month in {instrument_id}_{year}_{month:02d}")
            
            finally:
                await pool.close()
            
            result.details = {
                'metadata_files_checked': total_metadata_checked,
                'metadata_issues': len(metadata_issues),
                'consistency_rate': ((total_metadata_checked - len(metadata_issues)) / total_metadata_checked * 100) if total_metadata_checked > 0 else 100
            }
            
            if metadata_issues:
                if len(metadata_issues) > total_metadata_checked * 0.05:  # >5% error rate
                    result.success = False
                    result.errors.extend(metadata_issues[:5])
                else:
                    result.warnings.extend(metadata_issues[:3])
        
        except Exception as e:
            result.success = False
            result.errors.append(f"Metadata validation failed: {e}")
        
        return result
    
    async def validate_sample_data_content(self, 
                                         start_date: date = None,
                                         end_date: date = None, 
                                         instrument_ids: List[int] = None) -> ValidationResult:
        """Validate actual data content between database and files"""
        
        result = ValidationResult(
            test_name="Sample Data Content",
            success=True
        )
        
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=3)
        
        try:
            async with pool.acquire() as conn:
                # Get sample instruments and records
                if instrument_ids is None:
                    instrument_rows = await conn.fetch(
                        "SELECT id FROM dev_instruments WHERE symbol IS NOT NULL LIMIT 5"
                    )
                    instrument_ids = [row['id'] for row in instrument_rows]
                
                content_mismatches = []
                successful_comparisons = 0
                total_comparisons = 0
                
                for instrument_id in instrument_ids:
                    # Get sample records from database
                    for table in self.config.source_tables:
                        try:
                            table_exists = await conn.fetchval(
                                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1",
                                table
                            )
                            
                            if not table_exists:
                                continue
                            
                            # Get sample records
                            sample_query = f"""
                                SELECT timestamp, open_price, high_price, low_price, close_price, volume
                                FROM {table}
                                WHERE instrument_id = $1
                                {f"AND timestamp::date BETWEEN '{start_date}' AND '{end_date}'" if start_date and end_date else ""}
                                ORDER BY RANDOM()
                                LIMIT {min(self.config.sample_size // len(instrument_ids), 20)}
                            """
                            
                            db_records = await conn.fetch(sample_query, instrument_id)
                            
                            for db_record in db_records:
                                total_comparisons += 1
                                
                                # Get corresponding file data
                                file_start = db_record['timestamp'] - timedelta(minutes=5)
                                file_end = db_record['timestamp'] + timedelta(minutes=5)
                                
                                file_data = await self.query_engine.query_range(
                                    [instrument_id], file_start, file_end
                                )
                                
                                file_records = file_data.get(instrument_id, [])
                                matching_record = None
                                
                                # Find matching timestamp
                                for file_record in file_records:
                                    if file_record.timestamp == db_record['timestamp']:
                                        matching_record = file_record
                                        break
                                
                                if not matching_record:
                                    content_mismatches.append(f"Missing file record for {instrument_id} at {db_record['timestamp']}")
                                    continue
                                
                                # Compare values
                                fields_to_compare = [
                                    ('open_price', 'open_price'),
                                    ('high_price', 'high_price'),
                                    ('low_price', 'low_price'),
                                    ('close_price', 'close_price'),
                                    ('volume', 'volume')
                                ]
                                
                                mismatch_found = False
                                for db_field, file_field in fields_to_compare:
                                    db_value = float(db_record[db_field] or 0)
                                    file_value = getattr(matching_record, file_field, 0)
                                    
                                    if abs(db_value - file_value) > self.config.tolerance:
                                        content_mismatches.append(
                                            f"Value mismatch for {instrument_id} at {db_record['timestamp']}: "
                                            f"{db_field} DB={db_value}, File={file_value}"
                                        )
                                        mismatch_found = True
                                        break
                                
                                if not mismatch_found:
                                    successful_comparisons += 1
                        
                        except Exception as e:
                            result.warnings.append(f"Could not compare data for table {table}: {e}")
                
                result.details = {
                    'total_comparisons': total_comparisons,
                    'successful_comparisons': successful_comparisons,
                    'content_mismatches': len(content_mismatches),
                    'accuracy_rate': (successful_comparisons / total_comparisons * 100) if total_comparisons > 0 else 0
                }
                
                if content_mismatches:
                    if len(content_mismatches) > total_comparisons * 0.02:  # >2% mismatch rate
                        result.success = False
                        result.errors.extend(content_mismatches[:5])
                    else:
                        result.warnings.extend(content_mismatches[:3])
                
                if total_comparisons == 0:
                    result.success = False
                    result.errors.append("No data available for content comparison")
        
        finally:
            await pool.close()
        
        return result
    
    async def validate_storage_statistics(self) -> ValidationResult:
        """Validate storage statistics and file organization"""
        
        result = ValidationResult(
            test_name="Storage Statistics",
            success=True
        )
        
        try:
            stats = await self.file_manager.get_storage_stats()
            
            result.details = {
                'total_files': stats['total_files'],
                'total_size_gb': stats['total_size_bytes'] / (1024**3),
                'years_covered': stats['years_covered'],
                'instruments_count': stats['instruments_count'],
                'compression_ratio': stats.get('compression_ratio', 0),
                'avg_file_size_mb': (stats['total_size_bytes'] / stats['total_files'] / (1024**2)) if stats['total_files'] > 0 else 0
            }
            
            # Validate reasonable statistics
            if stats['total_files'] == 0:
                result.success = False
                result.errors.append("No files found in storage")
            elif stats['total_files'] < 100:  # Expect at least 100 files for reasonable coverage
                result.warnings.append(f"Low file count: {stats['total_files']}")
            
            if stats['total_size_bytes'] < 1024**2:  # Less than 1MB total
                result.warnings.append("Very small total storage size")
            
            if stats.get('compression_ratio', 0) > 0.8:  # Poor compression
                result.warnings.append(f"Poor compression ratio: {stats['compression_ratio']:.1%}")
        
        except Exception as e:
            result.success = False
            result.errors.append(f"Storage statistics validation failed: {e}")
        
        return result
    
    async def validate_performance(self) -> ValidationResult:
        """Compare query performance between database and files"""
        
        result = ValidationResult(
            test_name="Performance Comparison",
            success=True
        )
        
        try:
            pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=3)
            
            try:
                async with pool.acquire() as conn:
                    # Get sample instruments
                    instrument_rows = await conn.fetch(
                        f"SELECT id FROM dev_instruments WHERE symbol IS NOT NULL LIMIT {self.config.performance_test_instruments}"
                    )
                    instrument_ids = [row['id'] for row in instrument_rows]
                
                if not instrument_ids:
                    result.success = False
                    result.errors.append("No instruments found for performance test")
                    return result
                
                # Test date range
                end_date = datetime.now()
                start_date = end_date - timedelta(days=self.config.performance_test_days)
                
                # Time database query
                db_start_time = datetime.now()
                db_records_total = 0
                
                for table in self.config.source_tables:
                    try:
                        table_exists = await conn.fetchval(
                            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = $1",
                            table
                        )
                        
                        if table_exists:
                            records = await conn.fetch(f"""
                                SELECT timestamp, open_price, high_price, low_price, close_price, volume
                                FROM {table}
                                WHERE instrument_id = ANY($1)
                                AND timestamp BETWEEN $2 AND $3
                                ORDER BY instrument_id, timestamp
                            """, instrument_ids, start_date, end_date)
                            
                            db_records_total += len(records)
                    
                    except Exception as e:
                        result.warnings.append(f"Database query failed for table {table}: {e}")
                
                db_query_time = (datetime.now() - db_start_time).total_seconds()
            
            finally:
                await pool.close()
            
            # Time file query
            file_start_time = datetime.now()
            file_data = await self.query_engine.query_range(instrument_ids, start_date, end_date)
            file_records_total = sum(len(records) for records in file_data.values())
            file_query_time = (datetime.now() - file_start_time).total_seconds()
            
            result.details = {
                'instruments_tested': len(instrument_ids),
                'date_range_days': self.config.performance_test_days,
                'database_query_time': db_query_time,
                'file_query_time': file_query_time,
                'database_records': db_records_total,
                'file_records': file_records_total,
                'speedup_factor': db_query_time / file_query_time if file_query_time > 0 else float('inf'),
                'db_records_per_second': db_records_total / db_query_time if db_query_time > 0 else 0,
                'file_records_per_second': file_records_total / file_query_time if file_query_time > 0 else 0
            }
            
            # Determine performance success
            if file_query_time > db_query_time * 2:  # Files are more than 2x slower
                result.warnings.append("File queries are significantly slower than database")
            elif file_query_time < db_query_time * 0.5:  # Files are more than 2x faster
                result.details['performance_improvement'] = f"{db_query_time / file_query_time:.1f}x faster"
            
            # Check record count consistency
            if abs(db_records_total - file_records_total) > max(db_records_total * 0.05, 100):
                result.warnings.append(f"Record count difference: DB={db_records_total}, Files={file_records_total}")
        
        except Exception as e:
            result.success = False
            result.errors.append(f"Performance validation failed: {e}")
        
        return result
    
    async def generate_validation_report(self):
        """Generate comprehensive validation report"""
        
        report_path = Path("validation_report.json")
        html_report_path = Path("validation_report.html")
        
        # JSON report
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'summary': asdict(self.summary),
            'configuration': asdict(self.config)
        }
        
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        # HTML report
        html_content = self.generate_html_report()
        with open(html_report_path, 'w') as f:
            f.write(html_content)
        
        self.logger.info(f"📊 Validation reports generated:")
        self.logger.info(f"   JSON: {report_path}")
        self.logger.info(f"   HTML: {html_report_path}")
    
    def generate_html_report(self) -> str:
        """Generate HTML validation report"""
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Data Integrity Validation Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        .header {{ background: #f0f0f0; padding: 20px; margin-bottom: 20px; }}
        .summary {{ background: #e8f5e8; padding: 15px; margin-bottom: 20px; border-left: 4px solid #4caf50; }}
        .test-result {{ margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; }}
        .success {{ border-left: 4px solid #4caf50; }}
        .failure {{ border-left: 4px solid #f44336; }}
        .details {{ background: #f9f9f9; padding: 10px; margin-top: 10px; }}
        .error {{ color: #d32f2f; }}
        .warning {{ color: #f57c00; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔍 Data Integrity Validation Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <div class="summary">
        <h2>📊 Summary</h2>
        <p><strong>Total Tests:</strong> {self.summary.total_tests}</p>
        <p><strong>Passed:</strong> {self.summary.passed_tests}</p>
        <p><strong>Failed:</strong> {self.summary.failed_tests}</p>
        <p><strong>Warnings:</strong> {self.summary.warnings_count}</p>
        <p><strong>Success Rate:</strong> {(self.summary.passed_tests/self.summary.total_tests*100):.1f}%</p>
        <p><strong>Total Execution Time:</strong> {self.summary.total_execution_time:.2f}s</p>
    </div>
    
    <h2>🧪 Test Results</h2>
"""
        
        for result in self.summary.results:
            status_class = "success" if result.success else "failure"
            status_icon = "✅" if result.success else "❌"
            
            html += f"""
    <div class="test-result {status_class}">
        <h3>{status_icon} {result.test_name} ({result.execution_time:.2f}s)</h3>
"""
            
            if result.details:
                html += '<div class="details"><h4>Details:</h4><table>'
                for key, value in result.details.items():
                    html += f'<tr><td><strong>{key.replace("_", " ").title()}:</strong></td><td>{value}</td></tr>'
                html += '</table></div>'
            
            if result.errors:
                html += '<div class="error"><h4>Errors:</h4><ul>'
                for error in result.errors:
                    html += f'<li>{error}</li>'
                html += '</ul></div>'
            
            if result.warnings:
                html += '<div class="warning"><h4>Warnings:</h4><ul>'
                for warning in result.warnings:
                    html += f'<li>{warning}</li>'
                html += '</ul></div>'
            
            html += '</div>'
        
        html += """
</body>
</html>"""
        
        return html

async def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Data Integrity Validator for File-Based Migration')
    parser.add_argument('--file-path', default='/data/monthly/interval', help='File storage base path')
    parser.add_argument('--start-date', help='Start date for validation (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for validation (YYYY-MM-DD)')
    parser.add_argument('--sample-size', type=int, default=100, help='Sample size for content validation')
    parser.add_argument('--full-validation', action='store_true', help='Run comprehensive validation')
    parser.add_argument('--performance-test', action='store_true', help='Include performance comparison')
    parser.add_argument('--instruments', type=int, default=10, help='Number of instruments for performance test')
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('data_validation.log')
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Parse dates
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date() if args.start_date else None
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else None
        
        # Configure validator
        config = ValidationConfig(
            file_base_path=args.file_path,
            sample_size=args.sample_size,
            performance_test_instruments=args.instruments if args.performance_test else 0
        )
        
        validator = DataIntegrityValidator(config)
        
        # Run validation
        summary = await validator.run_all_validations(start_date, end_date)
        
        # Print final summary
        print("\n" + "="*80)
        print("🎉 VALIDATION COMPLETE")
        print("="*80)
        print(f"Total Tests: {summary.total_tests}")
        print(f"Passed: {summary.passed_tests}")
        print(f"Failed: {summary.failed_tests}")
        print(f"Warnings: {summary.warnings_count}")
        print(f"Success Rate: {(summary.passed_tests/summary.total_tests*100):.1f}%")
        print(f"Total Time: {summary.total_execution_time:.2f}s")
        print("="*80)
        
        # Exit with appropriate code
        sys.exit(0 if summary.failed_tests == 0 else 1)
    
    except Exception as e:
        logger.error(f"💥 Validation suite failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())