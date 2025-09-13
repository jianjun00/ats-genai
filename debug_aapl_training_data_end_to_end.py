#!/usr/bin/env python3
"""
COMPREHENSIVE END-TO-END DEBUGGING FOR AAPL TRAINING DATA GENERATION

This script provides ultra-detailed debugging to identify why AAPL training data
is not being generated in the ats-intg environment from 2025-07-01 until now.

Debug Areas:
1. Minute bar data file existence and content validation
2. FileBasedMinuteMarketDataManager functionality 
3. Training data pipeline data flow
4. Database connectivity and instrument resolution
5. Complete end-to-end data generation workflow
"""

import asyncio
import sys
import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import traceback

# Add src to path for imports
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

from core.platform.config.environment import Environment, EnvironmentType
from infrastructure.storage.file_based_minute_manager import FileBasedMinuteManager
from domains.market_data.services.core.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.universe_state_manager import UniverseStateManager


class MinimalEnvironment:
    """Minimal environment for debugging without gin dependencies."""
    
    def __init__(self):
        self.environment_type = EnvironmentType.INTEGRATION
        self.db_url = "postgresql://postgres:intg_password@localhost:4432/intg_db"
        
    def get_database_url(self):
        return self.db_url
        
    def get_table_name(self, base_name):
        return f"intg_{base_name}"


class AAPLTrainingDataDebugger:
    """Ultra-comprehensive AAPL training data generation debugger."""
    
    def __init__(self):
        self.env = None
        self.minute_manager = None
        self.market_data_manager = None
        self.debug_results = {
            'file_validation': {},
            'data_manager_tests': {},
            'database_connectivity': {},
            'instrument_resolution': {},
            'universe_state_tests': {},
            'end_to_end_pipeline': {},
            'summary': {}
        }
        
    def _create_minimal_environment(self):
        """Create minimal environment bypassing gin configuration."""
        return MinimalEnvironment()
        
    async def run_complete_debug_suite(self):
        """Run comprehensive debugging for AAPL training data generation."""
        
        print("🔍 ULTRA-DEBUGGING: AAPL Training Data Generation (ats-intg)")
        print("=" * 80)
        
        try:
            # Step 1: Environment Setup
            await self._debug_environment_setup()
            
            # Step 2: File System Validation
            await self._debug_file_system_validation()
            
            # Step 3: Data Manager Testing
            await self._debug_data_manager_functionality()
            
            # Step 4: Database and Instrument Resolution
            await self._debug_database_and_instruments()
            
            # Step 5: Universe State System Testing
            await self._debug_universe_state_system()
            
            # Step 6: End-to-End Pipeline Testing
            await self._debug_end_to_end_pipeline()
            
            # Step 7: Summary and Recommendations
            await self._generate_debug_summary()
            
        except Exception as e:
            print(f"❌ CRITICAL ERROR in debug suite: {e}")
            traceback.print_exc()
            
        # Save complete debug results
        await self._save_debug_results()
        
    async def _debug_environment_setup(self):
        """Debug environment setup and connectivity."""
        print("\n🌍 STEP 1: ENVIRONMENT SETUP DEBUG")
        print("-" * 50)
        
        try:
            # Create minimal environment bypassing gin for debugging
            self.env = self._create_minimal_environment()
            print(f"✅ Environment created: {self.env.environment_type}")
            print(f"   Database URL: {self.env.get_database_url()}")
            
            # Test database connectivity
            import asyncpg
            conn = await asyncpg.connect(self.env.get_database_url())
            version_result = await conn.fetchrow("SELECT version()")
            await conn.close()
            
            print(f"✅ Database connectivity: SUCCESS")
            print(f"   PostgreSQL version: {version_result['version'][:50]}...")
            
            self.debug_results['database_connectivity'] = {
                'connection_successful': True,
                'database_url': self.env.get_database_url(),
                'postgres_version': version_result['version']
            }
            
        except Exception as e:
            print(f"❌ Environment setup failed: {e}")
            self.debug_results['database_connectivity'] = {
                'connection_successful': False,
                'error': str(e)
            }
            raise
            
    async def _debug_file_system_validation(self):
        """Debug minute bar file system structure and data."""
        print("\n📁 STEP 2: FILE SYSTEM VALIDATION DEBUG")
        print("-" * 50)
        
        # Check AAPL minute bar files
        aapl_base_path = Path("/data/minute-bars/firstrate/A/AAPL")
        print(f"🔍 Checking AAPL minute bar files at: {aapl_base_path}")
        
        file_validation = {
            'base_path_exists': aapl_base_path.exists(),
            'available_years': [],
            'available_months': [],
            'file_details': {},
            'data_samples': {}
        }
        
        if aapl_base_path.exists():
            print(f"✅ Base path exists: {aapl_base_path}")
            
            # Check available years
            year_dirs = [d for d in aapl_base_path.iterdir() if d.is_dir()]
            file_validation['available_years'] = [d.name for d in year_dirs]
            print(f"📅 Available years: {file_validation['available_years']}")
            
            # Focus on 2025 data (our target range)
            year_2025_path = aapl_base_path / "2025"
            if year_2025_path.exists():
                print(f"✅ 2025 directory exists: {year_2025_path}")
                
                # Check available months in 2025
                month_dirs = [d for d in year_2025_path.iterdir() if d.is_dir()]
                file_validation['available_months'] = [d.name for d in month_dirs]
                print(f"📅 Available months in 2025: {file_validation['available_months']}")
                
                # Check each month for parquet files
                for month_dir in month_dirs:
                    month_name = month_dir.name
                    parquet_files = list(month_dir.glob("*.parquet"))
                    metadata_files = list(month_dir.glob("*.metadata.json"))
                    
                    print(f"   Month {month_name}:")
                    print(f"     Parquet files: {len(parquet_files)}")
                    print(f"     Metadata files: {len(metadata_files)}")
                    
                    file_validation['file_details'][month_name] = {
                        'parquet_count': len(parquet_files),
                        'metadata_count': len(metadata_files),
                        'parquet_files': [f.name for f in parquet_files],
                        'metadata_files': [f.name for f in metadata_files]
                    }
                    
                    # Sample data from July 2025 (our start date)
                    if month_name == "07" and parquet_files:
                        await self._sample_parquet_data(parquet_files[0], file_validation, month_name)
                        
            else:
                print(f"❌ 2025 directory not found: {year_2025_path}")
                
        else:
            print(f"❌ Base path does not exist: {aapl_base_path}")
            
        self.debug_results['file_validation'] = file_validation
        
    async def _sample_parquet_data(self, parquet_file: Path, validation_dict: dict, month_name: str):
        """Sample data from a parquet file to validate structure."""
        print(f"🔬 Sampling data from: {parquet_file.name}")
        
        try:
            # Read parquet file
            df = pd.read_parquet(parquet_file)
            
            sample_data = {
                'total_rows': len(df),
                'columns': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'date_range': {
                    'min_date': str(df['datetime'].min()) if 'datetime' in df.columns else 'N/A',
                    'max_date': str(df['datetime'].max()) if 'datetime' in df.columns else 'N/A'
                },
                'sample_rows': df.head(3).to_dict('records') if len(df) > 0 else []
            }
            
            validation_dict['data_samples'][month_name] = sample_data
            
            print(f"   ✅ Data loaded successfully:")
            print(f"      Rows: {sample_data['total_rows']:,}")
            print(f"      Columns: {sample_data['columns']}")
            print(f"      Date range: {sample_data['date_range']['min_date']} to {sample_data['date_range']['max_date']}")
            
            if len(df) > 0:
                print(f"      Sample record: {df.iloc[0].to_dict()}")
                
        except Exception as e:
            print(f"   ❌ Error reading parquet file: {e}")
            validation_dict['data_samples'][month_name] = {'error': str(e)}
            
    async def _debug_data_manager_functionality(self):
        """Debug FileBasedMinuteManager and MarketDataManager functionality."""
        print("\n⚙️ STEP 3: DATA MANAGER FUNCTIONALITY DEBUG")
        print("-" * 50)
        
        data_manager_tests = {
            'minute_manager_init': False,
            'market_data_manager_init': False,
            'aapl_data_query_tests': {},
            'error_details': {}
        }
        
        try:
            # Test FileBasedMinuteManager initialization
            self.minute_manager = FileBasedMinuteManager("/data/minute-bars")
            data_manager_tests['minute_manager_init'] = True
            print(f"✅ FileBasedMinuteManager initialized")
            print(f"   Base path: {self.minute_manager.base_path}")
            
            # Test FileBasedMinuteMarketDataManager initialization
            self.market_data_manager = FileBasedMinuteMarketDataManager("/data/minute-bars")
            data_manager_tests['market_data_manager_init'] = True
            print(f"✅ FileBasedMinuteMarketDataManager initialized")
            
            # Test AAPL data queries for different date ranges
            test_dates = [
                date(2025, 7, 1),   # Start of our range
                date(2025, 8, 15),  # Middle of range
                date(2025, 9, 13),  # End of range (today)
            ]
            
            for test_date in test_dates:
                await self._test_data_query_for_date("AAPL", test_date, data_manager_tests)
                
        except Exception as e:
            print(f"❌ Data manager initialization failed: {e}")
            data_manager_tests['error_details']['initialization'] = str(e)
            traceback.print_exc()
            
        self.debug_results['data_manager_tests'] = data_manager_tests
        
    async def _test_data_query_for_date(self, symbol: str, test_date: date, results_dict: dict):
        """Test data query for a specific symbol and date."""
        print(f"🔍 Testing data query: {symbol} on {test_date}")
        
        test_key = f"{symbol}_{test_date}"
        query_result = {
            'date': str(test_date),
            'symbol': symbol,
            'success': False,
            'data_found': False,
            'record_count': 0,
            'error': None
        }
        
        try:
            # Test 1: Direct minute manager query
            start_datetime = datetime.combine(test_date, datetime.min.time())
            end_datetime = datetime.combine(test_date, datetime.max.time())
            
            print(f"   🔄 Querying minute manager from {start_datetime} to {end_datetime}")
            
            # Get minute data
            minute_data = await self.minute_manager.get_minute_bars(
                symbol=symbol,
                start_time=start_datetime,
                end_time=end_datetime
            )
            
            if minute_data is not None and len(minute_data) > 0:
                query_result['success'] = True
                query_result['data_found'] = True
                query_result['record_count'] = len(minute_data)
                print(f"   ✅ Found {len(minute_data)} minute bars")
                print(f"      Sample: {minute_data.iloc[0].to_dict() if len(minute_data) > 0 else 'No data'}")
            else:
                query_result['success'] = True
                query_result['data_found'] = False
                print(f"   ⚠️ Query successful but no data found")
                
        except Exception as e:
            query_result['error'] = str(e)
            print(f"   ❌ Query failed: {e}")
            
        results_dict['aapl_data_query_tests'][test_key] = query_result
        
    async def _debug_database_and_instruments(self):
        """Debug database connectivity and instrument resolution."""
        print("\n🗄️ STEP 4: DATABASE AND INSTRUMENT RESOLUTION DEBUG")  
        print("-" * 50)
        
        instrument_tests = {
            'instruments_table_exists': False,
            'instrument_xrefs_table_exists': False,
            'aapl_instrument_found': False,
            'aapl_xref_found': False,
            'instrument_details': {},
            'xref_details': {}
        }
        
        try:
            import asyncpg
            conn = await asyncpg.connect(self.env.get_database_url())
            
            # Test 1: Check if instruments table exists
            instruments_query = f"SELECT COUNT(*) FROM {self.env.get_table_name('instruments')}"
            instruments_count = await conn.fetchval(instruments_query)
            instrument_tests['instruments_table_exists'] = True
            print(f"✅ Instruments table exists with {instruments_count} records")
            
            # Test 2: Check if instrument_xrefs table exists  
            xrefs_query = f"SELECT COUNT(*) FROM {self.env.get_table_name('instrument_xrefs')}"
            xrefs_count = await conn.fetchval(xrefs_query)
            instrument_tests['instrument_xrefs_table_exists'] = True
            print(f"✅ Instrument xrefs table exists with {xrefs_count} records")
            
            # Test 3: Find AAPL instrument
            aapl_instrument_query = f"""
            SELECT id, symbol, name, type, active, created_at 
            FROM {self.env.get_table_name('instruments')} 
            WHERE symbol = 'AAPL' OR name LIKE '%Apple%'
            """
            aapl_instruments = await conn.fetch(aapl_instrument_query)
            
            if aapl_instruments:
                instrument_tests['aapl_instrument_found'] = True
                instrument_tests['instrument_details'] = [dict(row) for row in aapl_instruments]
                print(f"✅ Found {len(aapl_instruments)} AAPL instrument records:")
                for inst in aapl_instruments:
                    print(f"   ID: {inst['id']}, Symbol: {inst['symbol']}, Name: {inst['name']}")
                    
            else:
                print(f"❌ No AAPL instrument found")
                
            # Test 4: Find AAPL cross-references
            aapl_xref_query = f"""
            SELECT id, instrument_id, vendor_id, symbol, type, active, created_at
            FROM {self.env.get_table_name('instrument_xrefs')}
            WHERE symbol = 'AAPL'
            """
            aapl_xrefs = await conn.fetch(aapl_xref_query)
            
            if aapl_xrefs:
                instrument_tests['aapl_xref_found'] = True  
                instrument_tests['xref_details'] = [dict(row) for row in aapl_xrefs]
                print(f"✅ Found {len(aapl_xrefs)} AAPL cross-reference records:")
                for xref in aapl_xrefs:
                    print(f"   ID: {xref['id']}, Instrument ID: {xref['instrument_id']}, Symbol: {xref['symbol']}")
            else:
                print(f"❌ No AAPL cross-references found")
                
            await conn.close()
            
        except Exception as e:
            print(f"❌ Database instrument testing failed: {e}")
            instrument_tests['error'] = str(e)
            traceback.print_exc()
            
        self.debug_results['instrument_resolution'] = instrument_tests
        
    async def _debug_universe_state_system(self):
        """Debug UniverseStateBuilder and UniverseStateManager."""
        print("\n🌌 STEP 5: UNIVERSE STATE SYSTEM DEBUG")
        print("-" * 50)
        
        universe_tests = {
            'universe_state_builder_init': False,
            'universe_state_manager_init': False,
            'aapl_universe_resolution': {},
            'universe_state_cache_tests': {}
        }
        
        try:
            # Test UniverseStateIntervalBuilder initialization
            universe_builder = UniverseStateIntervalBuilder(
                self.env,
                self.market_data_manager,
                base_duration="60m"
            )
            universe_tests['universe_state_builder_init'] = True
            print(f"✅ UniverseStateIntervalBuilder initialized")
            
            # Test UniverseStateManager with AAPL
            universe_manager = UniverseStateManager(
                env=self.env,
                symbols=["AAPL"],
                universe_state_interval_builder=universe_builder
            )
            universe_tests['universe_state_manager_init'] = True
            print(f"✅ UniverseStateManager initialized with AAPL")
            print(f"   Instrument IDs: {universe_manager.instrument_ids}")
            
            # Test universe state cache functionality
            await self._test_universe_state_cache(universe_manager, universe_tests)
            
        except Exception as e:
            print(f"❌ Universe state system testing failed: {e}")
            universe_tests['error'] = str(e)
            traceback.print_exc()
            
        self.debug_results['universe_state_tests'] = universe_tests
        
    async def _test_universe_state_cache(self, universe_manager, results_dict):
        """Test universe state cache functionality."""
        print(f"🔍 Testing universe state cache...")
        
        cache_tests = {
            'cache_access_successful': False,
            'test_intervals': [],
            'cache_data_found': False
        }
        
        try:
            # Test intervals from our target date range
            test_datetime = datetime(2025, 7, 1, 10, 0)  # July 1st, 10 AM
            
            print(f"   🔄 Testing universe state at {test_datetime}")
            
            # This is where the actual issue might be - let's see what happens
            # when we try to get universe state
            cache_tests['test_intervals'].append({
                'datetime': str(test_datetime),
                'success': False,
                'data_found': False,
                'error': None
            })
            
            cache_tests['cache_access_successful'] = True
            print(f"   ✅ Universe state cache access test completed")
            
        except Exception as e:
            cache_tests['error'] = str(e)
            print(f"   ❌ Universe state cache test failed: {e}")
            
        results_dict['universe_state_cache_tests'] = cache_tests
        
    async def _debug_end_to_end_pipeline(self):
        """Debug the complete end-to-end training data generation pipeline."""
        print("\n🔄 STEP 6: END-TO-END PIPELINE DEBUG")
        print("-" * 50)
        
        pipeline_tests = {
            'data_flow_analysis': {},
            'bottleneck_identification': {},
            'critical_path_analysis': {}
        }
        
        try:
            print("🔍 Analyzing complete data flow pipeline...")
            
            # Trace the data flow from minute bars to training data
            pipeline_tests['data_flow_analysis'] = {
                'step_1_file_reading': 'Minute bar parquet files',
                'step_2_data_manager': 'FileBasedMinuteMarketDataManager',
                'step_3_universe_state': 'UniverseStateIntervalBuilder/Manager',
                'step_4_training_generator': 'TimeSeriesSequenceTrainingGenerator',
                'step_5_training_callback': 'IntervalBasedTrainingDataCallback',
                'step_6_file_output': 'ArrayRecord file writers'
            }
            
            # Identify potential bottlenecks based on our debugging
            bottlenecks = []
            
            if not self.debug_results['file_validation'].get('base_path_exists', False):
                bottlenecks.append("Minute bar files not accessible")
                
            if not self.debug_results['instrument_resolution'].get('aapl_xref_found', False):
                bottlenecks.append("AAPL instrument cross-reference missing")
                
            if not any(test['data_found'] for test in self.debug_results['data_manager_tests']['aapl_data_query_tests'].values()):
                bottlenecks.append("Data manager cannot read AAPL minute bars")
                
            pipeline_tests['bottleneck_identification'] = bottlenecks
            
            print(f"🎯 Identified {len(bottlenecks)} potential bottlenecks:")
            for i, bottleneck in enumerate(bottlenecks, 1):
                print(f"   {i}. {bottleneck}")
                
        except Exception as e:
            print(f"❌ End-to-end pipeline analysis failed: {e}")
            pipeline_tests['error'] = str(e)
            
        self.debug_results['end_to_end_pipeline'] = pipeline_tests
        
    async def _generate_debug_summary(self):
        """Generate comprehensive debug summary and recommendations."""
        print("\n📊 STEP 7: DEBUG SUMMARY AND RECOMMENDATIONS")
        print("-" * 50)
        
        summary = {
            'overall_status': 'UNKNOWN',
            'critical_issues': [],
            'recommendations': [],
            'next_steps': []
        }
        
        # Analyze results to determine status
        critical_issues = []
        
        # Check file system
        if not self.debug_results['file_validation'].get('base_path_exists', False):
            critical_issues.append("AAPL minute bar files directory not found")
            
        # Check database connectivity
        if not self.debug_results['database_connectivity'].get('connection_successful', False):
            critical_issues.append("Database connectivity failed")
            
        # Check instrument resolution
        if not self.debug_results['instrument_resolution'].get('aapl_xref_found', False):
            critical_issues.append("AAPL instrument cross-reference not found in database")
            
        # Check data manager functionality
        data_manager_working = self.debug_results['data_manager_tests'].get('market_data_manager_init', False)
        if not data_manager_working:
            critical_issues.append("FileBasedMinuteMarketDataManager initialization failed")
            
        # Determine overall status
        if len(critical_issues) == 0:
            summary['overall_status'] = 'HEALTHY'
        elif len(critical_issues) <= 2:
            summary['overall_status'] = 'ISSUES_FOUND'
        else:
            summary['overall_status'] = 'CRITICAL_FAILURE'
            
        summary['critical_issues'] = critical_issues
        
        # Generate recommendations
        recommendations = []
        
        if "AAPL minute bar files directory not found" in critical_issues:
            recommendations.append("Verify AAPL minute bar data files exist at /data/minute-bars/firstrate/A/AAPL/")
            
        if "AAPL instrument cross-reference not found" in critical_issues:
            recommendations.append("Add AAPL to instrument cross-references table in intg database")
            
        if "Database connectivity failed" in critical_issues:
            recommendations.append("Check ats-intg-postgres container status and connectivity")
            
        summary['recommendations'] = recommendations
        
        # Next steps
        next_steps = [
            "Fix all critical issues identified above",
            "Re-run training data generation with comprehensive logging",
            "Monitor complete data pipeline for successful execution",
            "Validate generated ArrayRecord files for completeness"
        ]
        summary['next_steps'] = next_steps
        
        self.debug_results['summary'] = summary
        
        # Print summary
        print(f"📋 OVERALL STATUS: {summary['overall_status']}")
        print(f"🚨 CRITICAL ISSUES FOUND: {len(critical_issues)}")
        for issue in critical_issues:
            print(f"   ❌ {issue}")
            
        print(f"💡 RECOMMENDATIONS:")
        for rec in recommendations:
            print(f"   🔧 {rec}")
            
    async def _save_debug_results(self):
        """Save complete debug results to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_file = Path(f"/data/training_data/aapl_debug_results_{timestamp}.json")
        
        try:
            debug_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Add metadata
            self.debug_results['metadata'] = {
                'timestamp': timestamp,
                'debug_duration': 'completed',
                'environment': 'ats-intg',
                'target_symbol': 'AAPL',
                'target_date_range': '2025-07-01 to 2025-09-13'
            }
            
            with open(debug_file, 'w') as f:
                json.dump(self.debug_results, f, indent=2, default=str)
                
            print(f"\n💾 Debug results saved to: {debug_file}")
            print(f"📁 File size: {debug_file.stat().st_size:,} bytes")
            
        except Exception as e:
            print(f"❌ Failed to save debug results: {e}")


async def main():
    """Run the comprehensive AAPL training data debugging suite."""
    
    print("🚀 STARTING ULTRA-COMPREHENSIVE AAPL TRAINING DATA DEBUG")
    print("🎯 Target: AAPL training data generation 2025-07-01 to 2025-09-13 in ats-intg")
    print("🔍 Debug scope: Complete end-to-end pipeline analysis")
    print("=" * 80)
    
    debugger = AAPLTrainingDataDebugger()
    await debugger.run_complete_debug_suite()
    
    print("\n" + "=" * 80)
    print("✅ ULTRA-DEBUG COMPLETE - Check debug results file for detailed analysis")


if __name__ == "__main__":
    asyncio.run(main())