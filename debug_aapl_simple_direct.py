#!/usr/bin/env python3
"""
DIRECT AAPL TRAINING DATA DEBUG - Bypassing Environment Issues

This script directly tests the components needed for AAPL training data generation
without relying on the gin-configured Environment setup that's causing issues.
"""

import asyncio
import sys
import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, date, timedelta
import traceback
import asyncpg

# Add src to path for imports
sys.path.insert(0, '/home/jianjun/ats-genai-admin/src')

from infrastructure.storage.file_based_minute_manager import FileBasedMinuteManager


class DirectAAPLDebugger:
    """Direct AAPL debugging without Environment dependencies."""
    
    def __init__(self):
        self.debug_results = {}
        
    async def run_direct_debug(self):
        """Run direct debugging of AAPL data components."""
        
        print("🔍 DIRECT AAPL TRAINING DATA DEBUG")
        print("🎯 Target: Identify why AAPL training data fails in ats-intg")
        print("=" * 70)
        
        # Step 1: Test file system access
        await self._test_file_system_access()
        
        # Step 2: Test database connectivity 
        await self._test_database_connectivity()
        
        # Step 3: Test minute bar data reading
        await self._test_minute_bar_reading()
        
        # Step 4: Test AAPL instrument resolution
        await self._test_aapl_instrument_resolution()
        
        # Step 5: Generate summary
        await self._generate_summary()
        
        # Save results
        await self._save_results()
        
    async def _test_file_system_access(self):
        """Test direct file system access to AAPL minute bars."""
        print("\n📁 STEP 1: FILE SYSTEM ACCESS TEST")
        print("-" * 40)
        
        file_tests = {
            'base_path_exists': False,
            'aapl_directory_exists': False,
            'year_2025_exists': False,
            'july_2025_exists': False,
            'parquet_files_found': [],
            'file_details': {}
        }
        
        # Check base path
        base_path = Path("/data/minute-bars/firstrate/A/AAPL")
        file_tests['base_path_exists'] = base_path.exists()
        print(f"Base path exists: {file_tests['base_path_exists']} - {base_path}")
        
        if base_path.exists():
            file_tests['aapl_directory_exists'] = True
            
            # Check 2025 directory
            year_2025_path = base_path / "2025"
            file_tests['year_2025_exists'] = year_2025_path.exists()
            print(f"2025 directory exists: {file_tests['year_2025_exists']} - {year_2025_path}")
            
            if year_2025_path.exists():
                # Check July 2025
                july_path = year_2025_path / "07"
                file_tests['july_2025_exists'] = july_path.exists()
                print(f"July 2025 exists: {file_tests['july_2025_exists']} - {july_path}")
                
                if july_path.exists():
                    # List parquet files
                    parquet_files = list(july_path.glob("*.parquet"))
                    file_tests['parquet_files_found'] = [f.name for f in parquet_files]
                    print(f"Parquet files found: {len(parquet_files)}")
                    
                    for pf in parquet_files:
                        print(f"  📄 {pf.name} ({pf.stat().st_size:,} bytes)")
                        
                        # Sample the data
                        try:
                            df = pd.read_parquet(pf)
                            file_tests['file_details'][pf.name] = {
                                'rows': len(df),
                                'columns': list(df.columns),
                                'date_range': {
                                    'min': str(df['datetime'].min()) if 'datetime' in df.columns else 'N/A',
                                    'max': str(df['datetime'].max()) if 'datetime' in df.columns else 'N/A'
                                },
                                'sample_row': df.iloc[0].to_dict() if len(df) > 0 else None
                            }
                            print(f"    ✅ {len(df):,} rows, date range: {file_tests['file_details'][pf.name]['date_range']['min']} to {file_tests['file_details'][pf.name]['date_range']['max']}")
                            
                        except Exception as e:
                            file_tests['file_details'][pf.name] = {'error': str(e)}
                            print(f"    ❌ Error reading: {e}")
                            
        self.debug_results['file_system'] = file_tests
        
    async def _test_database_connectivity(self):
        """Test direct database connectivity to ats-intg.""" 
        print("\n🗄️ STEP 2: DATABASE CONNECTIVITY TEST")
        print("-" * 40)
        
        db_tests = {
            'connection_successful': False,
            'postgres_version': None,
            'table_checks': {},
            'error': None
        }
        
        try:
            # Direct connection to intg database
            db_url = "postgresql://postgres:intg_password@localhost:4432/intg_db"
            print(f"Connecting to: {db_url}")
            
            conn = await asyncpg.connect(db_url)
            db_tests['connection_successful'] = True
            print("✅ Database connection successful")
            
            # Get PostgreSQL version
            version_result = await conn.fetchrow("SELECT version()")
            db_tests['postgres_version'] = version_result['version']
            print(f"PostgreSQL version: {version_result['version'][:50]}...")
            
            # Check critical tables
            tables_to_check = [
                'intg_instruments',
                'intg_instrument_xrefs',
                'intg_universe_state_interval',
                'intg_instrument_interval'
            ]
            
            for table_name in tables_to_check:
                try:
                    count_result = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                    db_tests['table_checks'][table_name] = {
                        'exists': True,
                        'row_count': count_result
                    }
                    print(f"  ✅ {table_name}: {count_result:,} rows")
                    
                except Exception as e:
                    db_tests['table_checks'][table_name] = {
                        'exists': False,
                        'error': str(e)
                    }
                    print(f"  ❌ {table_name}: {e}")
                    
            await conn.close()
            
        except Exception as e:
            db_tests['error'] = str(e)
            print(f"❌ Database connection failed: {e}")
            
        self.debug_results['database'] = db_tests
        
    async def _test_minute_bar_reading(self):
        """Test direct minute bar reading with FileBasedMinuteManager."""
        print("\n⚙️ STEP 3: MINUTE BAR READING TEST")
        print("-" * 40)
        
        minute_bar_tests = {
            'manager_init_successful': False,
            'aapl_data_queries': {},
            'error': None
        }
        
        try:
            # Initialize FileBasedMinuteManager directly
            minute_manager = FileBasedMinuteManager("/data/minute-bars")
            minute_bar_tests['manager_init_successful'] = True
            print("✅ FileBasedMinuteManager initialized")
            print(f"   Base path: {minute_manager.base_path}")
            
            # Test AAPL data queries for key dates
            test_dates = [
                date(2025, 7, 1),   # Start of our target range
                date(2025, 8, 1),   # Middle of range
                date(2025, 9, 1),   # Recent date
            ]
            
            for test_date in test_dates:
                test_key = str(test_date)
                print(f"🔍 Testing AAPL data for {test_date}")
                
                start_datetime = datetime.combine(test_date, datetime.min.time())
                end_datetime = datetime.combine(test_date, datetime.max.time())
                
                try:
                    # Query minute bars
                    minute_data = await minute_manager.get_minute_bars(
                        symbol="AAPL",
                        start_time=start_datetime,
                        end_time=end_datetime
                    )
                    
                    minute_bar_tests['aapl_data_queries'][test_key] = {
                        'success': True,
                        'data_found': minute_data is not None and len(minute_data) > 0,
                        'record_count': len(minute_data) if minute_data is not None else 0,
                        'sample_record': minute_data.iloc[0].to_dict() if minute_data is not None and len(minute_data) > 0 else None
                    }
                    
                    if minute_data is not None and len(minute_data) > 0:
                        print(f"  ✅ Found {len(minute_data):,} records")
                        print(f"  📊 Sample: {minute_data.iloc[0][['datetime', 'open', 'close', 'volume']].to_dict()}")
                    else:
                        print(f"  ⚠️ No data found for {test_date}")
                        
                except Exception as e:
                    minute_bar_tests['aapl_data_queries'][test_key] = {
                        'success': False,
                        'error': str(e)
                    }
                    print(f"  ❌ Query failed: {e}")
                    
        except Exception as e:
            minute_bar_tests['error'] = str(e)
            print(f"❌ FileBasedMinuteManager initialization failed: {e}")
            traceback.print_exc()
            
        self.debug_results['minute_bars'] = minute_bar_tests
        
    async def _test_aapl_instrument_resolution(self):
        """Test AAPL instrument resolution in intg database."""
        print("\n🔍 STEP 4: AAPL INSTRUMENT RESOLUTION TEST")
        print("-" * 40)
        
        instrument_tests = {
            'aapl_instruments_found': [],
            'aapl_xrefs_found': [],
            'resolution_successful': False
        }
        
        try:
            db_url = "postgresql://postgres:intg_password@localhost:4432/intg_db"
            conn = await asyncpg.connect(db_url)
            
            # Find AAPL instruments
            aapl_instruments_query = """
            SELECT id, symbol, name, type, active, created_at 
            FROM intg_instruments 
            WHERE symbol = 'AAPL' OR name ILIKE '%Apple%'
            """
            aapl_instruments = await conn.fetch(aapl_instruments_query)
            instrument_tests['aapl_instruments_found'] = [dict(row) for row in aapl_instruments]
            
            print(f"AAPL instruments found: {len(aapl_instruments)}")
            for inst in aapl_instruments:
                print(f"  📋 ID: {inst['id']}, Symbol: {inst['symbol']}, Name: {inst['name']}")
                
            # Find AAPL cross-references
            aapl_xrefs_query = """
            SELECT id, instrument_id, vendor_id, symbol, type, active, created_at
            FROM intg_instrument_xrefs
            WHERE symbol = 'AAPL'
            """
            aapl_xrefs = await conn.fetch(aapl_xrefs_query)
            instrument_tests['aapl_xrefs_found'] = [dict(row) for row in aapl_xrefs]
            
            print(f"AAPL cross-references found: {len(aapl_xrefs)}")
            for xref in aapl_xrefs:
                print(f"  🔗 ID: {xref['id']}, Instrument ID: {xref['instrument_id']}, Vendor: {xref['vendor_id']}")
                
            # Resolution test
            if len(aapl_instruments) > 0 and len(aapl_xrefs) > 0:
                instrument_tests['resolution_successful'] = True
                print("✅ AAPL instrument resolution should work")
            else:
                print("❌ AAPL instrument resolution will fail")
                if len(aapl_instruments) == 0:
                    print("  Missing: AAPL instrument record")
                if len(aapl_xrefs) == 0:
                    print("  Missing: AAPL cross-reference record")
                    
            await conn.close()
            
        except Exception as e:
            instrument_tests['error'] = str(e)
            print(f"❌ Instrument resolution test failed: {e}")
            
        self.debug_results['instruments'] = instrument_tests
        
    async def _generate_summary(self):
        """Generate comprehensive debug summary."""
        print("\n📊 STEP 5: DEBUG SUMMARY")
        print("-" * 40)
        
        # Analyze all test results
        issues_found = []
        recommendations = []
        
        # File system analysis
        if not self.debug_results.get('file_system', {}).get('base_path_exists', False):
            issues_found.append("AAPL minute bar directory not found")
            recommendations.append("Verify AAPL minute bar data is properly mounted at /data/minute-bars/firstrate/A/AAPL/")
            
        if not self.debug_results.get('file_system', {}).get('july_2025_exists', False):
            issues_found.append("July 2025 AAPL data directory missing")
            recommendations.append("Check if July 2025 AAPL minute bar data has been collected")
            
        # Database analysis
        if not self.debug_results.get('database', {}).get('connection_successful', False):
            issues_found.append("Cannot connect to intg database")
            recommendations.append("Check ats-intg-postgres container status")
            
        # Instrument analysis
        if len(self.debug_results.get('instruments', {}).get('aapl_xrefs_found', [])) == 0:
            issues_found.append("AAPL instrument cross-reference missing from intg database")
            recommendations.append("Add AAPL to intg_instrument_xrefs table")
            
        # Minute bar analysis
        minute_bar_success = any(
            query.get('data_found', False) 
            for query in self.debug_results.get('minute_bars', {}).get('aapl_data_queries', {}).values()
        )
        if not minute_bar_success:
            issues_found.append("FileBasedMinuteManager cannot read AAPL data")
            recommendations.append("Debug file permissions and data format compatibility")
            
        summary = {
            'total_issues_found': len(issues_found),
            'critical_issues': issues_found,
            'recommendations': recommendations,
            'root_cause_analysis': self._analyze_root_cause(issues_found)
        }
        
        self.debug_results['summary'] = summary
        
        print(f"🚨 TOTAL ISSUES FOUND: {len(issues_found)}")
        for i, issue in enumerate(issues_found, 1):
            print(f"  {i}. {issue}")
            
        print(f"\n💡 RECOMMENDATIONS:")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
            
        print(f"\n🎯 ROOT CAUSE: {summary['root_cause_analysis']}")
        
    def _analyze_root_cause(self, issues):
        """Analyze the most likely root cause based on issues found."""
        if "AAPL minute bar directory not found" in issues:
            return "Missing or improperly mounted minute bar data files"
        elif "AAPL instrument cross-reference missing" in issues:
            return "Database missing AAPL instrument configuration"
        elif "FileBasedMinuteManager cannot read AAPL data" in issues:
            return "Data reading infrastructure issue"
        elif "Cannot connect to intg database" in issues:
            return "Database connectivity issue"
        else:
            return "Multiple configuration issues preventing data access"
            
    async def _save_results(self):
        """Save debug results."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = Path(f"/data/training_data/aapl_direct_debug_{timestamp}.json")
        
        try:
            results_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(results_file, 'w') as f:
                json.dump(self.debug_results, f, indent=2, default=str)
                
            print(f"\n💾 Debug results saved: {results_file}")
            print(f"📁 File size: {results_file.stat().st_size:,} bytes")
            
        except Exception as e:
            print(f"❌ Failed to save results: {e}")


async def main():
    debugger = DirectAAPLDebugger()
    await debugger.run_direct_debug()


if __name__ == "__main__":
    asyncio.run(main())