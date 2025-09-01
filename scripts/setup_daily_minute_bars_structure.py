#!/usr/bin/env python3
"""
Setup Daily 1-Minute Bar File Organization Structure

Creates the proper directory hierarchy for daily 1-minute bar files under
/mnt/d/ats-data/firstrate-data/daily/ with the format:
yyyy/mm/dd/<first_letter>/<symbol>_YYYYMMDD.parquet

This script ensures the directory structure exists and sets up proper 
permissions for the daily backfill process.

Usage:
    python3 scripts/setup_daily_minute_bars_structure.py
    python3 scripts/setup_daily_minute_bars_structure.py --years 2024,2025
    python3 scripts/setup_daily_minute_bars_structure.py --test
"""

import os
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
import argparse
import logging

logger = logging.getLogger(__name__)

class DailyMinuteBarsStructureSetup:
    """Setup directory structure for daily 1-minute bar files."""
    
    def __init__(self, base_path: str = "/mnt/d/ats-data/firstrate-data/daily"):
        self.base_path = Path(base_path)
        self.created_dirs = []
        self.skipped_dirs = []
        
    def create_year_structure(self, year: int) -> bool:
        """
        Create directory structure for a specific year.
        
        Args:
            year: Year to create structure for
            
        Returns:
            True if successful, False otherwise
        """
        try:
            year_path = self.base_path / str(year)
            year_path.mkdir(parents=True, exist_ok=True)
            
            # Create monthly directories
            for month in range(1, 13):
                month_path = year_path / f"{month:02d}"
                month_path.mkdir(exist_ok=True)
                
                # Create daily directories (for valid dates)
                # We'll create based on actual calendar days to avoid invalid dates
                try:
                    start_date = date(year, month, 1)
                    
                    # Find last day of month
                    if month == 12:
                        next_month_start = date(year + 1, 1, 1)
                    else:
                        next_month_start = date(year, month + 1, 1)
                    
                    last_day = (next_month_start - timedelta(days=1)).day
                    
                    for day in range(1, last_day + 1):
                        day_path = month_path / f"{day:02d}"
                        
                        if not day_path.exists():
                            day_path.mkdir(exist_ok=True)
                            self.created_dirs.append(str(day_path))
                        else:
                            self.skipped_dirs.append(str(day_path))
                            
                        # Create first letter directories A-Z
                        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                            letter_path = day_path / letter
                            if not letter_path.exists():
                                letter_path.mkdir(exist_ok=True)
                                self.created_dirs.append(str(letter_path))
                            else:
                                self.skipped_dirs.append(str(letter_path))
                                
                except ValueError as e:
                    logger.warning(f"Invalid date for {year}-{month}: {e}")
                    continue
                    
            logger.info(f"✅ Created directory structure for year {year}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create structure for year {year}: {e}")
            return False
            
    def create_date_range_structure(self, start_date: date, end_date: date) -> bool:
        """
        Create directory structure for a specific date range.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            True if successful, False otherwise
        """
        try:
            current_date = start_date
            dates_processed = 0
            
            while current_date <= end_date:
                year_path = self.base_path / str(current_date.year)
                month_path = year_path / f"{current_date.month:02d}"
                day_path = month_path / f"{current_date.day:02d}"
                
                # Create day directory
                if not day_path.exists():
                    day_path.mkdir(parents=True, exist_ok=True)
                    self.created_dirs.append(str(day_path))
                else:
                    self.skipped_dirs.append(str(day_path))
                    
                # Create first letter directories A-Z
                for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
                    letter_path = day_path / letter
                    if not letter_path.exists():
                        letter_path.mkdir(exist_ok=True)
                        self.created_dirs.append(str(letter_path))
                    else:
                        self.skipped_dirs.append(str(letter_path))
                        
                current_date += timedelta(days=1)
                dates_processed += 1
                
            logger.info(f"✅ Created directory structure for {dates_processed} dates")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create date range structure: {e}")
            return False
            
    def validate_structure(self, test_symbols: list = None) -> bool:
        """
        Validate the directory structure by checking key paths.
        
        Args:
            test_symbols: Optional list of symbols to test paths for
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            if test_symbols is None:
                test_symbols = ['AAPL', 'TSLA', 'SPY', 'QQQ']
                
            validation_errors = []
            
            # Test current year structure
            current_year = datetime.now().year
            test_date = date.today()
            
            for symbol in test_symbols:
                first_letter = symbol[0].upper()
                expected_path = (
                    self.base_path / 
                    str(test_date.year) / 
                    f"{test_date.month:02d}" / 
                    f"{test_date.day:02d}" / 
                    first_letter
                )
                
                if not expected_path.exists():
                    validation_errors.append(f"Missing path: {expected_path}")
                else:
                    # Test if we can create a sample file path
                    sample_file = expected_path / f"{symbol}_{test_date.strftime('%Y%m%d')}.parquet"
                    logger.debug(f"✅ Valid path structure: {sample_file}")
                    
            if validation_errors:
                logger.error(f"❌ Validation failed with {len(validation_errors)} errors:")
                for error in validation_errors:
                    logger.error(f"  {error}")
                return False
            else:
                logger.info(f"✅ Directory structure validation passed for {len(test_symbols)} test symbols")
                return True
                
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
            return False
            
    def get_stats(self) -> dict:
        """Get statistics about the setup process."""
        return {
            'base_path': str(self.base_path),
            'base_path_exists': self.base_path.exists(),
            'directories_created': len(self.created_dirs),
            'directories_skipped': len(self.skipped_dirs),
            'total_directories': len(self.created_dirs) + len(self.skipped_dirs)
        }
        
    def cleanup_empty_dirs(self) -> int:
        """
        Remove empty directories from the structure.
        
        Returns:
            Number of directories removed
        """
        removed_count = 0
        
        try:
            # Walk the structure bottom-up to remove empty dirs
            for root, dirs, files in os.walk(self.base_path, topdown=False):
                for dir_name in dirs:
                    dir_path = Path(root) / dir_name
                    try:
                        if dir_path.is_dir() and not any(dir_path.iterdir()):
                            dir_path.rmdir()
                            removed_count += 1
                            logger.debug(f"Removed empty directory: {dir_path}")
                    except OSError:
                        # Directory not empty or other error, skip
                        pass
                        
            if removed_count > 0:
                logger.info(f"🧹 Cleaned up {removed_count} empty directories")
                
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")
            
        return removed_count


def main():
    """Main function for directory structure setup."""
    parser = argparse.ArgumentParser(description='Setup Daily 1-Minute Bar Directory Structure')
    
    parser.add_argument('--base-path', default='/mnt/d/ats-data/firstrate-data/daily', 
                       help='Base path for directory structure')
    parser.add_argument('--years', help='Comma-separated list of years to create (e.g., 2024,2025)')
    parser.add_argument('--start-date', help='Start date for date range (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for date range (YYYY-MM-DD)')
    parser.add_argument('--test', action='store_true', help='Test mode - create limited structure')
    parser.add_argument('--validate', action='store_true', help='Validate existing structure only')
    parser.add_argument('--cleanup', action='store_true', help='Remove empty directories')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("="*80)
    logger.info("DAILY 1-MINUTE BAR DIRECTORY STRUCTURE SETUP")
    logger.info("="*80)
    logger.info(f"Base path: {args.base_path}")
    
    # Initialize setup
    setup = DailyMinuteBarsStructureSetup(args.base_path)
    
    try:
        # Validate only mode
        if args.validate:
            logger.info("🔍 Validating existing directory structure...")
            success = setup.validate_structure()
            stats = setup.get_stats()
            
            logger.info(f"📊 Validation Stats:")
            logger.info(f"  Base path exists: {stats['base_path_exists']}")
            
            return 0 if success else 1
            
        # Cleanup mode
        if args.cleanup:
            logger.info("🧹 Cleaning up empty directories...")
            removed = setup.cleanup_empty_dirs()
            logger.info(f"✅ Removed {removed} empty directories")
            return 0
            
        # Create structure
        success = True
        
        if args.test:
            # Test mode - create structure for next 30 days
            logger.info("🧪 Test mode: creating structure for next 30 days")
            start_date = date.today()
            end_date = start_date + timedelta(days=30)
            success = setup.create_date_range_structure(start_date, end_date)
            
        elif args.start_date and args.end_date:
            # Date range mode
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
            logger.info(f"📅 Creating structure for date range: {start_date} to {end_date}")
            success = setup.create_date_range_structure(start_date, end_date)
            
        elif args.years:
            # Specific years mode
            years = [int(y.strip()) for y in args.years.split(',')]
            logger.info(f"📅 Creating structure for years: {years}")
            
            for year in years:
                year_success = setup.create_year_structure(year)
                success = success and year_success
                
        else:
            # Default mode - create structure for current and next year
            current_year = datetime.now().year
            years = [current_year, current_year + 1]
            logger.info(f"📅 Creating structure for default years: {years}")
            
            for year in years:
                year_success = setup.create_year_structure(year)
                success = success and year_success
                
        # Get final stats
        stats = setup.get_stats()
        
        logger.info("✅ Directory structure setup completed!")
        logger.info(f"📊 Setup Stats:")
        logger.info(f"  Base path: {stats['base_path']}")
        logger.info(f"  Base path exists: {stats['base_path_exists']}")
        logger.info(f"  Directories created: {stats['directories_created']:,}")
        logger.info(f"  Directories skipped (already existed): {stats['directories_skipped']:,}")
        logger.info(f"  Total directories: {stats['total_directories']:,}")
        
        # Validate the created structure
        logger.info("🔍 Validating created structure...")
        validation_success = setup.validate_structure()
        
        if success and validation_success:
            logger.info("✅ Directory structure setup and validation completed successfully!")
            return 0
        else:
            logger.error("❌ Directory structure setup or validation failed")
            return 1
            
    except KeyboardInterrupt:
        logger.info("📤 Received keyboard interrupt")
        return 1
    except Exception as e:
        logger.error(f"❌ Setup failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())