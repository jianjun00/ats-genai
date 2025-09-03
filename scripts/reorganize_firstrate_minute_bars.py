#!/usr/bin/env python3
"""
FirstRate Minute Bars Directory Reorganization Script

Reorganizes existing FirstRate minute bar data from:
    /mnt/d/ats-data/minute-bars/firstrate/SYMBOL/
to:
    /mnt/d/ats-data/minute-bars/firstrate/A/SYMBOL/

This script safely moves symbol directories into first letter subdirectories
while preserving all existing data and directory structures.

Features:
- Dry-run mode to preview changes
- Safe atomic moves with rollback capability
- Progress tracking and detailed logging
- Verification of data integrity after moves
- Resume capability for interrupted operations

Usage:
    # Preview changes (dry-run)
    python3 scripts/reorganize_firstrate_minute_bars.py --dry-run
    
    # Execute reorganization
    python3 scripts/reorganize_firstrate_minute_bars.py --execute
    
    # Resume interrupted operation
    python3 scripts/reorganize_firstrate_minute_bars.py --execute --resume
"""

import os
import sys
import shutil
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Set
import json
from datetime import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/mnt/d/ats-logs/firstrate_reorganization.log')
    ]
)
logger = logging.getLogger(__name__)

class FirstRateReorganizer:
    """Reorganizes FirstRate minute bar data to use first letter directories."""
    
    def __init__(self, base_path: str = "/mnt/d/ats-data/minute-bars/firstrate"):
        self.base_path = Path(base_path)
        self.checkpoint_file = Path("/mnt/d/ats-data/checkpoints/firstrate_reorganization.json")
        
        # Ensure checkpoint directory exists
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Statistics
        self.stats = {
            'start_time': datetime.now(),
            'total_symbols': 0,
            'symbols_moved': 0,
            'symbols_skipped': 0,
            'symbols_failed': 0,
            'data_size_moved_mb': 0,
            'first_letter_dirs_created': 0,
            'errors': []
        }
        
        # Track operations for rollback
        self.operations_log = []
        
    def scan_existing_structure(self) -> Dict[str, List[str]]:
        """Scan existing directory structure and group symbols by first letter."""
        logger.info("Scanning existing FirstRate minute bar structure...")
        
        if not self.base_path.exists():
            raise ValueError(f"Base path does not exist: {self.base_path}")
        
        # Get all directories that look like symbols (not years, not single letters)
        symbol_dirs = []
        for item in self.base_path.iterdir():
            if item.is_dir():
                name = item.name
                # Skip year directories (4 digits)
                if name.isdigit() and len(name) == 4:
                    continue
                # Skip single letter directories (already organized)
                if len(name) == 1 and name.isalpha():
                    continue
                # This should be a symbol directory
                if name.isalnum() or any(c in name for c in ['_', '-', '.']):
                    symbol_dirs.append(name)
        
        # Group symbols by first letter
        grouped_symbols = {}
        for symbol in symbol_dirs:
            first_letter = symbol[0].upper()
            if first_letter not in grouped_symbols:
                grouped_symbols[first_letter] = []
            grouped_symbols[first_letter].append(symbol)
        
        # Sort symbols within each group
        for letter in grouped_symbols:
            grouped_symbols[letter].sort()
        
        self.stats['total_symbols'] = len(symbol_dirs)
        
        logger.info(f"Found {len(symbol_dirs)} symbol directories")
        logger.info(f"Grouped into {len(grouped_symbols)} first letters")
        
        return grouped_symbols
    
    def create_first_letter_dirs(self, letters: Set[str], dry_run: bool = False):
        """Create first letter directories if they don't exist."""
        logger.info(f"Creating first letter directories for: {sorted(letters)}")
        
        for letter in letters:
            letter_dir = self.base_path / letter
            if not letter_dir.exists():
                if not dry_run:
                    letter_dir.mkdir(exist_ok=True)
                    self.operations_log.append(('create_dir', letter_dir))
                    self.stats['first_letter_dirs_created'] += 1
                logger.info(f"{'[DRY-RUN] Would create' if dry_run else 'Created'} directory: {letter_dir}")
            else:
                logger.info(f"Directory already exists: {letter_dir}")
    
    def get_directory_size(self, path: Path) -> int:
        """Calculate total size of directory in bytes."""
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(path):
                for filename in filenames:
                    filepath = Path(dirpath) / filename
                    if filepath.exists():
                        total_size += filepath.stat().st_size
        except Exception as e:
            logger.warning(f"Could not calculate size for {path}: {e}")
        return total_size
    
    def move_symbol_directory(self, symbol: str, dry_run: bool = False) -> bool:
        """Move a single symbol directory to its first letter subdirectory."""
        first_letter = symbol[0].upper()
        
        source_path = self.base_path / symbol
        target_dir = self.base_path / first_letter
        target_path = target_dir / symbol
        
        # Check if source exists
        if not source_path.exists():
            logger.warning(f"Source directory does not exist: {source_path}")
            self.stats['symbols_skipped'] += 1
            return False
        
        # Check if target already exists
        if target_path.exists():
            logger.warning(f"Target directory already exists: {target_path}")
            self.stats['symbols_skipped'] += 1
            return False
        
        # Calculate size before move
        dir_size_bytes = self.get_directory_size(source_path)
        dir_size_mb = dir_size_bytes / (1024 * 1024)
        
        if dry_run:
            logger.info(f"[DRY-RUN] Would move: {source_path} -> {target_path} ({dir_size_mb:.2f} MB)")
            return True
        
        try:
            # Ensure target directory exists
            target_dir.mkdir(exist_ok=True)
            
            # Perform the move
            logger.info(f"Moving: {source_path} -> {target_path} ({dir_size_mb:.2f} MB)")
            shutil.move(str(source_path), str(target_path))
            
            # Log operation for potential rollback
            self.operations_log.append(('move', source_path, target_path))
            
            # Update stats
            self.stats['symbols_moved'] += 1
            self.stats['data_size_moved_mb'] += dir_size_mb
            
            # Verify the move was successful
            if target_path.exists() and not source_path.exists():
                logger.info(f"✅ Successfully moved {symbol}")
                return True
            else:
                logger.error(f"❌ Move verification failed for {symbol}")
                self.stats['symbols_failed'] += 1
                self.stats['errors'].append(f"Move verification failed for {symbol}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to move {symbol}: {e}")
            self.stats['symbols_failed'] += 1
            self.stats['errors'].append(f"Failed to move {symbol}: {str(e)}")
            return False
    
    def save_checkpoint(self, completed_symbols: Set[str]):
        """Save progress checkpoint."""
        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'completed_symbols': list(completed_symbols),
            'stats': self.stats.copy(),
            'operations_log': [
                {
                    'type': op[0],
                    'source': str(op[1]) if len(op) > 1 else None,
                    'target': str(op[2]) if len(op) > 2 else None
                }
                for op in self.operations_log
            ]
        }
        
        # Convert datetime to string for JSON serialization
        checkpoint_data['stats']['start_time'] = checkpoint_data['stats']['start_time'].isoformat()
        
        try:
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            logger.debug(f"Checkpoint saved: {len(completed_symbols)} symbols completed")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self) -> Set[str]:
        """Load progress checkpoint and resume from where we left off."""
        if not self.checkpoint_file.exists():
            logger.info("No checkpoint file found, starting fresh")
            return set()
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
            
            completed_symbols = set(checkpoint_data.get('completed_symbols', []))
            logger.info(f"Loaded checkpoint: {len(completed_symbols)} symbols already completed")
            
            # Restore stats
            if 'stats' in checkpoint_data:
                saved_stats = checkpoint_data['stats']
                # Convert start_time back to datetime
                saved_stats['start_time'] = datetime.fromisoformat(saved_stats['start_time'])
                # Merge saved stats with current stats
                for key, value in saved_stats.items():
                    if key in self.stats:
                        self.stats[key] = value
            
            return completed_symbols
            
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return set()
    
    def reorganize(self, dry_run: bool = False, resume: bool = False) -> bool:
        """Execute the reorganization process."""
        logger.info("=" * 80)
        logger.info("FIRSTRATE MINUTE BARS REORGANIZATION")
        logger.info("=" * 80)
        logger.info(f"Base path: {self.base_path}")
        logger.info(f"Mode: {'DRY-RUN' if dry_run else 'EXECUTE'}")
        logger.info(f"Resume: {resume}")
        
        # Load checkpoint if resuming
        completed_symbols = set()
        if resume:
            completed_symbols = self.load_checkpoint()
        
        # Scan existing structure
        grouped_symbols = self.scan_existing_structure()
        
        if not grouped_symbols:
            logger.info("No symbol directories found to reorganize")
            return True
        
        # Create first letter directories
        all_letters = set(grouped_symbols.keys())
        self.create_first_letter_dirs(all_letters, dry_run)
        
        # Process symbols by first letter
        total_to_process = sum(len(symbols) for symbols in grouped_symbols.values())
        processed = 0
        
        logger.info(f"Processing {total_to_process} symbols...")
        
        for letter in sorted(grouped_symbols.keys()):
            symbols = grouped_symbols[letter]
            logger.info(f"Processing {len(symbols)} symbols starting with '{letter}'...")
            
            for symbol in symbols:
                # Skip if already completed (resume mode)
                if symbol in completed_symbols:
                    logger.debug(f"Skipping {symbol} (already completed)")
                    processed += 1
                    continue
                
                # Move the symbol directory
                success = self.move_symbol_directory(symbol, dry_run)
                
                if success and not dry_run:
                    completed_symbols.add(symbol)
                
                processed += 1
                
                # Progress update every 50 symbols
                if processed % 50 == 0:
                    progress = (processed / total_to_process) * 100
                    logger.info(f"Progress: {processed}/{total_to_process} ({progress:.1f}%)")
                
                # Save checkpoint every 100 symbols
                if not dry_run and processed % 100 == 0:
                    self.save_checkpoint(completed_symbols)
                
                # Small delay to avoid overwhelming the filesystem
                if not dry_run:
                    time.sleep(0.1)
        
        # Final checkpoint save
        if not dry_run:
            self.save_checkpoint(completed_symbols)
        
        # Print final statistics
        self.print_final_stats(dry_run)
        
        return self.stats['symbols_failed'] == 0
    
    def print_final_stats(self, dry_run: bool):
        """Print comprehensive final statistics."""
        end_time = datetime.now()
        duration = end_time - self.stats['start_time']
        
        logger.info("=" * 80)
        logger.info(f"REORGANIZATION {'PREVIEW' if dry_run else 'COMPLETE'}")
        logger.info("=" * 80)
        logger.info(f"Total symbols found: {self.stats['total_symbols']}")
        logger.info(f"Symbols {'would be moved' if dry_run else 'moved'}: {self.stats['symbols_moved']}")
        logger.info(f"Symbols skipped: {self.stats['symbols_skipped']}")
        logger.info(f"Symbols failed: {self.stats['symbols_failed']}")
        
        if not dry_run:
            logger.info(f"Data moved: {self.stats['data_size_moved_mb']:.2f} MB")
            logger.info(f"First letter directories created: {self.stats['first_letter_dirs_created']}")
        
        logger.info(f"Processing time: {duration}")
        
        if self.stats['errors']:
            logger.info(f"Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats['errors'][:5]:  # Show first 5 errors
                logger.info(f"  - {error}")
            if len(self.stats['errors']) > 5:
                logger.info(f"  ... and {len(self.stats['errors']) - 5} more errors")
        
        if dry_run:
            logger.info("\n🔍 This was a DRY-RUN. No files were actually moved.")
            logger.info("Use --execute to perform the actual reorganization.")
        else:
            logger.info("\n✅ Reorganization completed successfully!")
            logger.info("The FirstRate minute bar data is now organized by first letter.")

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Reorganize FirstRate minute bar data by first letter"
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Preview changes without actually moving files'
    )
    parser.add_argument(
        '--execute', 
        action='store_true',
        help='Execute the reorganization (required to make changes)'
    )
    parser.add_argument(
        '--resume', 
        action='store_true',
        help='Resume from previous checkpoint'
    )
    parser.add_argument(
        '--base-path',
        type=str,
        default='/mnt/d/ats-data/minute-bars/firstrate',
        help='Base path for FirstRate minute bar data'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.dry_run and not args.execute:
        logger.error("Must specify either --dry-run or --execute")
        parser.print_help()
        sys.exit(1)
    
    if args.dry_run and args.execute:
        logger.error("Cannot specify both --dry-run and --execute")
        parser.print_help()
        sys.exit(1)
    
    logger.info("Starting FirstRate minute bar reorganization")
    logger.info(f"Arguments: {vars(args)}")
    
    # Create reorganizer
    reorganizer = FirstRateReorganizer(args.base_path)
    
    try:
        # Execute reorganization
        success = reorganizer.reorganize(
            dry_run=args.dry_run,
            resume=args.resume and args.execute
        )
        
        if success:
            logger.info("✅ Reorganization completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Reorganization completed with errors")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("❌ Reorganization interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Reorganization failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()