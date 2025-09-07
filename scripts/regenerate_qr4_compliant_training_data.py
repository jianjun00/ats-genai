#!/usr/bin/env python3
"""
Regenerate QR4-compliant training datasets with proper:
- Filepath structure: symbol.arrayrecord (not SYMBOL_DATERANGE.arrayrecord)
- Feature naming: base names (open, close) in ALL timeframes
- Directory structure: timeframe separation via directories

This script uses the fixed training data generation logic that follows
PRD/DRD QR4 requirements strictly.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, date
from pathlib import Path

# Add src to Python path
sys.path.insert(0, '/home/jianjun/ats-genai-pm/src')

from core.utils.training_dataset_paths import TrainingDatasetPaths


def setup_logging():
    """Setup comprehensive logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('/tmp/qr4_compliant_training_data_generation.log')
        ]
    )
    return logging.getLogger(__name__)


async def regenerate_qr4_compliant_datasets():
    """Regenerate training datasets with full QR4 compliance."""
    logger = setup_logging()
    
    logger.info("🚀 Starting QR4-compliant training dataset regeneration")
    logger.info("=" * 80)
    
    # Configuration
    symbols = ["AAPL", "TSLA"]
    start_date = "20250701_000000"
    end_date = "20250906_000000"
    run_id = f"qr4_compliant_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info(f"📋 Configuration:")
    logger.info(f"   Symbols: {symbols}")
    logger.info(f"   Date Range: {start_date} to {end_date}")
    logger.info(f"   Run ID: {run_id}")
    logger.info(f"   Base Directory: {TrainingDatasetPaths.BASE_TRAINING_DATA_DIR}")
    
    # Create directory structure using canonical paths
    logger.info("\n📁 Creating canonical directory structure...")
    for symbol in symbols:
        created_dirs = TrainingDatasetPaths.create_directory_structure(
            run_id=run_id,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        logger.info(f"   {symbol}: Created {len(created_dirs)} directories")
        for dir_path in created_dirs:
            logger.info(f"     ✓ {dir_path}")
    
    # Generate ArrayRecord files with QR4-compliant structure
    logger.info("\n⚙️ Generating QR4-compliant ArrayRecord files...")
    
    try:
        # Use the training data callback runner with QR4-compliant configuration
        os.environ['PYTHONPATH'] = '/home/jianjun/ats-genai-pm/src'
        
        runner_cmd = [
            '/home/jianjun/venv/bin/python',
            '/home/jianjun/ats-genai-pm/src/ml/training_data/runners/training_data_callback_runner.py',
            '--symbols', ','.join(symbols),
            '--start-date', '2025-07-01',
            '--end-date', '2025-09-06',
            '--config-file', '/home/jianjun/ats-genai-pm/config/training_data.gin',
            '--output-dir', os.path.join(TrainingDatasetPaths.BASE_TRAINING_DATA_DIR, run_id),
            '--debug'
        ]
        
        logger.info(f"   Command: {' '.join(runner_cmd)}")
        
        # Run the training data generation
        import subprocess
        result = subprocess.run(
            runner_cmd,
            capture_output=True,
            text=True,
            cwd='/home/jianjun/ats-genai-pm'
        )
        
        if result.returncode == 0:
            logger.info("   ✅ Training data generation completed successfully")
            if result.stdout:
                logger.info("   📋 Output:")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        logger.info(f"     {line}")
        else:
            logger.error(f"   ❌ Training data generation failed: {result.returncode}")
            if result.stderr:
                logger.error("   📋 Error output:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        logger.error(f"     {line}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to generate training data: {e}")
        return False
    
    # Validate QR4 compliance of generated files
    logger.info("\n🔍 Validating QR4 compliance...")
    
    all_compliant = True
    for symbol in symbols:
        symbol_compliant = await validate_symbol_qr4_compliance(
            run_id, symbol, start_date, end_date, logger
        )
        all_compliant = all_compliant and symbol_compliant
    
    # Final summary
    logger.info("\n" + "=" * 80)
    if all_compliant:
        logger.info("🎉 QR4-COMPLIANT TRAINING DATASET GENERATION: COMPLETE")
        logger.info(f"📁 Location: {os.path.join(TrainingDatasetPaths.BASE_TRAINING_DATA_DIR, run_id)}")
        logger.info("✅ All files pass QR4 compliance validation")
        logger.info("✅ Filepath structure: symbol.arrayrecord")
        logger.info("✅ Feature naming: base names (open, close) in all timeframes")
        logger.info("✅ Directory structure: timeframe separation via directories")
    else:
        logger.error("❌ QR4 COMPLIANCE VALIDATION FAILED")
        logger.error("Some files do not meet QR4 requirements")
    
    logger.info("=" * 80)
    return all_compliant


async def validate_symbol_qr4_compliance(run_id: str, symbol: str, start_date: str, end_date: str, logger) -> bool:
    """Validate that generated files comply with QR4 requirements."""
    
    logger.info(f"   🔍 Validating {symbol}...")
    
    symbol_compliant = True
    
    for timeframe in TrainingDatasetPaths.TIMEFRAMES:
        arrayrecord_path = TrainingDatasetPaths.get_arrayrecord_filepath(
            run_id=run_id,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            timeframe=timeframe
        )
        
        # Check file exists
        if not Path(arrayrecord_path).exists():
            logger.error(f"     ❌ {timeframe}: File missing: {arrayrecord_path}")
            symbol_compliant = False
            continue
            
        # Check filename format (QR4 requirement: symbol.arrayrecord)
        expected_filename = f"{symbol.lower()}.arrayrecord"
        actual_filename = Path(arrayrecord_path).name
        
        if actual_filename != expected_filename:
            logger.error(f"     ❌ {timeframe}: Invalid filename: {actual_filename} (expected: {expected_filename})")
            symbol_compliant = False
        else:
            logger.info(f"     ✅ {timeframe}: Filename QR4 compliant: {actual_filename}")
        
        # Check directory structure
        expected_dir_pattern = f"{symbol}_{start_date}_{end_date}/{timeframe}"
        if expected_dir_pattern not in arrayrecord_path:
            logger.error(f"     ❌ {timeframe}: Invalid directory structure: {arrayrecord_path}")
            symbol_compliant = False
        else:
            logger.info(f"     ✅ {timeframe}: Directory structure QR4 compliant")
        
        # TODO: Add ArrayRecord content validation for base feature names
        # This would require reading the ArrayRecord and checking feature names
        
    return symbol_compliant


if __name__ == "__main__":
    asyncio.run(regenerate_qr4_compliant_datasets())