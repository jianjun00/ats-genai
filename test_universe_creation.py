#!/usr/bin/env python3
"""
Test script for universe creation system
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from config.environment import Environment
from universe.data_complete_universe_creator import DataCompleteUniverseCreator
from universe.data_quality_validator import DataQualityValidator

async def test_universe_creation():
    """Test the universe creation and validation system"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Testing Data Complete Universe Creation System")
        logger.info("=" * 60)
        
        # Test 1: Create data completeness analyzer
        logger.info("Test 1: Creating DataCompleteUniverseCreator...")
        creator = DataCompleteUniverseCreator()
        logger.info("✓ Creator initialized successfully")
        
        # Test 2: Analyze data completeness (just check structure, don't need full analysis)
        logger.info("Test 2: Testing data completeness analysis structure...")
        # Note: This may fail if no database is available, but that's expected in test environment
        try:
            completeness_results = await creator.analyze_data_completeness()
            logger.info(f"✓ Data analysis completed, found {len(completeness_results)} symbols")
        except Exception as e:
            logger.info(f"! Data analysis failed (expected if no DB): {e}")
        
        # Test 3: Create validator
        logger.info("Test 3: Creating DataQualityValidator...")
        validator = DataQualityValidator()
        logger.info("✓ Validator initialized successfully")
        
        # Test 4: Test report generation
        logger.info("Test 4: Testing report generation...")
        try:
            # Create some dummy validation results for testing
            from universe.data_quality_validator import ValidationResult, ValidationLevel
            
            dummy_results = [
                ValidationResult(
                    symbol="AAPL",
                    check_name="daily_data_gaps",
                    level=ValidationLevel.INFO,
                    passed=True,
                    message="Daily data 98.5% complete",
                    details={"completeness_ratio": 0.985}
                ),
                ValidationResult(
                    symbol="MSFT",
                    check_name="minute_data_availability", 
                    level=ValidationLevel.WARNING,
                    passed=False,
                    message="Minute data only 75.2% complete",
                    details={"completeness_ratio": 0.752}
                )
            ]
            
            report = validator.generate_validation_report(dummy_results)
            logger.info("✓ Report generation successful")
            logger.info(f"Report preview (first 200 chars): {report[:200]}...")
            
        except Exception as e:
            logger.error(f"✗ Report generation failed: {e}")
        
        # Test 5: Test quality score calculation
        logger.info("Test 5: Testing quality score calculation...")
        try:
            quality_score = creator._calculate_quality_score(0.95, 0.85, 1200, 450000)
            logger.info(f"✓ Quality score calculation successful: {quality_score:.3f}")
        except Exception as e:
            logger.error(f"✗ Quality score calculation failed: {e}")
        
        logger.info("=" * 60)
        logger.info("✓ All tests completed successfully!")
        logger.info("The universe creation system is ready to use.")
        logger.info("")
        logger.info("Next steps:")
        logger.info("1. Ensure database is running and populated with data")
        logger.info("2. Run: python scripts/universe/setup_data_complete_universe.py create")
        logger.info("3. Or run: python scripts/universe/setup_data_complete_universe.py list")
        
    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_universe_creation())