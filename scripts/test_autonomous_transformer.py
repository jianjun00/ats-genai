#!/usr/bin/env python3
"""
Test script for the Autonomous Driving Inspired Financial Transformer.

This script tests the complete pipeline within the Docker environment where
PyTorch and other dependencies are available.

Usage:
    python scripts/run_dev.py run --script scripts/test_autonomous_transformer.py
"""

import sys
import os
sys.path.insert(0, '/app/src')  # Ensure we can import from src

import logging
import torch
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_imports():
    """Test that all components can be imported."""
    logger.info("Testing imports...")

    try:
        from ml.models.autonomous_driving_inspired.data_preprocessing import (
            AutonomousFinanceDataLoader, MultiTimeframeProcessor,
            TimeframeConfig, AutonomousFinanceDataset
        )
        logger.info("✅ Data preprocessing imports successful")

        from ml.models.autonomous_driving_inspired.transformer_model import (
            AutonomousFinanceTransformer, TransformerConfig
        )
        logger.info("✅ Transformer model imports successful")

        from ml.models.autonomous_driving_inspired.attention_mechanisms import (
            MultiScaleAttentionLayer, AttentionConfig
        )
        logger.info("✅ Attention mechanism imports successful")

        from ml.models.autonomous_driving_inspired.training import (
            AutonomousFinanceTrainer, TrainingConfig, MultiTaskLoss
        )
        logger.info("✅ Training pipeline imports successful")

        return True

    except Exception as e:
        logger.error(f"❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_data_loading():
    """Test data loading components (mock test when real data not available)."""
    logger.info("Testing data loading components...")

    try:
        from ml.models.autonomous_driving_inspired.data_preprocessing import (
            AutonomousFinanceDataLoader, MultiTimeframeProcessor,
            TimeframeConfig, AutonomousFinanceDataset
        )

        # Test data path
        data_path = Path('/mnt/d/ats-data/training_data/83')

        if not data_path.exists():
            logger.warning(f"Real data path does not exist: {data_path}")
            logger.info("Running mock data loading test instead...")

            # Test timeframe processor with synthetic data
            timeframe_configs = [
                TimeframeConfig('5m', 52, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 1.0),
                TimeframeConfig('1h', 24, ['open', 'high', 'low', 'close', 'volume', 'vwap'], 0.6),
            ]

            processor = MultiTimeframeProcessor(timeframe_configs)
            logger.info(f"✅ Created MultiTimeframeProcessor with {len(timeframe_configs)} timeframes")

            # Create mock DataFrame
            import pandas as pd
            mock_data = {}
            for i in range(52):
                mock_data[f'5m_open_{i:03d}'] = [100.0 + i * 0.1] * 2  # 2 samples
                mock_data[f'5m_high_{i:03d}'] = [101.0 + i * 0.1] * 2
                mock_data[f'5m_low_{i:03d}'] = [99.0 + i * 0.1] * 2
                mock_data[f'5m_close_{i:03d}'] = [100.5 + i * 0.1] * 2
                mock_data[f'5m_volume_{i:03d}'] = [1000 + i * 10] * 2
                mock_data[f'5m_vwap_{i:03d}'] = [100.2 + i * 0.1] * 2

            for i in range(24):
                mock_data[f'1h_open_{i:03d}'] = [100.0 + i * 0.5] * 2
                mock_data[f'1h_high_{i:03d}'] = [101.0 + i * 0.5] * 2
                mock_data[f'1h_low_{i:03d}'] = [99.0 + i * 0.5] * 2
                mock_data[f'1h_close_{i:03d}'] = [100.5 + i * 0.5] * 2
                mock_data[f'1h_volume_{i:03d}'] = [5000 + i * 50] * 2
                mock_data[f'1h_vwap_{i:03d}'] = [100.2 + i * 0.5] * 2

            df = pd.DataFrame(mock_data)

            # Test sequence extraction
            timeframe_sequences = processor.extract_timeframe_sequences(df)
            logger.info(f"✅ Extracted sequences for timeframes: {list(timeframe_sequences.keys())}")

            for tf_name, sequences in timeframe_sequences.items():
                logger.info(f"  {tf_name}: {sequences.shape}")

            return True

        # Real data path exists - try real loading
        data_loader_factory = AutonomousFinanceDataLoader(data_path, batch_size=2, num_workers=0)
        train_loader = data_loader_factory.create_train_loader('AAPL')

        logger.info(f"✅ Created train loader successfully")
        logger.info(f"Dataset length: {len(train_loader.dataset)}")

        # Test loading a batch
        for batch_idx, batch in enumerate(train_loader):
            logger.info(f"📦 Batch {batch_idx}:")
            logger.info(f"Timeframes: {list(batch['timeframe_sequences'].keys())}")

            for tf_name, sequences in batch['timeframe_sequences'].items():
                logger.info(f"  {tf_name}: {sequences.shape}")

            if batch_idx >= 1:  # Test 2 batches
                break

        logger.info("✅ Data loading test completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Data loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_model_creation():
    """Test creating the autonomous finance transformer."""
    logger.info("Testing model creation...")

    try:
        from ml.models.autonomous_driving_inspired.transformer_model import (
            AutonomousFinanceTransformer, TransformerConfig
        )

        # Create model configuration
        config = TransformerConfig(
            d_model=128,  # Smaller for testing
            num_heads=4,
            num_layers=2,
            dropout=0.1,
            num_tasks=5,
            prediction_horizon=10
        )

        # Create model
        model = AutonomousFinanceTransformer(config)

        # Get model info
        model_info = model.get_model_info()
        logger.info(f"✅ Model created successfully:")
        logger.info(f"   Total parameters: {model_info['total_parameters']:,}")
        logger.info(f"   Model size: {model_info['model_size_mb']:.2f} MB")
        logger.info(f"   Components: {model_info['components']}")

        return True

    except Exception as e:
        logger.error(f"❌ Model creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_forward_pass():
    """Test forward pass with synthetic data."""
    logger.info("Testing forward pass...")

    try:
        from ml.models.autonomous_driving_inspired.transformer_model import (
            AutonomousFinanceTransformer, TransformerConfig
        )

        # Create model
        config = TransformerConfig(d_model=128, num_heads=4, num_layers=2)
        model = AutonomousFinanceTransformer(config)
        model.eval()

        # Create synthetic input data
        batch_size = 2
        timeframe_sequences = {
            '5m': torch.randn(batch_size, 52, 6),    # 52 bars, 6 features (OHLCV + VWAP)
            '15m': torch.randn(batch_size, 52, 6),
            '1h': torch.randn(batch_size, 24, 6),
            '1d': torch.randn(batch_size, 20, 6),
            '1w': torch.randn(batch_size, 12, 6)
        }

        # Create synthetic position data
        position_data = {}
        for tf_name, sequences in timeframe_sequences.items():
            seq_len = sequences.shape[1]
            position_data[tf_name] = {
                'timeframe_ids': torch.zeros(batch_size, seq_len, dtype=torch.long),
                'bar_indices': torch.arange(seq_len).unsqueeze(0).repeat(batch_size, 1),
                'temporal_offsets': torch.arange(seq_len, 0, -1).unsqueeze(0).repeat(batch_size, 1),
                'market_regimes': torch.zeros(batch_size, seq_len, dtype=torch.long)
            }

        # Forward pass
        with torch.no_grad():
            outputs = model(timeframe_sequences, position_data, return_attention_weights=True)

        predictions = outputs['predictions']
        attention_weights = outputs['attention_weights']

        logger.info("✅ Forward pass successful!")
        logger.info(f"Predictions generated for tasks:")
        for task_name, pred in predictions.items():
            logger.info(f"   {task_name}: {pred.shape}")

        logger.info(f"Attention weights available for {len(attention_weights)} layers")

        return True

    except Exception as e:
        logger.error(f"❌ Forward pass failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_training_setup():
    """Test training pipeline setup."""
    logger.info("Testing training setup...")

    try:
        from ml.models.autonomous_driving_inspired.transformer_model import (
            AutonomousFinanceTransformer, TransformerConfig
        )
        from ml.models.autonomous_driving_inspired.training import (
            AutonomousFinanceTrainer, TrainingConfig, MultiTaskLoss
        )

        # Create model
        model_config = TransformerConfig(d_model=64, num_heads=2, num_layers=1)  # Very small for test
        model = AutonomousFinanceTransformer(model_config)

        # Create training config
        training_config = TrainingConfig(
            num_epochs=2,  # Very short for test
            batch_size=2,
            eval_every_n_epochs=1,
            checkpoint_dir="/tmp/test_autonomous_finance",
            tensorboard_log_dir="/tmp/test_autonomous_finance_logs"
        )

        # Create loss function
        loss_fn = MultiTaskLoss(training_config)

        # Test loss computation with synthetic data
        batch_size = 2
        mock_predictions = {
            'price_movement': torch.randn(batch_size, 10, 1),
            'volatility': torch.randn(batch_size, 10, 1),
            'volume_profile': torch.randn(batch_size, 10, 1),
            'regime_change': torch.randn(batch_size, 10, 4),  # 4 classes
            'risk_assessment': torch.randn(batch_size, 10, 1)
        }

        mock_targets = {
            'price_movement': torch.randn(batch_size, 10, 1),
            'volatility': torch.randn(batch_size, 10, 1),
            'volume_profile': torch.randn(batch_size, 10, 1),
            'regime_change': torch.randint(0, 4, (batch_size, 10)),  # Class indices
            'risk_assessment': torch.randn(batch_size, 10, 1)
        }

        # Compute loss
        total_loss, individual_losses = loss_fn(mock_predictions, mock_targets, return_individual_losses=True)

        logger.info(f"✅ Training setup successful!")
        logger.info(f"Total loss: {total_loss.item():.4f}")
        logger.info(f"Individual losses: {list(individual_losses.keys())}")

        return True

    except Exception as e:
        logger.error(f"❌ Training setup failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    logger.info("🚀 Starting Autonomous Driving Inspired Financial Transformer Tests")
    logger.info("=" * 70)

    tests = [
        ("Import Test", test_imports),
        ("Data Loading Test", test_data_loading),
        ("Model Creation Test", test_model_creation),
        ("Forward Pass Test", test_forward_pass),
        ("Training Setup Test", test_training_setup)
    ]

    results = []

    for test_name, test_func in tests:
        logger.info(f"\n🧪 Running {test_name}...")
        try:
            success = test_func()
            results.append((test_name, success))
            if success:
                logger.info(f"✅ {test_name} PASSED")
            else:
                logger.error(f"❌ {test_name} FAILED")
        except Exception as e:
            logger.error(f"❌ {test_name} CRASHED: {e}")
            results.append((test_name, False))

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("📊 TEST SUMMARY:")
    passed = sum(1 for _, success in results if success)
    total = len(results)

    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"   {test_name}: {status}")

    logger.info(f"\n🎯 Overall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")

    if passed == total:
        logger.info("🎉 All tests passed! The Autonomous Finance Transformer is ready to use.")
    else:
        logger.warning(f"⚠️  {total - passed} test(s) failed. Please check the errors above.")

    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)