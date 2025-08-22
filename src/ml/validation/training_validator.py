"""
Training Validation System

Ensures that model training is performed correctly with real data
and prevents fake/mock training from being used in production.
"""

import os
import sys
import asyncio
import logging
from typing import Dict, List, Tuple, Optional
from datetime import date, datetime
import asyncpg
from pathlib import Path

class TrainingValidationError(Exception):
    """Raised when training validation fails"""
    pass

class ProductionTrainingValidator:
    """
    Validates that model training meets production standards and
    prevents fake/mock training from being deployed.
    """
    
    def __init__(self, environment: str = None):
        self.environment = environment or os.environ.get('ENVIRONMENT', 'local')
        self.logger = logging.getLogger(__name__)
        
        # Production validation requirements
        self.min_training_examples = 10000  # Minimum real examples
        self.min_instruments = 100         # Minimum instruments with data
        self.min_training_period_days = 365 # Minimum 1 year of data
        self.required_features = 5          # Minimum feature count
        
    async def validate_training_environment(self) -> bool:
        """Validate that we're in a proper training environment"""
        
        self.logger.info("🔍 Validating training environment...")
        
        # Check 1: Environment validation
        if self.environment == 'local':
            raise TrainingValidationError(
                "❌ BLOCKED: Local training not allowed for production models. "
                "Use Kubernetes/cloud environment for real training."
            )
        
        # Check 2: Kubernetes/container validation
        if not self._is_running_in_kubernetes():
            raise TrainingValidationError(
                "❌ BLOCKED: Training must run in Kubernetes environment with proper resources."
            )
        
        # Check 3: Database connectivity validation
        db_accessible = await self._validate_database_access()
        if not db_accessible:
            raise TrainingValidationError(
                "❌ BLOCKED: Cannot access production database. Real training requires database connectivity."
            )
        
        self.logger.info("✅ Environment validation passed")
        return True
    
    async def validate_training_data(self, training_data: List[Dict]) -> bool:
        """Validate that training data is real and sufficient"""
        
        self.logger.info(f"🔍 Validating training data ({len(training_data)} examples)...")
        
        # Check 1: Minimum example count
        if len(training_data) < self.min_training_examples:
            raise TrainingValidationError(
                f"❌ BLOCKED: Insufficient training data. "
                f"Found {len(training_data)} examples, minimum required: {self.min_training_examples}"
            )
        
        # Check 2: Data diversity (number of unique symbols)
        unique_symbols = set()
        for example in training_data:
            if 'symbol' in example:
                unique_symbols.add(example['symbol'])
        
        if len(unique_symbols) < self.min_instruments:
            raise TrainingValidationError(
                f"❌ BLOCKED: Insufficient instrument diversity. "
                f"Found {len(unique_symbols)} symbols, minimum required: {self.min_instruments}"
            )
        
        # Check 3: Feature validation
        if training_data:
            example_features = training_data[0].get('features', [])
            if len(example_features) < self.required_features:
                raise TrainingValidationError(
                    f"❌ BLOCKED: Insufficient features. "
                    f"Found {len(example_features)} features, minimum required: {self.required_features}"
                )
        
        # Check 4: Date range validation
        dates = []
        for example in training_data:
            if 'date' in example:
                if isinstance(example['date'], str):
                    dates.append(datetime.fromisoformat(example['date']).date())
                elif isinstance(example['date'], date):
                    dates.append(example['date'])
        
        if dates:
            date_range = (max(dates) - min(dates)).days
            if date_range < self.min_training_period_days:
                raise TrainingValidationError(
                    f"❌ BLOCKED: Insufficient date range. "
                    f"Found {date_range} days, minimum required: {self.min_training_period_days}"
                )
        
        # Check 5: Data reality validation (detect mock/fake data)
        await self._validate_data_reality(training_data[:100])  # Sample validation
        
        self.logger.info(f"✅ Training data validation passed")
        self.logger.info(f"   📊 Examples: {len(training_data):,}")
        self.logger.info(f"   🎯 Instruments: {len(unique_symbols):,}")
        self.logger.info(f"   📅 Date Range: {date_range} days")
        self.logger.info(f"   🧮 Features: {len(example_features)}")
        
        return True
    
    async def validate_model_training(self, model_results: Dict) -> bool:
        """Validate that model training was real and meets quality standards"""
        
        self.logger.info("🔍 Validating model training results...")
        
        # Check 1: Model existence validation
        if 'support_model' not in model_results or 'resistance_model' not in model_results:
            raise TrainingValidationError(
                "❌ BLOCKED: Missing trained models. Real training must produce actual model objects."
            )
        
        # Check 2: Performance validation
        required_metrics = ['support_mae', 'resistance_mae', 'support_r2', 'resistance_r2']
        for metric in required_metrics:
            if metric not in model_results:
                raise TrainingValidationError(
                    f"❌ BLOCKED: Missing performance metric: {metric}. "
                    "Real training must compute actual performance metrics."
                )
        
        # Check 3: Quality thresholds
        support_mae = model_results.get('support_mae', 1.0)
        resistance_mae = model_results.get('resistance_mae', 1.0)
        
        if support_mae > 0.1:  # MAE should be reasonable
            raise TrainingValidationError(
                f"❌ BLOCKED: Support MAE too high: {support_mae:.4f}. "
                "This indicates fake/random predictions."
            )
        
        if resistance_mae > 0.1:
            raise TrainingValidationError(
                f"❌ BLOCKED: Resistance MAE too high: {resistance_mae:.4f}. "
                "This indicates fake/random predictions."
            )
        
        # Check 4: Model complexity validation
        support_model = model_results['support_model']
        resistance_model = model_results['resistance_model']
        
        # Validate model types (should be actual sklearn models)
        if not hasattr(support_model, 'predict'):
            raise TrainingValidationError(
                "❌ BLOCKED: Invalid support model. Must be a trained ML model with predict method."
            )
        
        if not hasattr(resistance_model, 'predict'):
            raise TrainingValidationError(
                "❌ BLOCKED: Invalid resistance model. Must be a trained ML model with predict method."
            )
        
        # Check 5: Training sample validation
        training_samples = model_results.get('training_samples', 0)
        if training_samples < self.min_training_examples:
            raise TrainingValidationError(
                f"❌ BLOCKED: Insufficient training samples: {training_samples}. "
                f"Minimum required: {self.min_training_examples}"
            )
        
        self.logger.info("✅ Model training validation passed")
        self.logger.info(f"   📈 Support MAE: {support_mae:.4f}")
        self.logger.info(f"   📈 Resistance MAE: {resistance_mae:.4f}")
        self.logger.info(f"   📊 Training Samples: {training_samples:,}")
        
        return True
    
    async def validate_deployment_readiness(self, model_path: Path, training_report: Dict) -> bool:
        """Validate that model is ready for production deployment"""
        
        self.logger.info("🔍 Validating deployment readiness...")
        
        # Check 1: Model file existence and size
        if not model_path.exists():
            raise TrainingValidationError(
                f"❌ BLOCKED: Model file does not exist: {model_path}"
            )
        
        model_size_mb = model_path.stat().st_size / (1024 * 1024)
        if model_size_mb < 1.0:  # Too small indicates fake model
            raise TrainingValidationError(
                f"❌ BLOCKED: Model file too small: {model_size_mb:.1f}MB. "
                "Real models should be larger than 1MB."
            )
        
        if model_size_mb > 500.0:  # Too large indicates potential issue
            self.logger.warning(f"⚠️  Large model file: {model_size_mb:.1f}MB")
        
        # Check 2: Training report validation
        required_report_fields = [
            'training_id', 'training_timestamp', 'data_summary', 
            'validation_results', 'model_artifacts'
        ]
        
        for field in required_report_fields:
            if field not in training_report:
                raise TrainingValidationError(
                    f"❌ BLOCKED: Missing training report field: {field}"
                )
        
        # Check 3: Validation results check
        validation_results = training_report.get('validation_results', {})
        overall_accuracy = validation_results.get('overall_accuracy', 0.0)
        
        if overall_accuracy < 0.3:  # Minimum accuracy threshold
            raise TrainingValidationError(
                f"❌ BLOCKED: Model accuracy too low: {overall_accuracy:.3f}. "
                "Minimum accuracy for deployment: 0.3"
            )
        
        # Check 4: Environment-specific validation
        if self.environment == 'prod':
            # Stricter validation for production
            if overall_accuracy < 0.5:
                raise TrainingValidationError(
                    f"❌ BLOCKED: Production deployment requires accuracy ≥ 0.5. "
                    f"Current: {overall_accuracy:.3f}"
                )
        
        self.logger.info("✅ Deployment readiness validation passed")
        self.logger.info(f"   💾 Model Size: {model_size_mb:.1f}MB")
        self.logger.info(f"   🎯 Accuracy: {overall_accuracy:.3f}")
        
        return True
    
    def _is_running_in_kubernetes(self) -> bool:
        """Check if running in Kubernetes environment"""
        
        # Check for Kubernetes service account
        if Path('/var/run/secrets/kubernetes.io/serviceaccount').exists():
            return True
        
        # Check for Kubernetes environment variables
        k8s_env_vars = [
            'KUBERNETES_SERVICE_HOST',
            'KUBERNETES_SERVICE_PORT',
            'KUBERNETES_PORT'
        ]
        
        if any(var in os.environ for var in k8s_env_vars):
            return True
        
        # Check for container environment
        if Path('/.dockerenv').exists():
            return True
        
        return False
    
    async def _validate_database_access(self) -> bool:
        """Validate database connectivity and data access"""
        
        try:
            # Get database URL from environment
            db_url = f"postgresql://{os.environ.get('DB_USER', 'postgres')}:{os.environ.get('DB_PASSWORD', 'password')}@{os.environ.get('DB_HOST', 'localhost')}:{os.environ.get('DB_PORT', '5432')}/{os.environ.get('DB_NAME', 'dev_db')}"
            
            # Test connection
            conn = await asyncpg.connect(db_url)
            
            # Test data access
            instrument_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            price_count = await conn.fetchval("SELECT COUNT(*) FROM dev_daily_prices")
            
            await conn.close()
            
            if instrument_count < 100:
                raise TrainingValidationError(
                    f"❌ BLOCKED: Insufficient instruments in database: {instrument_count}"
                )
            
            if price_count < 10000:
                raise TrainingValidationError(
                    f"❌ BLOCKED: Insufficient price data in database: {price_count}"
                )
            
            self.logger.info(f"✅ Database validation passed ({instrument_count:,} instruments, {price_count:,} price records)")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Database validation failed: {e}")
            return False
    
    async def _validate_data_reality(self, sample_data: List[Dict]) -> bool:
        """Validate that data appears to be real market data, not fake/mock"""
        
        if not sample_data:
            return True
        
        # Check for obvious fake patterns
        for example in sample_data:
            features = example.get('features', [])
            
            # Check for all-zero features (fake data indicator)
            if all(f == 0.0 for f in features):
                raise TrainingValidationError(
                    "❌ BLOCKED: Detected fake data (all-zero features). "
                    "Real market data should have non-zero variation."
                )
            
            # Check for repeated identical values (fake data indicator)
            if len(set(features)) == 1 and len(features) > 1:
                raise TrainingValidationError(
                    "❌ BLOCKED: Detected fake data (identical feature values). "
                    "Real market data should have variation."
                )
            
            # Check for unrealistic values
            for feature in features:
                if abs(feature) > 100:  # Unrealistic feature values
                    raise TrainingValidationError(
                        f"❌ BLOCKED: Detected unrealistic feature value: {feature}. "
                        "Real market features should be normalized."
                    )
        
        return True


# Convenience function for easy validation
async def validate_production_training(
    training_data: List[Dict],
    model_results: Dict,
    model_path: Path,
    training_report: Dict,
    environment: str = None
) -> bool:
    """
    Complete validation pipeline for production model training.
    
    Raises TrainingValidationError if any validation fails.
    Returns True if all validations pass.
    """
    
    validator = ProductionTrainingValidator(environment)
    
    # Run all validations
    await validator.validate_training_environment()
    await validator.validate_training_data(training_data)
    await validator.validate_model_training(model_results)
    await validator.validate_deployment_readiness(model_path, training_report)
    
    logging.getLogger(__name__).info("🎉 ALL PRODUCTION TRAINING VALIDATIONS PASSED!")
    
    return True