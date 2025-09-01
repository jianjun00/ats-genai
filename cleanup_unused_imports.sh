#!/bin/bash
# AUTOMATED UNUSED IMPORTS CLEANUP
# Generated from static analysis - Review before running!

set -e  # Exit on any error

echo "Starting unused imports cleanup..."
echo "Creating backup of modified files..."

# Create backup directory
BACKUP_DIR="cleanup_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"


# Cleaning src/intg_conftest.py
echo "Processing src/intg_conftest.py..."
cp "src/intg_conftest.py" "$BACKUP_DIR/"

sed -i "8d" "src/intg_conftest.py"  # Remove unused import: asyncpg
sed -i "7d" "src/intg_conftest.py"  # Remove unused import: auto_backup_restore_all_intg_tables

# Cleaning src/factor_interval_pb2.py
echo "Processing src/factor_interval_pb2.py..."
cp "src/factor_interval_pb2.py" "$BACKUP_DIR/"

sed -i "14d" "src/factor_interval_pb2.py"  # Remove unused import: instrument__interval__pb2

# Cleaning src/analytics_api_dynamic.py
echo "Processing src/analytics_api_dynamic.py..."
cp "src/analytics_api_dynamic.py" "$BACKUP_DIR/"

sed -i "19d" "src/analytics_api_dynamic.py"  # Remove unused import: BaseModel
sed -i "19d" "src/analytics_api_dynamic.py"  # Remove unused import: Field
sed -i "18d" "src/analytics_api_dynamic.py"  # Remove unused import: JSONResponse
sed -i "17d" "src/analytics_api_dynamic.py"  # Remove unused import: CORSMiddleware
sed -i "11d" "src/analytics_api_dynamic.py"  # Remove unused import: Dict
sed -i "11d" "src/analytics_api_dynamic.py"  # Remove unused import: List
sed -i "11d" "src/analytics_api_dynamic.py"  # Remove unused import: Optional
sed -i "11d" "src/analytics_api_dynamic.py"  # Remove unused import: Any
sed -i "8d" "src/analytics_api_dynamic.py"  # Remove unused import: asyncio

# Cleaning src/universe_state_interval_pb2.py
echo "Processing src/universe_state_interval_pb2.py..."
cp "src/universe_state_interval_pb2.py" "$BACKUP_DIR/"

sed -i "17d" "src/universe_state_interval_pb2.py"  # Remove unused import: time__duration__pb2
sed -i "16d" "src/universe_state_interval_pb2.py"  # Remove unused import: indicator__interval__pb2
sed -i "15d" "src/universe_state_interval_pb2.py"  # Remove unused import: instrument__interval__pb2
sed -i "14d" "src/universe_state_interval_pb2.py"  # Remove unused import: factor__interval__pb2

# Cleaning src/schema/registry.py
echo "Processing src/schema/registry.py..."
cp "src/schema/registry.py" "$BACKUP_DIR/"

sed -i "9d" "src/schema/registry.py"  # Remove unused import: EntitySchema
sed -i "9d" "src/schema/registry.py"  # Remove unused import: FieldDefinition
sed -i "8d" "src/schema/registry.py"  # Remove unused import: Dict
sed -i "8d" "src/schema/registry.py"  # Remove unused import: List
sed -i "8d" "src/schema/registry.py"  # Remove unused import: Optional
sed -i "8d" "src/schema/registry.py"  # Remove unused import: Any

# Cleaning src/schema/types.py
echo "Processing src/schema/types.py..."
cp "src/schema/types.py" "$BACKUP_DIR/"

sed -i "10d" "src/schema/types.py"  # Remove unused import: dataclass
sed -i "9d" "src/schema/types.py"  # Remove unused import: List
sed -i "9d" "src/schema/types.py"  # Remove unused import: Optional
sed -i "8d" "src/schema/types.py"  # Remove unused import: Enum

# Cleaning src/schema/__init__.py
echo "Processing src/schema/__init__.py..."
cp "src/schema/__init__.py" "$BACKUP_DIR/"

sed -i "10d" "src/schema/__init__.py"  # Remove unused import: INSTRUMENT_SCHEMA
sed -i "10d" "src/schema/__init__.py"  # Remove unused import: PRICE_SCHEMA
sed -i "9d" "src/schema/__init__.py"  # Remove unused import: schema_registry
sed -i "8d" "src/schema/__init__.py"  # Remove unused import: FieldType
sed -i "8d" "src/schema/__init__.py"  # Remove unused import: FieldSemantics
sed -i "8d" "src/schema/__init__.py"  # Remove unused import: FieldDefinition

# Cleaning src/schema/training_schema.py
echo "Processing src/schema/training_schema.py..."
cp "src/schema/training_schema.py" "$BACKUP_DIR/"

sed -i "14d" "src/schema/training_schema.py"  # Remove unused import: date
sed -i "10d" "src/schema/training_schema.py"  # Remove unused import: List
sed -i "10d" "src/schema/training_schema.py"  # Remove unused import: Dict
sed -i "10d" "src/schema/training_schema.py"  # Remove unused import: Any
sed -i "10d" "src/schema/training_schema.py"  # Remove unused import: Optional
sed -i "10d" "src/schema/training_schema.py"  # Remove unused import: Union
sed -i "10d" "src/schema/training_schema.py"  # Remove unused import: Tuple
sed -i "9d" "src/schema/training_schema.py"  # Remove unused import: Enum
sed -i "9d" "src/schema/training_schema.py"  # Remove unused import: auto
sed -i "8d" "src/schema/training_schema.py"  # Remove unused import: dataclass

# Cleaning src/analytics/portfolio_analytics.py
echo "Processing src/analytics/portfolio_analytics.py..."
cp "src/analytics/portfolio_analytics.py" "$BACKUP_DIR/"

sed -i "13d" "src/analytics/portfolio_analytics.py"  # Remove unused import: dataclass
sed -i "12d" "src/analytics/portfolio_analytics.py"  # Remove unused import: Dict
sed -i "12d" "src/analytics/portfolio_analytics.py"  # Remove unused import: List
sed -i "12d" "src/analytics/portfolio_analytics.py"  # Remove unused import: Optional
sed -i "12d" "src/analytics/portfolio_analytics.py"  # Remove unused import: Any

# Cleaning src/utils/db_utils.py
echo "Processing src/utils/db_utils.py..."
cp "src/utils/db_utils.py" "$BACKUP_DIR/"

sed -i "13d" "src/utils/db_utils.py"  # Remove unused import: Dict
sed -i "13d" "src/utils/db_utils.py"  # Remove unused import: List
sed -i "13d" "src/utils/db_utils.py"  # Remove unused import: Any
sed -i "13d" "src/utils/db_utils.py"  # Remove unused import: Optional
sed -i "12d" "src/utils/db_utils.py"  # Remove unused import: os

# Cleaning src/modeling/training_data_metadata.py
echo "Processing src/modeling/training_data_metadata.py..."
cp "src/modeling/training_data_metadata.py" "$BACKUP_DIR/"

sed -i "13d" "src/modeling/training_data_metadata.py"  # Remove unused import: pd
sed -i "11d" "src/modeling/training_data_metadata.py"  # Remove unused import: os
sed -i "9d" "src/modeling/training_data_metadata.py"  # Remove unused import: Enum
sed -i "8d" "src/modeling/training_data_metadata.py"  # Remove unused import: Dict
sed -i "8d" "src/modeling/training_data_metadata.py"  # Remove unused import: List
sed -i "8d" "src/modeling/training_data_metadata.py"  # Remove unused import: Optional
sed -i "8d" "src/modeling/training_data_metadata.py"  # Remove unused import: Union
sed -i "8d" "src/modeling/training_data_metadata.py"  # Remove unused import: Any
sed -i "7d" "src/modeling/training_data_metadata.py"  # Remove unused import: dataclass

# Cleaning src/modeling/multi_timeframe_signal_pipeline.py
echo "Processing src/modeling/multi_timeframe_signal_pipeline.py..."
cp "src/modeling/multi_timeframe_signal_pipeline.py" "$BACKUP_DIR/"

sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: PL
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: L11
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: H11
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Z1B
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Z2B
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: EnvelopeBot
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: EnvelopeTop
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Z5T
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Z6T
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: FiveNineSell
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: FiveNineBuy
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: FiveOneBuy
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: FiveOneSell
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: FiveTwoBuy
sed -i "18d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: FiveTwoSell
sed -i "14d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Enum
sed -i "13d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: dataclass
sed -i "11d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: List
sed -i "11d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Dict
sed -i "11d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Optional
sed -i "11d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Any
sed -i "11d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: Tuple
sed -i "10d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: timedelta
sed -i "9d" "src/modeling/multi_timeframe_signal_pipeline.py"  # Remove unused import: np

# Cleaning src/modeling/pytorch_multi_instrument_forecast.py
echo "Processing src/modeling/pytorch_multi_instrument_forecast.py..."
cp "src/modeling/pytorch_multi_instrument_forecast.py" "$BACKUP_DIR/"

sed -i "10d" "src/modeling/pytorch_multi_instrument_forecast.py"  # Remove unused import: np
sed -i "9d" "src/modeling/pytorch_multi_instrument_forecast.py"  # Remove unused import: Dataset

# Cleaning src/modeling/configurable_train_data_generator.py
echo "Processing src/modeling/configurable_train_data_generator.py..."
cp "src/modeling/configurable_train_data_generator.py" "$BACKUP_DIR/"

sed -i "31d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: TrainingDataMetadata
sed -i "31d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: FeatureType
sed -i "31d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: VisualizationType
sed -i "30d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: Environment
sed -i "29d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: RunnerCallback
sed -i "27d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: LabelConfig
sed -i "26d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: FeatureConfig
sed -i "24d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: Path
sed -i "21d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: timedelta
sed -i "20d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: dataclass
sed -i "19d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: Dict
sed -i "19d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: List
sed -i "19d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: Any
sed -i "19d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: Optional
sed -i "19d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: Tuple
sed -i "19d" "src/modeling/configurable_train_data_generator.py"  # Remove unused import: Union

# Cleaning src/modeling/factor_models.py
echo "Processing src/modeling/factor_models.py..."
cp "src/modeling/factor_models.py" "$BACKUP_DIR/"

sed -i "13d" "src/modeling/factor_models.py"  # Remove unused import: PCA
sed -i "10d" "src/modeling/factor_models.py"  # Remove unused import: dataclass
sed -i "9d" "src/modeling/factor_models.py"  # Remove unused import: datetime
sed -i "9d" "src/modeling/factor_models.py"  # Remove unused import: timedelta
sed -i "8d" "src/modeling/factor_models.py"  # Remove unused import: Dict
sed -i "8d" "src/modeling/factor_models.py"  # Remove unused import: List
sed -i "8d" "src/modeling/factor_models.py"  # Remove unused import: Optional
sed -i "8d" "src/modeling/factor_models.py"  # Remove unused import: Tuple
sed -i "8d" "src/modeling/factor_models.py"  # Remove unused import: Any

# Cleaning src/modeling/portfolio_evaluator.py
echo "Processing src/modeling/portfolio_evaluator.py..."
cp "src/modeling/portfolio_evaluator.py" "$BACKUP_DIR/"

sed -i "15d" "src/modeling/portfolio_evaluator.py"  # Remove unused import: UniverseStateManager
sed -i "12d" "src/modeling/portfolio_evaluator.py"  # Remove unused import: asyncio
sed -i "10d" "src/modeling/portfolio_evaluator.py"  # Remove unused import: dataclass
sed -i "8d" "src/modeling/portfolio_evaluator.py"  # Remove unused import: Dict
sed -i "8d" "src/modeling/portfolio_evaluator.py"  # Remove unused import: List
sed -i "8d" "src/modeling/portfolio_evaluator.py"  # Remove unused import: Optional
sed -i "8d" "src/modeling/portfolio_evaluator.py"  # Remove unused import: Tuple
sed -i "8d" "src/modeling/portfolio_evaluator.py"  # Remove unused import: Any

# Cleaning src/modeling/multi_timeframe_analyzer.py
echo "Processing src/modeling/multi_timeframe_analyzer.py..."
cp "src/modeling/multi_timeframe_analyzer.py" "$BACKUP_DIR/"

sed -i "17d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: PatternAnalysis
sed -i "15d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: UniverseStateManager
sed -i "13d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: asyncio
sed -i "11d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: Enum
sed -i "10d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: dataclass
sed -i "9d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: datetime
sed -i "9d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: timedelta
sed -i "8d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: Dict
sed -i "8d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: List
sed -i "8d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: Optional
sed -i "8d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: Tuple
sed -i "8d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: Any
sed -i "8d" "src/modeling/multi_timeframe_analyzer.py"  # Remove unused import: Union

# Cleaning src/modeling/multi_timeframe_data_collector.py
echo "Processing src/modeling/multi_timeframe_data_collector.py..."
cp "src/modeling/multi_timeframe_data_collector.py" "$BACKUP_DIR/"

sed -i "23d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: FeatureSpecification
sed -i "18d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: FeatureSpecification
sed -i "15d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: dataclass
sed -i "13d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: datetime
sed -i "13d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: timedelta
sed -i "12d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: Dict
sed -i "12d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: List
sed -i "12d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: Optional
sed -i "12d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: Tuple
sed -i "12d" "src/modeling/multi_timeframe_data_collector.py"  # Remove unused import: Any

# Cleaning src/modeling/llm_pattern_recognition.py
echo "Processing src/modeling/llm_pattern_recognition.py..."
cp "src/modeling/llm_pattern_recognition.py" "$BACKUP_DIR/"

sed -i "15d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: Enum
sed -i "10d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: dataclass
sed -i "9d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: datetime
sed -i "9d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: timedelta
sed -i "8d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: Dict
sed -i "8d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: List
sed -i "8d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: Optional
sed -i "8d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: Tuple
sed -i "8d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: Any
sed -i "8d" "src/modeling/llm_pattern_recognition.py"  # Remove unused import: Union

# Cleaning src/modeling/event_features.py
echo "Processing src/modeling/event_features.py..."
cp "src/modeling/event_features.py" "$BACKUP_DIR/"

sed -i "13d" "src/modeling/event_features.py"  # Remove unused import: UniverseStateManager
sed -i "10d" "src/modeling/event_features.py"  # Remove unused import: dataclass
sed -i "8d" "src/modeling/event_features.py"  # Remove unused import: Dict
sed -i "8d" "src/modeling/event_features.py"  # Remove unused import: List
sed -i "8d" "src/modeling/event_features.py"  # Remove unused import: Optional
sed -i "8d" "src/modeling/event_features.py"  # Remove unused import: Tuple
sed -i "8d" "src/modeling/event_features.py"  # Remove unused import: Any
sed -i "6d" "src/modeling/event_features.py"  # Remove unused import: pd

echo "Unused imports cleanup completed."
echo "Backup files stored in: $BACKUP_DIR"
echo "Please run tests to verify changes: python3 scripts/run_dev.py test"
