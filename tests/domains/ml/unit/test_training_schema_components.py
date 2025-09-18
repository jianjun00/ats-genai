"""
Unit tests for training schema management components.

Tests individual components of the schema system including schema creation,
validation, serialization, and DAO operations.
"""

import pytest
import json
from datetime import datetime, date
from unittest.mock import AsyncMock, MagicMock, patch

from src.schema.training_schema import (
    TrainingDatasetSchema,
    FeatureSchema,
    LabelSchema,
    DatasetMetadata,
    ValidationResult,
    FeatureType,
    create_ohlcv_schema,
    create_multi_horizon_schema
)
from domains.ml.repositories.training_schema_dao import TrainingSchemaDAO, create_schema_dao


class TestFeatureSchema:
    """Test FeatureSchema dataclass and methods."""

    def test_feature_schema_creation(self):
        """Test basic feature schema creation."""
        feature = FeatureSchema(
            name="sma_20",
            feature_type=FeatureType.TECHNICAL_INDICATOR,
            data_type="float32",
            shape=[100, 1],
            description="20-period Simple Moving Average",
            validation_rules={"min_value": 0.0},
            statistics={"mean": 150.5, "std": 10.2},
            transformation_info={"normalization": "z_score"}
        )

        assert feature.name == "sma_20"
        assert feature.feature_type == FeatureType.TECHNICAL_INDICATOR
        assert feature.shape == [100, 1]
        assert feature.statistics["mean"] == 150.5
        assert feature.validation_rules["min_value"] == 0.0

    def test_feature_schema_to_dict(self):
        """Test feature schema serialization to dictionary."""
        feature = FeatureSchema(
            name="test_feature",
            feature_type=FeatureType.RETURN_SERIES,
            data_type="float32"
        )

        feature_dict = feature.to_dict()
        assert isinstance(feature_dict, dict)
        assert feature_dict["name"] == "test_feature"
        assert feature_dict["feature_type"] == "RETURN_SERIES"
        assert feature_dict["data_type"] == "float32"

    def test_feature_schema_from_dict(self):
        """Test feature schema deserialization from dictionary."""
        feature_dict = {
            "name": "test_feature",
            "feature_type": "TECHNICAL_INDICATOR",
            "data_type": "float32",
            "shape": [50, 1],
            "description": "Test feature",
            "validation_rules": {},
            "statistics": {},
            "transformation_info": {}
        }

        feature = FeatureSchema.from_dict(feature_dict)
        assert feature.name == "test_feature"
        assert feature.feature_type == FeatureType.TECHNICAL_INDICATOR
        assert feature.shape == [50, 1]


class TestLabelSchema:
    """Test LabelSchema dataclass and methods."""

    def test_label_schema_creation(self):
        """Test basic label schema creation."""
        label = LabelSchema(
            name="return_1d",
            label_type="regression",
            data_type="float32",
            shape=[100],
            description="1-day forward return",
            class_mapping={},
            statistics={"mean": 0.001, "std": 0.02}
        )

        assert label.name == "return_1d"
        assert label.label_type == "regression"
        assert label.shape == [100]
        assert label.statistics["std"] == 0.02

    def test_classification_label_schema(self):
        """Test classification label schema with class mapping."""
        label = LabelSchema(
            name="direction",
            label_type="classification",
            data_type="int32",
            class_mapping={"down": 0, "up": 1}
        )

        assert label.label_type == "classification"
        assert label.class_mapping["up"] == 1
        assert label.class_mapping["down"] == 0


class TestDatasetMetadata:
    """Test DatasetMetadata dataclass."""

    def test_metadata_creation(self):
        """Test dataset metadata creation."""
        metadata = DatasetMetadata(
            symbol="AAPL",
            additional_symbols=["MSFT", "GOOGL"],
            base_timeframe="daily",
            sequence_length=60,
            prediction_horizon=5,
            total_features=25,
            total_labels=3,
            total_samples=1000,
            date_range_start=date(2023, 1, 1),
            date_range_end=date(2023, 12, 31),
            generation_timestamp=datetime(2024, 1, 1, 12, 0, 0),
            model_type="return_prediction",
            feature_engineering_version="1.0.0"
        )

        assert metadata.symbol == "AAPL"
        assert metadata.sequence_length == 60
        assert metadata.total_features == 25
        assert metadata.date_range_start == date(2023, 1, 1)

    def test_metadata_serialization(self):
        """Test metadata serialization handles dates properly."""
        metadata = DatasetMetadata(
            symbol="TEST",
            date_range_start=date(2023, 1, 1),
            date_range_end=date(2023, 12, 31),
            generation_timestamp=datetime(2024, 1, 1, 12, 0, 0)
        )

        metadata_dict = metadata.to_dict()
        assert isinstance(metadata_dict["date_range_start"], str)
        assert isinstance(metadata_dict["generation_timestamp"], str)

        # Test round-trip serialization
        restored_metadata = DatasetMetadata.from_dict(metadata_dict)
        assert restored_metadata.symbol == "TEST"
        assert restored_metadata.date_range_start == date(2023, 1, 1)


class TestTrainingDatasetSchema:
    """Test TrainingDatasetSchema main class."""

    def test_schema_creation(self):
        """Test complete schema creation."""
        features = [
            FeatureSchema(name="feature1", feature_type=FeatureType.RETURN_SERIES),
            FeatureSchema(name="feature2", feature_type=FeatureType.TECHNICAL_INDICATOR)
        ]

        labels = [
            LabelSchema(name="return_1d", label_type="regression")
        ]

        metadata = DatasetMetadata(symbol="AAPL", total_features=2, total_labels=1)

        schema = TrainingDatasetSchema(
            schema_version="1.0.0",
            dataset_name="test_dataset",
            features=features,
            labels=labels,
            metadata=metadata
        )

        assert schema.schema_version == "1.0.0"
        assert len(schema.features) == 2
        assert len(schema.labels) == 1
        assert schema.metadata.symbol == "AAPL"

    def test_schema_hash_generation(self):
        """Test schema hash generation for consistency."""
        schema1 = TrainingDatasetSchema(
            schema_version="1.0.0",
            dataset_name="test",
            features=[FeatureSchema(name="f1", feature_type=FeatureType.RETURN_SERIES)],
            metadata=DatasetMetadata(symbol="TEST")
        )

        schema2 = TrainingDatasetSchema(
            schema_version="1.0.0",
            dataset_name="test",
            features=[FeatureSchema(name="f1", feature_type=FeatureType.RETURN_SERIES)],
            metadata=DatasetMetadata(symbol="TEST")
        )

        # Identical schemas should have same hash
        assert schema1.get_schema_hash() == schema2.get_schema_hash()

        # Different schemas should have different hashes
        schema2.features[0].name = "f2"
        assert schema1.get_schema_hash() != schema2.get_schema_hash()

    def test_schema_serialization(self):
        """Test complete schema serialization and deserialization."""
        original_schema = TrainingDatasetSchema(
            schema_version="1.0.0",
            dataset_name="test_serialization",
            features=[
                FeatureSchema(
                    name="test_feature",
                    feature_type=FeatureType.TECHNICAL_INDICATOR,
                    statistics={"mean": 100.0, "std": 15.5}
                )
            ],
            labels=[
                LabelSchema(
                    name="test_label",
                    label_type="regression",
                    class_mapping={}
                )
            ],
            metadata=DatasetMetadata(
                symbol="TEST",
                date_range_start=date(2023, 1, 1),
                generation_timestamp=datetime(2024, 1, 1, 12, 0, 0)
            )
        )

        # Serialize to dict
        schema_dict = original_schema.to_dict()
        assert isinstance(schema_dict, dict)
        assert "features" in schema_dict
        assert "labels" in schema_dict
        assert "metadata" in schema_dict

        # Deserialize back to schema
        restored_schema = TrainingDatasetSchema.from_dict(schema_dict)

        # Verify restoration
        assert restored_schema.schema_version == original_schema.schema_version
        assert restored_schema.dataset_name == original_schema.dataset_name
        assert len(restored_schema.features) == len(original_schema.features)
        assert restored_schema.features[0].name == original_schema.features[0].name
        assert restored_schema.metadata.symbol == original_schema.metadata.symbol

        # Verify hashes match (indicates identical schemas)
        assert restored_schema.get_schema_hash() == original_schema.get_schema_hash()


class TestValidationResult:
    """Test ValidationResult class."""

    def test_validation_result_creation(self):
        """Test validation result creation."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=["Minor data quality issue"],
            confidence_score=0.85,
            validation_timestamp=datetime(2024, 1, 1, 12, 0, 0)
        )

        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.warnings) == 1
        assert result.confidence_score == 0.85

    def test_validation_result_with_errors(self):
        """Test validation result with errors."""
        result = ValidationResult(
            is_valid=False,
            errors=["Critical schema mismatch", "Data type error"],
            warnings=[],
            confidence_score=0.2
        )

        assert result.is_valid is False
        assert len(result.errors) == 2
        assert result.confidence_score == 0.2


class TestSchemaFactoryFunctions:
    """Test schema factory functions."""

    def test_create_ohlcv_schema(self):
        """Test OHLCV schema creation."""
        schema = create_ohlcv_schema(
            dataset_name="test_ohlcv",
            symbol="AAPL",
            sequence_length=60,
            include_volume=True,
            technical_indicators=["sma_10", "rsi_14"]
        )

        assert schema.dataset_name == "test_ohlcv"
        assert schema.metadata.symbol == "AAPL"
        assert schema.metadata.sequence_length == 60

        # Should have OHLC + volume + technical indicators
        feature_names = [f.name for f in schema.features]
        assert "open" in feature_names
        assert "high" in feature_names
        assert "low" in feature_names
        assert "close" in feature_names
        assert "volume" in feature_names
        assert "sma_10" in feature_names
        assert "rsi_14" in feature_names

    def test_create_multi_horizon_schema(self):
        """Test multi-horizon schema creation."""
        schema = create_multi_horizon_schema(
            dataset_name="test_multi_horizon",
            symbol="MSFT",
            horizons=[1, 3, 5],
            sequence_length=30
        )

        assert schema.dataset_name == "test_multi_horizon"
        assert schema.metadata.symbol == "MSFT"
        assert schema.metadata.sequence_length == 30

        # Should have labels for each horizon
        label_names = [l.name for l in schema.labels]
        assert "return_1d" in label_names
        assert "return_3d" in label_names
        assert "return_5d" in label_names


class TestTrainingSchemaDAO:
    """Test TrainingSchemaDAO database operations."""

    @pytest.fixture
    def mock_environment(self):
        """Mock environment for DAO testing."""
        env = MagicMock()
        env.env_type.value = 'dev'
        env.get_db_connection = AsyncMock()
        return env

    @pytest.fixture
    def mock_connection(self):
        """Mock database connection."""
        conn = AsyncMock()
        return conn

    @pytest.fixture
    def sample_schema(self):
        """Sample schema for testing."""
        return TrainingDatasetSchema(
            schema_version="1.0.0",
            dataset_name="test_dao_schema",
            features=[
                FeatureSchema(name="test_feature", feature_type=FeatureType.RETURN_SERIES)
            ],
            labels=[
                LabelSchema(name="test_label", label_type="regression")
            ],
            metadata=DatasetMetadata(symbol="TEST")
        )

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dao_initialization(self, mock_environment):
        """Test DAO initialization with environment."""
        dao = TrainingSchemaDAO(mock_environment)

        assert dao.environment == mock_environment
        assert dao.datasets_table == 'dev_training_dataset'
        assert dao.registry_table == 'dev_training_schema_registry'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dao_register_schema(self, mock_environment, mock_connection, sample_schema):
        """Test schema registration in database."""
        mock_environment.get_db_connection.return_value = mock_connection
        mock_connection.fetchval.return_value = 123  # Mock registry ID

        dao = TrainingSchemaDAO(mock_environment)
        dao.conn = mock_connection

        schema_hash = await dao.register_schema(
            sample_schema,
            created_by="test_user",
            tags=["test", "sample"],
            description="Test schema"
        )

        # Verify database call was made
        mock_connection.fetchval.assert_called_once()
        call_args = mock_connection.fetchval.call_args

        # Check SQL contains expected fields
        sql = call_args[0][0]
        assert "INSERT INTO" in sql
        assert "schema_name" in sql
        assert "schema_hash" in sql
        assert "created_by" in sql

        # Verify schema hash is returned
        assert isinstance(schema_hash, str)
        assert len(schema_hash) > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dao_get_schema_by_hash(self, mock_environment, mock_connection, sample_schema):
        """Test schema retrieval by hash."""
        schema_dict = sample_schema.to_dict()
        mock_environment.get_db_connection.return_value = mock_connection
        mock_connection.fetchrow.return_value = {'schema_json': schema_dict}

        dao = TrainingSchemaDAO(mock_environment)
        dao.conn = mock_connection

        test_hash = "test_schema_hash_123"
        retrieved_schema = await dao.get_schema_by_hash(test_hash)

        # Verify database query
        mock_connection.fetchrow.assert_called_once()
        call_args = mock_connection.fetchrow.call_args
        assert test_hash in call_args[0]  # Hash should be in parameters

        # Verify schema retrieval
        assert isinstance(retrieved_schema, TrainingDatasetSchema)
        assert retrieved_schema.dataset_name == sample_schema.dataset_name

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dao_get_schema_by_name_latest(self, mock_environment, mock_connection, sample_schema):
        """Test schema retrieval by name (latest version)."""
        schema_dict = sample_schema.to_dict()
        mock_environment.get_db_connection.return_value = mock_connection
        mock_connection.fetchrow.return_value = {'schema_json': schema_dict}

        dao = TrainingSchemaDAO(mock_environment)
        dao.conn = mock_connection

        retrieved_schema = await dao.get_schema_by_name_version(
            "test_dao_schema", "latest"
        )

        # Verify latest version query
        mock_connection.fetchrow.assert_called_once()
        call_args = mock_connection.fetchrow.call_args
        sql = call_args[0][0]
        assert "ORDER BY created_at DESC" in sql
        assert "LIMIT 1" in sql

        assert retrieved_schema.dataset_name == sample_schema.dataset_name

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dao_list_schemas(self, mock_environment, mock_connection):
        """Test schema listing with filters."""
        mock_environment.get_db_connection.return_value = mock_connection
        mock_connection.fetch.return_value = [
            {
                'schema_name': 'schema1',
                'schema_version': '1.0.0',
                'schema_hash': 'hash1',
                'description': 'Test schema 1',
                'created_at': datetime.now(),
                'created_by': 'user1',
                'tags': ['test'],
                'usage_count': 5,
                'last_used_at': datetime.now()
            }
        ]

        dao = TrainingSchemaDAO(mock_environment)
        dao.conn = mock_connection

        schemas = await dao.list_schemas(
            tags=["test"],
            status="active",
            limit=10
        )

        assert len(schemas) == 1
        assert schemas[0]['schema_name'] == 'schema1'
        assert schemas[0]['usage_count'] == 5

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_dao_find_compatible_schemas(self, mock_environment, mock_connection):
        """Test finding compatible schemas by parameters."""
        mock_environment.get_db_connection.return_value = mock_connection
        mock_connection.fetch.return_value = [
            {
                'schema_name': 'compatible_schema',
                'schema_version': '1.0.0',
                'schema_hash': 'compatible_hash',
                'description': 'Compatible schema',
                'metadata': {'total_features': 25, 'sequence_length': 60},
                'usage_count': 3,
                'last_used_at': datetime.now()
            }
        ]

        dao = TrainingSchemaDAO(mock_environment)
        dao.conn = mock_connection

        compatible_schemas = await dao.find_compatible_schemas(
            feature_count=25,
            sequence_length=60,
            symbol="AAPL"
        )

        assert len(compatible_schemas) == 1
        assert compatible_schemas[0]['schema_name'] == 'compatible_schema'

        # Verify SQL query includes parameter filters
        mock_connection.fetch.assert_called_once()
        call_args = mock_connection.fetch.call_args
        sql = call_args[0][0]
        assert "total_features" in sql
        assert "sequence_length" in sql


class TestSchemaUtilityFunctions:
    """Test utility functions and convenience methods."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_create_schema_dao_factory(self):
        """Test DAO factory function."""
        with patch('src.core.dao.training_schema_core.dao.Environment') as mock_env_class:
            mock_env_instance = MagicMock()
            mock_env_class.return_value = mock_env_instance

            dao = await create_schema_dao('dev')

            assert isinstance(dao, TrainingSchemaDAO)
            mock_env_class.assert_called_once()

    def test_feature_type_enum_completeness(self):
        """Test that FeatureType enum covers all financial ML use cases."""
        # Test that all expected feature types exist
        expected_types = [
            'OHLC_INTERVALS', 'PRICE_SERIES', 'RETURN_SERIES',
            'TECHNICAL_INDICATOR', 'VOLUME_PROFILE', 'VOLATILITY_METRICS',
            'CORRELATION_MATRIX', 'MARKET_REGIME', 'SECTOR_ROTATION',
            'EVENT_INDICATOR', 'EARNINGS_METRICS', 'TEMPORAL_FEATURES',
            'CUSTOM_INDICATOR'
        ]

        feature_type_names = [ft.name for ft in FeatureType]

        for expected_type in expected_types:
            assert expected_type in feature_type_names, f"Missing feature type: {expected_type}"

    def test_schema_version_validation(self):
        """Test schema version validation and compatibility."""
        # Test valid schema versions
        valid_versions = ["1.0.0", "2.1.3", "0.9.0"]

        for version in valid_versions:
            schema = TrainingDatasetSchema(
                schema_version=version,
                dataset_name="version_test",
                metadata=DatasetMetadata(symbol="TEST")
            )
            assert schema.schema_version == version

    def test_json_serialization_compatibility(self):
        """Test JSON serialization handles all data types properly."""
        complex_schema = TrainingDatasetSchema(
            schema_version="1.0.0",
            dataset_name="complex_test",
            features=[
                FeatureSchema(
                    name="complex_feature",
                    feature_type=FeatureType.TECHNICAL_INDICATOR,
                    statistics={
                        "mean": 123.456,
                        "std": 45.789,
                        "min": -100.0,
                        "max": 500.0,
                        "null_count": 0
                    },
                    validation_rules={
                        "min_value": -1000.0,
                        "max_value": 1000.0,
                        "required": True
                    }
                )
            ],
            metadata=DatasetMetadata(
                symbol="COMPLEX",
                date_range_start=date(2020, 1, 1),
                date_range_end=date(2023, 12, 31),
                generation_timestamp=datetime(2024, 1, 1, 15, 30, 45)
            )
        )

        # Test JSON serialization
        schema_dict = complex_schema.to_dict()
        json_str = json.dumps(schema_dict, default=str)

        # Verify it can be parsed back
        parsed_dict = json.loads(json_str)
        assert isinstance(parsed_dict, dict)

        # Test deserialization
        restored_schema = TrainingDatasetSchema.from_dict(parsed_dict)
        assert restored_schema.dataset_name == complex_schema.dataset_name
        assert len(restored_schema.features) == 1