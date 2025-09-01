#!/usr/bin/env python3
"""
Register the generated AAPL training dataset in the training_datasets table
so the analytics service can find and visualize it.
"""

import asyncio
import sys
import os
import json
import asyncpg
from datetime import datetime, date
from pathlib import Path

# Add src to path
sys.path.append('src')

# Import environment and database dependencies without gin
from config.environment import Environment, EnvironmentType

async def register_training_dataset():
    """Register the AAPL training dataset in the database."""
    
    # Use host path since we're running outside container
    metadata_path = "/mnt/d/ats-data/training/aapl_2000_2025/AAPL_metadata.json"
    
    if not os.path.exists(metadata_path):
        print(f"❌ Metadata file not found: {metadata_path}")
        return 1
    
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    
    print(f"📋 Registering training dataset from metadata: {metadata_path}")
    print(f"   Symbol: {metadata['symbol']}")
    print(f"   Sequences: {metadata['num_sequences']}")
    print(f"   Features: {metadata['num_features']}")
    print(f"   Date range: {metadata['date_range']}")
    
    # Calculate file sizes (host path)
    base_path = Path("/mnt/d/ats-data/training/aapl_2000_2025")
    features_size = (base_path / "AAPL_features.npy").stat().st_size / (1024 * 1024)  # MB
    labels_size = (base_path / "AAPL_labels.npy").stat().st_size / (1024 * 1024)  # MB
    total_size_mb = features_size + labels_size
    
    # Connect directly to database without Environment class (avoids gin issues)
    db_url = "postgresql://postgres:dev_password@localhost:3432/dev_db"
    conn = await asyncpg.connect(db_url)
    
    try:
        # First create a run record
        run_query = """
        INSERT INTO dev_runs (
            run_type, status, start_time, end_time, created_by, error_message, parameters
        ) VALUES ($1, $2, $3, $4, $5, $6, $7) 
        RETURNING id
        """
        
        run_parameters = {
            "features_shape": [metadata['num_sequences'], metadata['sequence_length'], metadata['num_features']],
            "labels_shape": [metadata['num_sequences'], 1],
            "dataset_name": f"aapl_training_2000_2025_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "generation_method": "synthetic_training_data_generator",
            "file_size_mb": total_size_mb
        }
        
        now = datetime.now()
        run_id = await conn.fetchval(
            run_query,
            "training_data_generation",
            "completed",
            now,
            now,
            "synthetic_training_data_generator",
            None,
            json.dumps(run_parameters)
        )
        
        print(f"📝 Created run record: {run_id}")
        
        # Now create the training dataset record
        dataset_query = """
        INSERT INTO dev_training_datasets (
            dataset_name, run_id, total_sequences, sequence_length, feature_count, label_count,
            symbols, date_range_start, date_range_end, data_quality_score, feature_completeness,
            label_completeness, generation_duration_seconds, file_size_mb, data_sources, status,
            features_file_path, labels_file_path, metadata_file_path, feature_metadata,
            technical_indicators, prediction_horizon, created_by, generation_parameters
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $23, $24
        ) RETURNING id
        """
        
        dataset_name = f"aapl_training_2000_2025_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        dataset_id = await conn.fetchval(
            dataset_query,
            dataset_name,
            run_id,
            metadata['num_sequences'],
            metadata['sequence_length'], 
            metadata['num_features'],
            1,  # label_count
            ["AAPL"],  # symbols array
            date(2000, 4, 24),  # date_range_start
            date(2024, 12, 25),  # date_range_end
            1.0,  # data_quality_score
            1.0,  # feature_completeness
            1.0,  # label_completeness
            0,    # generation_duration_seconds
            total_size_mb,  # file_size_mb
            ["synthetic_generator"],  # data_sources array
            "completed",  # status
            "/data/training/aapl_2000_2025/AAPL_features.npy",  # Container path for features
            "/data/training/aapl_2000_2025/AAPL_labels.npy",    # Container path for labels
            "/data/training/aapl_2000_2025/AAPL_metadata.json", # Container path for metadata
            json.dumps({
                "feature_names": metadata['feature_names'],
                "technical_indicators": metadata['technical_indicators'],
                "timeframes": ["5m", "15m", "1h", "1d"],
                "generation_method": "synthetic_with_technical_indicators"
            }),  # feature_metadata
            ",".join(metadata['technical_indicators']),  # technical_indicators
            metadata['prediction_horizon'],  # prediction_horizon
            "synthetic_training_data_generator",  # created_by
            json.dumps({
                "symbol": metadata['symbol'],
                "base_price": 30.0,
                "sequence_length": metadata['sequence_length'],
                "prediction_horizon": metadata['prediction_horizon'],
                "num_features": metadata['num_features'],
                "generation_timestamp": metadata['generation_timestamp']
            })  # generation_parameters
        )
        
        print(f"\n✅ Successfully registered training dataset!")
        print(f"   Run ID: {run_id}")
        print(f"   Dataset ID: {dataset_id}")
        print(f"   Dataset name: {dataset_name}")
        print(f"   File size: {total_size_mb:.1f} MB")
        print(f"   Features file: /data/training/aapl_2000_2025/AAPL_features.npy")
        print(f"   Labels file: /data/training/aapl_2000_2025/AAPL_labels.npy")
        print(f"   Metadata file: /data/training/aapl_2000_2025/AAPL_metadata.json")
        
        print(f"\n🔍 Dataset now available in analytics service at:")
        print(f"   http://localhost:3000/api/datasets")
        print(f"   The analytics dashboard can now visualize this training data!")
        
        return 0
        
    except Exception as e:
        print(f"❌ Failed to register dataset: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        await conn.close()

if __name__ == "__main__":
    exit_code = asyncio.run(register_training_dataset())
    exit(exit_code)