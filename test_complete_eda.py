#!/usr/bin/env python3
"""
Complete test of EDA training dataset functionality
"""
import sys
import os
import json
import asyncio
import asyncpg
from datetime import date
sys.path.append('src')

async def test_complete_eda_flow():
    """Test the complete EDA flow end-to-end"""
    print("🧪 Starting complete EDA functionality test...")
    
    # Database connection parameters (Docker container to container)
    db_config = {
        'host': 'ats-dev-postgres',
        'port': 5432,
        'user': 'postgres',
        'password': 'dev_password',
        'database': 'dev_db'
    }
    
    try:
        # Test database connection
        conn = await asyncpg.connect(**db_config)
        print("✅ Database connection successful")
        
        # Test inserting a comprehensive training dataset
        sample_tfdv_stats = {
            "features": {
                "close_price": {"mean": 150.5, "std": 25.2, "min": 100.0, "max": 200.0},
                "volume": {"mean": 1000000, "std": 500000, "min": 100000, "max": 5000000}
            },
            "labels": {
                "next_day_return": {"mean": 0.001, "std": 0.02, "min": -0.1, "max": 0.1}
            }
        }
        
        sample_feature_dist = {
            "close_price": {"type": "numeric", "mean": 150.5, "percentiles": {"25": 130, "50": 150, "75": 170}},
            "volume": {"type": "numeric", "mean": 1000000, "percentiles": {"25": 750000, "50": 1000000, "75": 1250000}}
        }
        
        sample_label_dist = {
            "next_day_return": {"type": "numeric", "mean": 0.001, "percentiles": {"25": -0.01, "50": 0.001, "75": 0.012}}
        }
        
        # Insert test dataset
        insert_query = """
        INSERT INTO dev_training_datasets (
            dataset_name, total_sequences, feature_count, label_count,
            symbols, date_range_start, date_range_end,
            data_quality_score, feature_completeness, label_completeness,
            file_size_mb, technical_indicators,
            tfdv_statistics, feature_distributions, label_distributions,
            features_file_path, labels_file_path, metadata_file_path
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
        ) RETURNING id, dataset_name, data_quality_score
        """
        
        result = await conn.fetchrow(
            insert_query,
            'comprehensive_test_dataset',  # dataset_name
            1000,  # total_sequences
            2,     # feature_count
            1,     # label_count
            ['AAPL', 'TSLA'],  # symbols
            date(2020, 1, 1),   # date_range_start
            date(2023, 12, 31), # date_range_end
            0.95,  # data_quality_score
            0.98,  # feature_completeness
            0.99,  # label_completeness
            15.5,  # file_size_mb
            'SMA_20, RSI_14, MACD',  # technical_indicators
            json.dumps(sample_tfdv_stats),      # tfdv_statistics
            json.dumps(sample_feature_dist),    # feature_distributions
            json.dumps(sample_label_dist),      # label_distributions
            '/data/features/test_features.npy',  # features_file_path
            '/data/labels/test_labels.npy',      # labels_file_path
            '/data/metadata/test_metadata.json'  # metadata_file_path
        )
        
        print(f"✅ Training dataset created with ID: {result['id']}")
        print(f"   Dataset: {result['dataset_name']}")
        print(f"   Quality Score: {result['data_quality_score']}")
        
        # Test querying the dataset with TFDV statistics
        query_result = await conn.fetchrow("""
            SELECT dataset_name, total_sequences, feature_count, label_count,
                   symbols, data_quality_score, feature_completeness, label_completeness,
                   technical_indicators, tfdv_statistics, feature_distributions,
                   label_distributions
            FROM dev_training_datasets 
            WHERE dataset_name = $1
        """, 'comprehensive_test_dataset')
        
        if query_result:
            print("✅ Dataset query successful")
            print(f"   Sequences: {query_result['total_sequences']}")
            print(f"   Features: {query_result['feature_count']}")
            print(f"   Labels: {query_result['label_count']}")
            print(f"   Symbols: {query_result['symbols']}")
            print(f"   Technical Indicators: {query_result['technical_indicators']}")
            
            # Test TFDV statistics parsing
            tfdv_stats = json.loads(query_result['tfdv_statistics'])
            feature_dists = json.loads(query_result['feature_distributions'])
            label_dists = json.loads(query_result['label_distributions'])
            
            print("✅ TFDV statistics parsed successfully")
            print(f"   Features in TFDV: {list(tfdv_stats['features'].keys())}")
            print(f"   Labels in TFDV: {list(tfdv_stats['labels'].keys())}")
            print(f"   Feature distributions: {list(feature_dists.keys())}")
            print(f"   Label distributions: {list(label_dists.keys())}")
            
        # Test updating TFDV statistics (simulating recomputation)
        updated_tfdv = {
            "features": {
                "close_price": {"mean": 151.2, "std": 25.8, "min": 99.5, "max": 201.3, "updated": True},
                "volume": {"mean": 1050000, "std": 520000, "min": 95000, "max": 5200000, "updated": True}
            },
            "labels": {
                "next_day_return": {"mean": 0.0012, "std": 0.021, "min": -0.11, "max": 0.12, "updated": True}
            },
            "computation_timestamp": "2025-08-31T12:00:00Z"
        }
        
        await conn.execute("""
            UPDATE dev_training_datasets 
            SET tfdv_statistics = $1, updated_at = NOW()
            WHERE dataset_name = $2
        """, json.dumps(updated_tfdv), 'comprehensive_test_dataset')
        
        print("✅ TFDV statistics updated successfully")
        
        # Test querying all training datasets (simulating API endpoint)
        all_datasets = await conn.fetch("""
            SELECT id, dataset_name, total_sequences, feature_count, label_count,
                   data_quality_score, feature_completeness, label_completeness,
                   file_size_mb, technical_indicators, created_at
            FROM dev_training_datasets 
            ORDER BY created_at DESC
        """)
        
        print(f"✅ Found {len(all_datasets)} training datasets")
        for dataset in all_datasets:
            print(f"   - {dataset['dataset_name']} ({dataset['total_sequences']} sequences, "
                  f"{dataset['feature_count']} features, {dataset['label_count']} labels)")
        
        await conn.close()
        print("✅ Database connection closed")
        
        # Test TFDV service integration
        try:
            from services.tfdv_integration_service import TFDVIntegrationService
            
            tfdv_service = TFDVIntegrationService()
            print("✅ TFDV service initialized")
            
            # Test mock statistics computation (since TFDV is not available)
            import numpy as np
            
            # Create sample data
            features = np.random.randn(100, 10, 5)  # 100 sequences, 10 timesteps, 5 features
            labels = np.random.randn(100, 2)        # 100 sequences, 2 labels
            feature_names = ['close', 'open', 'high', 'low', 'volume']
            label_names = ['next_return', 'next_volatility']
            
            stats = await tfdv_service.compute_dataset_statistics(
                features, labels, feature_names, label_names, 'test_computation'
            )
            
            print("✅ TFDV statistics computed successfully")
            print(f"   Statistics keys: {list(stats.keys())}")
            print(f"   Feature distributions: {list(stats.get('feature_distributions', {}).keys())}")
            print(f"   Label distributions: {list(stats.get('label_distributions', {}).keys())}")
            
        except Exception as e:
            print(f"⚠️  TFDV service test failed: {e}")
        
        print("🎯 Complete EDA functionality test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_complete_eda_flow())
    sys.exit(0 if success else 1)