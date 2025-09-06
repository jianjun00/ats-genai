#!/usr/bin/env python3
"""
Create training dataset record in the correct dev_training_datasets table
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from core.database.connection_manager import get_raw_connection
import psycopg2.extras
import json

def create_dataset_record():
    """Create dataset record in dev_training_datasets table"""
    
    dataset_data = {
        'dataset_name': 'AAPL_TSLA_20250701_20250906_Run89',
        'run_id': 89,
        'symbols': ['AAPL', 'TSLA'],
        'date_range_start': '2025-07-01',
        'date_range_end': '2025-09-06',
        'data_quality_score': 0.95,
        'feature_completeness': 0.98,
        'label_completeness': 0.97,
        'total_sequences': 2,
        'file_size_mb': 50.0,
        'status': 'completed',
        'dataset_path': '/mnt/d/ats-data/training_data/89',
        'symbol_files': {
            'AAPL': 'AAPL_20250701_000000_20250906_000000',
            'TSLA': 'TSLA_20250701_000000_20250906_000000'
        },
        'file_metadata': {
            'symbols': ['AAPL', 'TSLA'],
            'timeframes': ['5m', '15m', '1h', '1d', '1w'],
            'total_files': 10
        }
    }
    
    with get_raw_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("""
                INSERT INTO dev_training_datasets (
                    dataset_name, run_id, symbols, date_range_start, date_range_end,
                    data_quality_score, feature_completeness, label_completeness,
                    total_sequences, file_size_mb, status, dataset_path,
                    symbol_files, file_metadata
                ) VALUES (
                    %(dataset_name)s, %(run_id)s, %(symbols)s, 
                    %(date_range_start)s, %(date_range_end)s,
                    %(data_quality_score)s, %(feature_completeness)s, %(label_completeness)s,
                    %(total_sequences)s, %(file_size_mb)s, %(status)s, %(dataset_path)s,
                    %(symbol_files)s, %(file_metadata)s
                ) RETURNING id
            """, {
                **dataset_data,
                'symbol_files': json.dumps(dataset_data['symbol_files']),
                'file_metadata': json.dumps(dataset_data['file_metadata'])
            })
            
            dataset_id = cursor.fetchone()['id']
            conn.commit()
            
            print(f"✅ Created dataset record with ID: {dataset_id}")
            
            # Verify the record
            cursor.execute("""
                SELECT id, dataset_name, symbols, total_sequences, status 
                FROM dev_training_datasets 
                WHERE id = %s
            """, (dataset_id,))
            
            record = cursor.fetchone()
            print("Dataset record details:")
            for key, value in record.items():
                print(f"  {key}: {value}")
                
            return dataset_id

if __name__ == "__main__":
    try:
        dataset_id = create_dataset_record()
        print(f"\n🎉 Dataset should now appear in UI with ID {dataset_id}")
    except Exception as e:
        print(f"❌ Error creating dataset record: {e}")
        import traceback
        traceback.print_exc()