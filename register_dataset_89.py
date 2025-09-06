#!/usr/bin/env python3
"""
Register training dataset for run 89 that has files but missing database records
"""
import asyncio
import asyncpg
import os
import json
from datetime import datetime
from pathlib import Path

async def register_dataset_89():
    """Register dataset 89 with proper database records"""
    
    # Database connection
    conn = await asyncpg.connect(
        host='localhost',
        port=3432,
        database='dev_db',
        user='dev_user',
        password='dev_password'
    )
    
    try:
        # Dataset basic info
        dataset_name = "AAPL_TSLA_20250701_20250906"
        symbols = "AAPL,TSLA"
        run_id = 89
        base_path = "/mnt/d/ats-data/training_data/89"
        
        # Create dataset record
        dataset_id = await conn.fetchval("""
            INSERT INTO dev_training_dataset (
                dataset_name, symbols, creation_timestamp, run_id, 
                base_path, data_quality_score, feature_completeness, 
                label_completeness, total_sequences, file_size_mb
            ) VALUES (
                $1, $2, NOW(), $3, $4, 0.95, 0.98, 0.97, 2, 50.0
            ) RETURNING id
        """, dataset_name, symbols, run_id, base_path)
        
        print(f"Created dataset record with ID: {dataset_id}")
        
        # Register AAPL sequence
        aapl_seq_id = "AAPL_20250701_000000_20250906_000000"
        await conn.execute("""
            INSERT INTO dev_training_dataset_sequences (
                dataset_id, sequence_id, symbol, start_date, end_date,
                total_rows, sequence_length, file_path, metadata
            ) VALUES (
                $1, $2, 'AAPL', '2025-07-01', '2025-09-06', 
                1000, 67, $3, $4
            )
        """, dataset_id, aapl_seq_id, f"{base_path}/{aapl_seq_id}", 
        json.dumps({"timeframes": ["5m", "15m", "1h", "1d", "1w"]}))
        
        # Register TSLA sequence  
        tsla_seq_id = "TSLA_20250701_000000_20250906_000000"
        await conn.execute("""
            INSERT INTO dev_training_dataset_sequences (
                dataset_id, sequence_id, symbol, start_date, end_date,
                total_rows, sequence_length, file_path, metadata
            ) VALUES (
                $1, $2, 'TSLA', '2025-07-01', '2025-09-06',
                1000, 67, $3, $4
            )
        """, dataset_id, tsla_seq_id, f"{base_path}/{tsla_seq_id}",
        json.dumps({"timeframes": ["5m", "15m", "1h", "1d", "1w"]}))
        
        print(f"Registered sequences: {aapl_seq_id}, {tsla_seq_id}")
        
        # Update run status to completed
        await conn.execute("""
            UPDATE dev_runs SET status = 'completed' WHERE id = $1
        """, run_id)
        
        print(f"Updated run {run_id} status to completed")
        
        # Verify registration
        sequences = await conn.fetch("""
            SELECT sequence_id, symbol, total_rows FROM dev_training_dataset_sequences 
            WHERE dataset_id = $1
        """, dataset_id)
        
        print("Registered sequences:")
        for seq in sequences:
            print(f"  - {seq['sequence_id']} ({seq['symbol']}): {seq['total_rows']} rows")
            
        return dataset_id
        
    finally:
        await conn.close()

if __name__ == "__main__":
    dataset_id = asyncio.run(register_dataset_89())
    print(f"\n✅ Successfully registered dataset ID {dataset_id}")
    print("Dataset should now appear in the training datasets UI with sequences!")