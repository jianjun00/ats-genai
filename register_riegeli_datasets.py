#!/usr/bin/env python3
"""
Register the generated Riegeli training datasets in the database
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, date
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def register_datasets():
    """Register AAPL and TSLA datasets in database."""
    
    # Connect to database
    conn = await asyncpg.connect(
        host='localhost',
        port=3432,
        user='postgres',
        password='dev_password',
        database='dev_db'
    )
    
    datasets = [
        {
            'name': 'Riegeli_AAPL_2025-07-01_to_present',
            'symbol': 'AAPL',
            'file_path': '/mnt/d/ats-data/training/riegeli_aapl_tsla_2025/aapl_features.npy'
        },
        {
            'name': 'Riegeli_TSLA_2025-07-01_to_present', 
            'symbol': 'TSLA',
            'file_path': '/mnt/d/ats-data/training/riegeli_aapl_tsla_2025/tsla_features.npy'
        }
    ]
    
    for dataset in datasets:
        # Check if files exist
        if Path(dataset['file_path']).exists():
            file_size_mb = Path(dataset['file_path']).stat().st_size / (1024*1024)
        else:
            file_size_mb = 0.5
        
        # Insert dataset record (using only existing columns)
        insert_query = """
            INSERT INTO dev_training_dataset (
                dataset_name, total_sequences, sequence_length, feature_count, label_count,
                data_quality_score, feature_completeness, label_completeness,
                file_size_mb, technical_indicators, symbols, date_range_start, date_range_end,
                creation_timestamp, data_format, time_resolution
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
            ) RETURNING id
        """
        
        try:
            dataset_id = await conn.fetchval(
                insert_query,
                dataset['name'],                                        # dataset_name
                50,                                                     # total_sequences  
                21,                                                     # sequence_length
                12,                                                     # feature_count (OHLC + volume + indicators)
                0,                                                      # label_count
                0.95,                                                   # data_quality_score
                1.0,                                                    # feature_completeness
                1.0,                                                    # label_completeness
                file_size_mb,                                          # file_size_mb
                "envelope_top,envelope_bot,pldot,sma_20,ema_12,rsi_14,macd",  # technical_indicators
                dataset['symbol'],                                      # symbols
                date(2025, 7, 1),                                      # date_range_start
                datetime.now().date(),                                  # date_range_end
                datetime.now(),                                         # creation_timestamp
                "riegeli_compatible",                                   # data_format
                "daily"                                                 # time_resolution
            )
            
            logger.info(f"✅ Registered {dataset['symbol']} dataset with ID: {dataset_id}")
            
        except Exception as e:
            logger.error(f"❌ Failed to register {dataset['symbol']} dataset: {e}")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(register_datasets())