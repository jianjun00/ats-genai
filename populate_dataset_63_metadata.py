#!/usr/bin/env python3
"""
Manually populate file metadata for dataset 63 to test sequence-based structure.
"""

import asyncio
import asyncpg
import json
from pathlib import Path
from datetime import datetime

async def populate_dataset_63():
    """Populate file metadata for dataset 63."""
    print("🔧 Populating metadata for dataset 63...")

    # Connect to database
    conn = await asyncpg.connect(
        host="localhost",
        port=3432,
        user="postgres",
        password="dev_password",
        database="dev_db"
    )

    try:
        # Get dataset info
        dataset = await conn.fetchrow("""
            SELECT id, dataset_name, run_id, symbols FROM dev_training_datasets WHERE id = 63
        """)

        if not dataset:
            print("❌ Dataset 63 not found")
            return

        print(f"📊 Dataset: {dataset['dataset_name']}")
        print(f"   Run ID: {dataset['run_id']}")
        print(f"   Symbols: {dataset['symbols']}")

        # Scan files in the sequence directory
        base_dir = Path("/mnt/d/ats-data/training_data/83")
        sequence_dir = base_dir / "AAPL_20250801_000000_20250801_000000"

        if not sequence_dir.exists():
            print(f"❌ Sequence directory not found: {sequence_dir}")
            return

        print(f"✅ Found sequence directory: {sequence_dir}")

        # Create file metadata structure
        file_metadata = {
            "files": [],
            "total_sequences": 0,
            "total_files": 0,
            "timeframes": [],
            "symbols": ["AAPL"],
            "generation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Scan timeframes
        timeframes = ['5m', '15m', '1h', '1d', '1w']
        for timeframe in timeframes:
            tf_dir = sequence_dir / timeframe
            if tf_dir.exists():
                arrayrecord_files = list(tf_dir.glob("*.arrayrecord"))
                for file_path in arrayrecord_files:
                    file_stats = file_path.stat()

                    file_info = {
                        "symbol": "AAPL",
                        "timeframe": timeframe,
                        "file_path": "AAPL_20250801_000000_20250801_000000.arrayrecord",
                        "sequences": 1,  # Each file has 1 sequence
                        "file_size_bytes": file_stats.st_size,
                        "created_at": datetime.fromtimestamp(file_stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                    }

                    file_metadata["files"].append(file_info)
                    file_metadata["timeframes"].append(timeframe)

                    print(f"   ✅ {timeframe}: {file_path.name} ({file_stats.st_size} bytes)")

        # Finalize metadata
        file_metadata["total_files"] = len(file_metadata["files"])
        file_metadata["total_sequences"] = sum(f["sequences"] for f in file_metadata["files"])
        file_metadata["timeframes"] = sorted(list(set(file_metadata["timeframes"])))

        # Update database
        await conn.execute("""
            UPDATE dev_training_datasets
            SET file_metadata = $1, total_sequences = $2
            WHERE id = 63
        """, json.dumps(file_metadata), file_metadata["total_sequences"])

        print(f"✅ Updated database with metadata:")
        print(f"   Total files: {file_metadata['total_files']}")
        print(f"   Total sequences: {file_metadata['total_sequences']}")
        print(f"   Timeframes: {file_metadata['timeframes']}")

    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(populate_dataset_63())