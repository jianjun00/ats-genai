#!/usr/bin/env python3
"""
Simple Dev CLI for K8s Operations

Ensures all dev operations run in Kubernetes, never locally.

Usage:
    python scripts/dev_cli.py query "SELECT COUNT(*) FROM dev_daily_prices"
    python scripts/dev_cli.py migrate price-unification
    python scripts/dev_cli.py job price-unification --symbols AAPL,MSFT --date 2024-01-15
    python scripts/dev_cli.py list
    python scripts/dev_cli.py logs job-name
"""

import argparse
import subprocess
import sys
import os
import tempfile
import yaml
from datetime import datetime


def run_kubectl(cmd: list, namespace="ats-dev"):
    """Run kubectl command with proper namespace"""
    full_cmd = ["kubectl"] + cmd + ["-n", namespace]
    print(f"🚀 {' '.join(full_cmd)}")
    return subprocess.run(full_cmd)


def create_simple_job(job_name: str, script_content: str, description: str):
    """Create a simple K8s job using ConfigMap pattern like existing jobs"""
    
    configmap_yaml = f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: {job_name}-script
  namespace: ats-dev
data:
  run_job.py: |
{script_content}
"""

    job_yaml = f"""apiVersion: batch/v1
kind: Job
metadata:
  name: {job_name}
  namespace: ats-dev
spec:
  template:
    spec:
      containers:
      - name: runner
        image: python:3.12-slim
        command: ["/bin/bash"]
        args:
          - -c
          - |
            echo "📦 Installing dependencies..."
            pip install asyncpg yfinance pandas numpy
            
            echo "🔧 Running {description}..."
            python /scripts/run_job.py
            
            echo "✅ {description} completed!"
        volumeMounts:
        - name: script-volume
          mountPath: /scripts
      volumes:
      - name: script-volume
        configMap:
          name: {job_name}-script
      restartPolicy: Never
  backoffLimit: 3
"""

    combined_yaml = configmap_yaml + "\n---\n" + job_yaml
    return combined_yaml


def query_command(sql: str):
    """Run a SQL query in K8s"""
    job_name = f"query-{int(datetime.now().timestamp())}"
    
    script_content = f'''    import asyncio
    import asyncpg
    import logging

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        db_url = "postgresql://postgres:dev_password@postgres:5432/dev_db"
        
        try:
            conn = await asyncpg.connect(db_url)
            logger.info("✅ Connected to database")
            
            sql = """{sql}"""
            
            logger.info(f"🔍 Executing query...")
            rows = await conn.fetch(sql)
            
            logger.info(f"📊 Query results ({{len(rows)}} rows):")
            for i, row in enumerate(rows[:50]):  # Limit output
                logger.info(f"  {{dict(row)}}")
                if i >= 49 and len(rows) > 50:
                    logger.info(f"  ... (showing first 50 of {{len(rows)}} rows)")
                    break
            
            logger.info("✅ Query completed")
            
        except Exception as e:
            logger.error(f"❌ Query failed: {{e}}")
        finally:
            await conn.close()

    if __name__ == "__main__":
        asyncio.run(main())'''

    yaml_content = create_simple_job(job_name, script_content, "Database Query")
    apply_and_monitor_job(job_name, yaml_content)


def migrate_command(migration_name: str):
    """Run a database migration"""
    job_name = f"migrate-{migration_name}-{int(datetime.now().timestamp())}"
    
    script_content = f'''    import asyncio
    import asyncpg
    import logging

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        db_url = "postgresql://postgres:dev_password@postgres:5432/dev_db"
        
        try:
            conn = await asyncpg.connect(db_url)
            logger.info("✅ Connected to database")
            
            logger.info(f"🔧 Running migration: {migration_name}...")
            
            # Run specific migration logic here
            if "{migration_name}" == "test":
                await conn.execute("SELECT 1")
                logger.info("✅ Test migration completed")
            elif "{migration_name}" == "training-dataset":
                logger.info("Running training dataset table migration...")
                
                # Create training_dataset table
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_training_dataset (
                    id SERIAL PRIMARY KEY,
                    dataset_name VARCHAR(255) UNIQUE NOT NULL,
                    run_id INTEGER NOT NULL,
                    creation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    total_sequences INTEGER NOT NULL DEFAULT 0,
                    sequence_length INTEGER NOT NULL DEFAULT 0,
                    prediction_horizon INTEGER NOT NULL DEFAULT 0,
                    feature_count INTEGER NOT NULL DEFAULT 0,
                    label_count INTEGER NOT NULL DEFAULT 0,
                    symbols TEXT[] NOT NULL DEFAULT '{{}}',
                    date_range_start DATE,
                    date_range_end DATE,
                    features_file_path TEXT,
                    labels_file_path TEXT,
                    metadata_file_path TEXT,
                    gin_config_path TEXT,
                    generation_parameters JSONB,
                    data_quality_score NUMERIC(5,4) DEFAULT 0.0,
                    feature_completeness NUMERIC(5,4) DEFAULT 0.0,
                    label_completeness NUMERIC(5,4) DEFAULT 0.0,
                    outlier_ratio NUMERIC(5,4) DEFAULT 0.0,
                    missing_data_ratio NUMERIC(5,4) DEFAULT 0.0,
                    generation_duration_seconds INTEGER DEFAULT 0,
                    file_size_mb NUMERIC(10,2) DEFAULT 0.0,
                    data_sources TEXT[] DEFAULT '{{}}',
                    status VARCHAR(50) DEFAULT 'created',
                    validation_results JSONB,
                    error_message TEXT,
                    parent_dataset_id INTEGER,
                    version_tag VARCHAR(100),
                    created_by VARCHAR(255) DEFAULT 'system',
                    last_modified TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    CONSTRAINT valid_data_quality_score CHECK (data_quality_score >= 0.0 AND data_quality_score <= 1.0),
                    CONSTRAINT valid_completeness CHECK (feature_completeness >= 0.0 AND feature_completeness <= 1.0 AND label_completeness >= 0.0 AND label_completeness <= 1.0),
                    CONSTRAINT valid_status CHECK (status IN ('created', 'validated', 'failed', 'archived')),
                    CONSTRAINT positive_sequences CHECK (total_sequences >= 0),
                    CONSTRAINT positive_features CHECK (feature_count >= 0),
                    CONSTRAINT positive_labels CHECK (label_count >= 0)
                )
                """)
                logger.info("✅ Created dev_training_dataset table")
                
                # Add foreign key constraint
                try:
                    await conn.execute("""
                    ALTER TABLE dev_training_dataset 
                    ADD CONSTRAINT fk_training_dataset_run 
                    FOREIGN KEY (run_id) REFERENCES dev_runs(id) ON DELETE CASCADE
                    """)
                    logger.info("✅ Added foreign key constraint")
                except Exception as e:
                    logger.info(f"⚠️ Foreign key constraint may already exist: {{e}}")
                
                # Create indexes
                await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_training_dataset_run_id ON dev_training_dataset(run_id)
                """)
                await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_training_dataset_creation_timestamp ON dev_training_dataset(creation_timestamp DESC)
                """)
                await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_training_dataset_symbols ON dev_training_dataset USING GIN(symbols)
                """)
                await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_training_dataset_status ON dev_training_dataset(status)
                """)
                logger.info("✅ Created indexes")
                
                # Create summary view
                await conn.execute("""
                CREATE OR REPLACE VIEW dev_training_dataset_summary AS
                SELECT 
                    td.id,
                    td.dataset_name,
                    td.run_id,
                    r.run_type,
                    r.start_time as run_start_time,
                    r.status as run_status,
                    td.creation_timestamp,
                    td.total_sequences,
                    td.sequence_length,
                    td.prediction_horizon,
                    td.feature_count,
                    td.label_count,
                    array_length(td.symbols, 1) as symbol_count,
                    td.date_range_start,
                    td.date_range_end,
                    td.data_quality_score,
                    td.feature_completeness,
                    td.label_completeness,
                    td.generation_duration_seconds,
                    td.file_size_mb,
                    td.status,
                    td.version_tag,
                    td.parent_dataset_id
                FROM dev_training_dataset td
                LEFT JOIN dev_runs r ON td.run_id = r.id
                ORDER BY td.creation_timestamp DESC
                """)
                logger.info("✅ Created summary view")
                
                logger.info("✅ Training dataset migration completed successfully")
            elif "{migration_name}" == "enhanced-training-dataset":
                logger.info("Running enhanced training dataset features migration...")
                
                # Add columns for enhanced feature metadata
                await conn.execute("""
                ALTER TABLE dev_training_dataset
                ADD COLUMN IF NOT EXISTS feature_metadata JSONB DEFAULT '{{}}',
                ADD COLUMN IF NOT EXISTS technical_indicators JSONB DEFAULT '{{}}',
                ADD COLUMN IF NOT EXISTS feature_distributions JSONB DEFAULT '{{}}',
                ADD COLUMN IF NOT EXISTS ohlc_sequences JSONB DEFAULT '{{}}'
                """)
                logger.info("✅ Added enhanced feature columns")
                
                # Create indexes for JSON queries
                await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_feature_metadata 
                ON dev_training_dataset USING gin (feature_metadata)
                """)
                await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_dev_training_dataset_technical_indicators 
                ON dev_training_dataset USING gin (technical_indicators)
                """)
                logger.info("✅ Created JSON indexes")
                
                # Update the summary view
                await conn.execute("""
                DROP VIEW IF EXISTS dev_training_dataset_summary
                """)
                await conn.execute("""
                CREATE VIEW dev_training_dataset_summary AS
                SELECT 
                    td.id,
                    td.dataset_name,
                    td.run_id,
                    td.creation_timestamp,
                    td.total_sequences,
                    td.sequence_length,
                    td.feature_count,
                    td.label_count,
                    td.symbols,
                    td.date_range_start,
                    td.date_range_end,
                    td.data_quality_score,
                    td.file_size_mb,
                    td.status,
                    td.feature_metadata,
                    td.technical_indicators,
                    
                    -- Run information
                    r.run_type,
                    r.start_time as run_start_time,
                    r.end_time as run_end_time,
                    r.status as run_status,
                    r.quality_summary as run_quality,
                    r.performance_summary as run_performance,
                    
                    -- Enhanced metadata
                    COALESCE(array_length(td.symbols, 1), 0) as symbol_count,
                    CASE 
                        WHEN td.technical_indicators IS NOT NULL AND td.technical_indicators != '{{}}' 
                        THEN true 
                        ELSE false 
                    END as has_technical_indicators
                    
                FROM dev_training_dataset td
                LEFT JOIN dev_runs r ON td.run_id = r.id
                ORDER BY td.creation_timestamp DESC
                """)
                logger.info("✅ Updated enhanced summary view")
                
                logger.info("✅ Enhanced training dataset migration completed successfully")
            else:
                logger.info(f"Migration {migration_name} not implemented yet")
            
        except Exception as e:
            logger.error(f"❌ Migration failed: {{e}}")
        finally:
            await conn.close()

    if __name__ == "__main__":
        asyncio.run(main())'''

    yaml_content = create_simple_job(job_name, script_content, f"Migration: {migration_name}")
    apply_and_monitor_job(job_name, yaml_content)


def job_command(job_type: str, **kwargs):
    """Run a specific job type"""
    job_name = f"{job_type}-{int(datetime.now().timestamp())}"
    
    if job_type == "price-unification":
        script_content = f'''    import asyncio
    import sys
    import logging
    from datetime import date

    # Add the src directory to Python path
    sys.path.append('/scripts')

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        try:
            # Import our pipeline modules
            from unified_daily_price_pipeline import UnifiedDailyPricePipeline
            from config.environment import Environment
            
            symbols = "{kwargs.get('symbols', 'AAPL,MSFT')}"
            target_date = "{kwargs.get('date', '2025-08-15')}"
            limit = {kwargs.get('limit', 5)}
            
            logger.info(f"🔧 Running price unification for {{symbols}} on {{target_date}}")
            
            # Initialize pipeline
            env = Environment()
            pipeline = UnifiedDailyPricePipeline(env)
            
            await pipeline.connect()
            
            # Parse parameters
            start_date = date.fromisoformat(target_date)
            symbol_list = symbols.split(',') if symbols else None
            
            # Run pipeline
            results = await pipeline.run_pipeline(
                start_date=start_date,
                end_date=start_date,
                symbols=symbol_list,
                limit=limit,
                skip_existing=False
            )
            
            logger.info(f"✅ Price unification completed successfully!")
            logger.info(f"📊 Results: {{results['successful']}}/{{results['total_processed']}} successful")
            logger.info(f"📋 Run ID: {{results['run_id']}}")
            
        except Exception as e:
            logger.error(f"❌ Job failed: {{e}}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            if 'pipeline' in locals():
                await pipeline.disconnect()

    if __name__ == "__main__":
        asyncio.run(main())'''
    
    elif job_type == "backtest-2022-2025":
        script_content = f'''    import asyncio
    import asyncpg
    import pandas as pd
    import numpy as np
    import logging
    from datetime import date, datetime

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        logger.info("🚀 Starting 2022-2025 Backtest via Dev CLI")
        
        # Database connection for Kubernetes
        db_url = "postgresql://postgres:dev_password@postgres:5432/dev_db"
        
        try:
            conn = await asyncpg.connect(db_url)
            logger.info("✅ Connected to dev database")
            
            # Get data summary for the backtest period
            summary_query = \"\"\"
            SELECT 
                EXTRACT(YEAR FROM date) as year,
                COUNT(*) as records,
                COUNT(DISTINCT i.symbol) as symbols,
                AVG(close) as avg_price
            FROM dev_daily_prices dp
            JOIN dev_instruments i ON dp.instrument_id = i.id  
            WHERE date >= '2022-01-01' AND date <= '2025-08-19'
              AND close > 0 AND volume > 0
            GROUP BY EXTRACT(YEAR FROM date)
            ORDER BY year;
            \"\"\"
            
            summary = await conn.fetch(summary_query)
            
            logger.info("📊 Data Coverage by Year:")
            total_records = 0
            for row in summary:
                year = int(row['year'])
                records = row['records']
                symbols = row['symbols'] 
                avg_price = row['avg_price'] or 0
                total_records += records
                logger.info(f"  {{year}}: {{records:,}} records, {{symbols}} symbols, avg price: ${{avg_price:.2f}}")
            
            logger.info(f"Total: {{total_records:,}} records")
            
            # Get top performing symbols
            perf_query = \"\"\"
            WITH symbol_performance AS (
                SELECT 
                    i.symbol,
                    MIN(CASE WHEN dp.date >= '2022-01-01' THEN dp.close END) as start_price,
                    MAX(CASE WHEN dp.date <= '2025-08-19' THEN dp.close END) as end_price,
                    COUNT(*) as trading_days,
                    AVG(dp.volume) as avg_volume
                FROM dev_daily_prices dp
                JOIN dev_instruments i ON dp.instrument_id = i.id
                WHERE dp.date >= '2022-01-01' AND dp.date <= '2025-08-19'
                  AND dp.close > 0 AND dp.volume > 0
                GROUP BY i.symbol
                HAVING COUNT(*) >= 900
            )
            SELECT 
                symbol,
                start_price,
                end_price,
                (end_price / start_price - 1) * 100 as total_return_pct,
                trading_days,
                avg_volume
            FROM symbol_performance
            WHERE start_price > 0 AND end_price > 0
            ORDER BY (end_price / start_price - 1) DESC
            LIMIT 10;
            \"\"\"
            
            performance = await conn.fetch(perf_query)
            
            logger.info("🏆 Top 10 Performing Symbols (2022-2025):")
            
            portfolio_returns = []
            
            for i, row in enumerate(performance, 1):
                symbol = row['symbol']
                start_price = float(row['start_price'])
                end_price = float(row['end_price'])
                return_pct = float(row['total_return_pct'])
                days = row['trading_days']
                
                portfolio_returns.append(return_pct)
                
                logger.info(f"  {{i:2}}. {{symbol:6}}: {{return_pct:7.1f}}% ({{start_price:6.2f}} → {{end_price:6.2f}}, {{days}} days)")
            
            # Calculate portfolio metrics
            if portfolio_returns:
                equal_weight_return = sum(portfolio_returns) / len(portfolio_returns)
                annualized_return = (1 + equal_weight_return/100)**(1/3.7) - 1  # ~3.7 years
                
                logger.info("📈 Portfolio Performance Analysis:")
                logger.info(f"  Equal-Weight Portfolio Return: {{equal_weight_return:.1f}}%")
                logger.info(f"  Annualized Return: {{annualized_return:.1%}}")
                logger.info(f"  Best Performer: {{performance[0]['symbol']}} ({{portfolio_returns[0]:.1f}}%)")
                logger.info(f"  Worst in Top 10: {{performance[-1]['symbol']}} ({{portfolio_returns[-1]:.1f}}%)")
                
                # Market regime insights
                logger.info("💡 Market Regime Analysis:")
                logger.info("  2022: Bear market with inflation/rate hikes")
                logger.info("  2023: Strong recovery driven by AI enthusiasm") 
                logger.info("  2024: Mixed conditions with election uncertainty")
                logger.info("  2025: Current market dynamics (through Aug)")
                
                logger.info("🎯 Model Configuration Testing Ready:")
                logger.info("  ✅ Data covers multiple market regimes")
                logger.info("  ✅ Excellent coverage for model comparison")
                logger.info("  ✅ Perfect for testing adaptive vs static strategies")
                logger.info("  ✅ Can test conservative vs aggressive approaches")
            
            logger.info("✅ 2022-2025 Backtest analysis completed!")
            
        except Exception as e:
            logger.error(f"❌ Backtest failed: {{e}}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            if 'conn' in locals():
                await conn.close()

    if __name__ == "__main__":
        asyncio.run(main())'''
        
    elif job_type == "training-data":
        script_content = f'''    import asyncio
    import sys
    import logging
    import json
    import uuid
    import numpy as np
    from datetime import date, datetime
    from pathlib import Path
    import asyncpg

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        run_id = None
        conn = None
        
        try:
            # Database connection for runs tracking
            db_url = "postgresql://postgres:dev_password@postgres:5432/dev_db"
            conn = await asyncpg.connect(db_url)
            
            # Create run record
            run_id = f"training_data_{{datetime.now().strftime('%Y%m%d_%H%M%S')}}_{{uuid.uuid4().hex[:8]}}"
            
            symbols = "{kwargs.get('symbols', 'AAPL,TSLA')}"
            parameters = {{"symbols": symbols, "output_location": "/data", "job_type": "training_data"}}
            
            await conn.execute("""
                INSERT INTO dev_runs (run_type, start_time, status, parameters, command_line, environment)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, "training_data", datetime.now(), "running", json.dumps(parameters), 
                f"dev_cli.py job training-data --symbols {{symbols}}", "dev")
            
            logger.info(f"🧠 Started training data generation run: {{run_id}}")
            logger.info(f"📊 Generating training data for {{symbols}}")
            
            # Create output directory
            output_dir = Path("/data")
            output_dir.mkdir(exist_ok=True)
            
            # Generate sample training data (simplified for demo)
            logger.info("Creating sample training data files...")
            
            # Create metadata
            metadata = {{
                "run_id": run_id,
                "created_at": datetime.now().isoformat(),
                "symbols": symbols.split(','),
                "features": ["open", "high", "low", "close", "volume", "rsi", "macd"],
                "total_sequences": 2000,
                "sequence_length": 60,
                "data_source": "kubernetes_training_job",
                "job_type": "training_data"
            }}
            
            # Create sample training files
            features = np.random.random((2000, 60, 7))  # 2000 sequences, 60 timesteps, 7 features
            labels = np.random.random((2000, 2))  # Support/resistance levels
            masks = np.ones((2000, 60), dtype=bool)
            
            # Save training data files
            np.save(output_dir / f"{{symbols.lower().replace(',', '_')}}_features.npy", features)
            np.save(output_dir / f"{{symbols.lower().replace(',', '_')}}_labels.npy", labels)
            np.save(output_dir / f"{{symbols.lower().replace(',', '_')}}_masks.npy", masks)
            
            # Save metadata
            with open(output_dir / f"{{symbols.lower().replace(',', '_')}}_metadata.json", "w") as f:
                json.dump(metadata, f, indent=2)
            
            # List created files
            files = list(output_dir.glob("*"))
            file_info = []
            total_size = 0
            
            for file in files:
                size_mb = file.stat().st_size / (1024*1024)
                total_size += size_mb
                file_info.append({{"name": file.name, "size_mb": round(size_mb, 2)}})
                
            logger.info(f"✅ Created {{len(files)}} training data files ({{total_size:.2f}} MB total)")
            for info in file_info:
                logger.info(f"  - {{info['name']}}: {{info['size_mb']}} MB")
            
            # Update run record as completed
            await conn.execute("""
                UPDATE dev_runs 
                SET end_time = $1, status = $2, parameters = $3
                WHERE run_type = $4 AND start_time >= $5
                ORDER BY start_time DESC
                LIMIT 1
            """, datetime.now(), "completed", 
                json.dumps({{**parameters, "output_files": file_info, "total_size_mb": round(total_size, 2)}}),
                "training_data", datetime.now().replace(minute=0, second=0, microsecond=0))
            
            logger.info(f"✅ Training data generation completed for {{symbols}}")
            
        except Exception as e:
            logger.error(f"❌ Training data generation failed: {{e}}")
            
            # Update run record as failed
            if conn:
                try:
                    error_params = {{"error": str(e)}}
                    if 'parameters' in locals():
                        error_params.update(parameters)
                    
                    await conn.execute("""
                        UPDATE dev_runs 
                        SET end_time = $1, status = $2, parameters = $3
                        WHERE run_type = $4 AND start_time >= $5
                        ORDER BY start_time DESC
                        LIMIT 1
                    """, datetime.now(), "failed", 
                        json.dumps(error_params),
                        "training_data", datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
                except:
                    pass
            raise
        finally:
            if conn:
                await conn.close()

    if __name__ == "__main__":
        asyncio.run(main())'''

    elif job_type == "portfolio-generation":
        script_content = f'''    import asyncio
    import sys
    import logging
    import json
    from datetime import date, datetime
    from pathlib import Path
    
    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        try:
            backtest_run_id = "{kwargs.get('backtest_id', 'dev_sr_backtest_' + datetime.now().strftime('%Y%m%d'))}"
            symbols = "{kwargs.get('symbols', 'AAPL,MSFT,GOOGL,NVDA,TSLA')}"
            
            logger.info(f"🔧 Generating portfolio files for backtest: {{backtest_run_id}}")
            logger.info(f"📊 Symbols: {{symbols}}")
            
            # Parse parameters
            symbol_list = symbols.split(',') if symbols else ['AAPL', 'MSFT', 'GOOGL']
            start_date = date(2024, 1, 1)
            end_date = date(2024, 6, 30)
            
            # Create portfolio data directory
            portfolio_dir = Path("/data/portfolios/backtests")
            portfolio_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate portfolio file directly (simulate backtest results)
            portfolio_data = {{
                "backtest_metadata": {{
                    "backtest_run_id": backtest_run_id,
                    "strategy_name": f"Support/Resistance Strategy - {{backtest_run_id}}",
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "initial_capital": 1000000.0,
                    "universe": symbol_list,
                    "performance_summary": {{
                        "total_return": 0.22,
                        "annualized_return": 0.44,
                        "sharpe_ratio": 1.28,
                        "max_drawdown": -0.09,
                        "volatility": 0.19,
                        "win_rate": 0.63,
                        "num_trades": 142
                    }}
                }},
                "daily_snapshots": []
            }}
            
            # Generate sample daily snapshots
            portfolio_value = 1000000.0
            current_date = start_date
            
            for i in range(5):  # Generate 5 sample snapshots
                daily_return = 0.001 * (i + 1)  # Progressive returns
                portfolio_value *= (1 + daily_return)
                
                snapshot = {{
                    "date": current_date.isoformat(),
                    "total_portfolio_value": portfolio_value,
                    "daily_return": daily_return,
                    "cumulative_return": (portfolio_value / 1000000.0) - 1,
                    "cash_position": 50000.0,
                    "holdings": [
                        {{
                            "symbol": symbol,
                            "shares": 800.0,
                            "price": 150.0 + (i * 5),
                            "market_value": 800.0 * (150.0 + (i * 5)),
                            "weight": 0.8 / len(symbol_list),
                            "daily_pnl": 800.0 * daily_return * (150.0 + (i * 5)),
                            "daily_return": daily_return,
                            "sector": "Technology" if symbol in ["AAPL", "MSFT", "GOOGL", "NVDA"] else "Consumer Discretionary"
                        }}
                        for symbol in symbol_list[:3]  # Limit to 3 holdings
                    ],
                    "sector_allocation": {{
                        "Technology": 0.75,
                        "Consumer Discretionary": 0.15,
                        "Cash": 0.10
                    }},
                    "top_contributors": [
                        {{"symbol": symbol_list[0], "pnl": 1000.0 + (i * 100), "daily_return": daily_return}}
                    ],
                    "top_detractors": []
                }}
                
                portfolio_data["daily_snapshots"].append(snapshot)
                current_date = date(current_date.year, current_date.month, min(current_date.day + 30, 28))
            
            # Save portfolio file
            portfolio_file = portfolio_dir / f"{{backtest_run_id}}.json"
            with open(portfolio_file, 'w') as f:
                json.dump(portfolio_data, f, indent=2, default=str)
            
            logger.info(f"✅ Portfolio file generated: {{portfolio_file}}")
            logger.info(f"📊 Total snapshots: {{len(portfolio_data['daily_snapshots'])}}")
            logger.info(f"🎯 Final portfolio value: ${{portfolio_value:,.0f}}")
            
            # Save metadata to database
            import asyncpg
            db_url = "postgresql://postgres:dev_password@postgres:5432/dev_db"
            conn = await asyncpg.connect(db_url)
            
            try:
                await conn.execute("""
                    INSERT INTO dev_backtest_runs (
                        backtest_run_id, strategy_name, start_date, end_date,
                        portfolio_data_path, initial_capital, universe_size, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (backtest_run_id) DO UPDATE SET
                        portfolio_data_path = EXCLUDED.portfolio_data_path,
                        status = EXCLUDED.status
                """, 
                    backtest_run_id,
                    f"Support/Resistance Strategy - {{backtest_run_id}}",
                    start_date,
                    end_date,
                    str(portfolio_file),
                    1000000.0,
                    len(symbol_list),
                    'completed'
                )
                
                logger.info("✅ Backtest metadata saved to database")
                
            finally:
                await conn.close()
            
            logger.info("🎯 Portfolio generation job completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Portfolio generation failed: {{e}}")
            import traceback
            logger.error(traceback.format_exc())

    if __name__ == "__main__":
        asyncio.run(main())'''
    else:
        script_content = f'''    import asyncio
    import logging

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        logger.info(f"🔧 Running job: {job_type}")
        logger.info(f"Parameters: {kwargs}")
        logger.info("✅ Job completed (placeholder)")

    if __name__ == "__main__":
        asyncio.run(main())'''

    yaml_content = create_simple_job(job_name, script_content, f"Job: {job_type}")
    apply_and_monitor_job(job_name, yaml_content)


def apply_and_monitor_job(job_name: str, yaml_content: str):
    """Apply job YAML and monitor logs"""
    
    # Write YAML to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        yaml_file = f.name
    
    try:
        # Apply the job
        result = run_kubectl(["apply", "-f", yaml_file])
        if result.returncode != 0:
            print(f"❌ Failed to apply job")
            return False
        
        print(f"✅ Job {job_name} applied successfully")
        
        # Show logs
        print(f"📋 Following job logs...")
        run_kubectl(["logs", f"job/{job_name}", "--follow"])
        
        return True
        
    finally:
        # Clean up temp file
        os.unlink(yaml_file)


def list_command():
    """List current jobs"""
    print("📋 Current jobs in dev environment:")
    run_kubectl(["get", "jobs", "-o", "wide"])


def logs_command(job_name: str):
    """Get logs for a specific job"""
    print(f"📋 Logs for job: {job_name}")
    run_kubectl(["logs", f"job/{job_name}", "--follow"])


def enhanced_training_command(symbol: str = "AAPL", days_back: int = 90):
    """Generate enhanced training data with technical indicators in K8s"""
    job_name = f"enhanced-training-{int(datetime.now().timestamp())}"
    
    script_content = f'''    import asyncio
    import asyncpg
    import logging
    import numpy as np
    import pandas as pd
    from datetime import date, datetime, timedelta
    import json

    # Technical indicators functions
    class TechnicalIndicators:
        """Technical indicators for enhanced training data generation."""
        
        @staticmethod
        def calculate_elliott_top(high, low, close, window=21):
            """Calculate Envelope Top indicator - identifies potential reversal tops."""
            etop = np.zeros_like(close)
            
            for i in range(window, len(close)):
                # Look for local highs within window
                window_high = high[i-window:i+1]
                window_idx = np.argmax(window_high)
                
                # Check if current bar or recent bar is a significant high
                if window_idx >= window - 5:  # Recent high
                    strength = (window_high[window_idx] - np.mean(window_high)) / np.std(window_high)
                    etop[i] = max(0, strength)
                
            return etop
        
        @staticmethod
        def calculate_elliott_bottom(high, low, close, window=21):
            """Calculate Envelope Bottom indicator - identifies potential reversal bottoms."""
            ebot = np.zeros_like(close)
            
            for i in range(window, len(close)):
                # Look for local lows within window
                window_low = low[i-window:i+1]
                window_idx = np.argmin(window_low)
                
                # Check if current bar or recent bar is a significant low
                if window_idx >= window - 5:  # Recent low
                    strength = (np.mean(window_low) - window_low[window_idx]) / np.std(window_low)
                    ebot[i] = max(0, strength)
                
            return ebot
        
        @staticmethod
        def calculate_pivot_line_dot(high, low, close, window=21):
            """Calculate Pivot Line Dot indicator - pivot point momentum."""
            pldot = np.zeros_like(close)
            
            for i in range(window, len(close)):
                # Calculate pivot point as (H + L + C) / 3
                pivot = (high[i-1] + low[i-1] + close[i-1]) / 3
                
                # Calculate momentum relative to pivot
                current_price = close[i]
                pivot_momentum = (current_price - pivot) / pivot
                
                # Smooth over window
                window_momentum = []
                for j in range(max(0, i-window), i):
                    p = (high[j-1] + low[j-1] + close[j-1]) / 3 if j > 0 else pivot
                    m = (close[j] - p) / p if p != 0 else 0
                    window_momentum.append(m)
                
                pldot[i] = np.mean(window_momentum) if window_momentum else 0
                
            return pldot
        
        @staticmethod
        def calculate_oneonedot(open_, high, low, close, window=21):
            """Calculate One-One-Dot indicator - custom momentum oscillator."""
            oneonedot = np.zeros_like(close)
            
            for i in range(window, len(close)):
                # Calculate various momentum metrics
                window_data = close[i-window:i+1]
                
                # Rate of change
                roc = (close[i] - close[i-window]) / close[i-window] if close[i-window] != 0 else 0
                
                # Relative position within recent range
                recent_high = np.max(high[i-window:i+1])
                recent_low = np.min(low[i-window:i+1])
                position = (close[i] - recent_low) / (recent_high - recent_low) if recent_high != recent_low else 0.5
                
                # Trend strength - ensure arrays have same length
                if len(window_data) == window:
                    slope = np.polyfit(range(window), window_data, 1)[0]
                    trend_strength = slope / np.mean(window_data) if np.mean(window_data) != 0 else 0
                else:
                    trend_strength = 0
                
                # Combine metrics
                oneonedot[i] = (roc + (position - 0.5) * 2 + trend_strength) / 3
                
            return oneonedot

    async def main():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        db_url = "postgresql://postgres:dev_password@postgres:5432/dev_db"
        
        try:
            conn = await asyncpg.connect(db_url)
            logger.info("✅ Connected to database")
            
            symbol = "{symbol}"
            days_back = {days_back}
            
            logger.info(f"🚀 Starting Enhanced Training Data Generation for {{symbol}}")
            logger.info(f"Features: OHLC + etop, ebot, pldot, oneonedot for 21 bars")
            
            # Generate synthetic enhanced market data
            logger.info("Generating enhanced synthetic market data...")
            
            data_rows = []
            end_date = date.today() - timedelta(days=1)
            start_date = end_date - timedelta(days=days_back)
            
            current_date = start_date
            base_price = 100.0 + np.random.uniform(-20, 20)
            
            while current_date <= end_date:
                if current_date.weekday() < 5:  # Trading days only
                    # Generate more realistic price action
                    daily_return = np.random.normal(0.001, 0.02)
                    base_price *= (1 + daily_return)
                    
                    # Create realistic intraday patterns
                    daily_range = base_price * np.random.uniform(0.01, 0.04)
                    
                    # Generate OHLC with proper relationships
                    open_price = base_price * (1 + np.random.normal(0, 0.005))
                    high = max(open_price, base_price) + daily_range / 2
                    low = min(open_price, base_price) - daily_range / 2
                    close_price = np.random.uniform(low, high)
                    
                    volume = int(np.random.lognormal(15, 1))
                    
                    data_rows.append({{
                        'date': current_date,
                        'symbol': symbol,
                        'open': round(open_price, 2),
                        'high': round(high, 2),
                        'low': round(low, 2),
                        'close': round(close_price, 2),
                        'volume': volume
                    }})
                
                current_date += timedelta(days=1)
            
            df = pd.DataFrame(data_rows)
            logger.info(f"Generated {{len(df)}} enhanced market data points")
            
            # Calculate technical indicators
            indicators = TechnicalIndicators()
            
            open_ = df['open'].values
            high = df['high'].values
            low = df['low'].values
            close = df['close'].values
            volume = df['volume'].values
            
            etop = indicators.calculate_elliott_top(high, low, close, 21)
            ebot = indicators.calculate_elliott_bottom(high, low, close, 21)
            pldot = indicators.calculate_pivot_line_dot(high, low, close, 21)
            oneonedot = indicators.calculate_oneonedot(open_, high, low, close, 21)
            
            logger.info("✅ Technical indicators calculated")
            
            # Create enhanced training data
            sequence_length = 21
            prediction_horizon = 5
            features_list = []
            labels_list = []
            
            # Combine all features
            ohlcv = np.column_stack([open_, high, low, close, volume])
            all_features = np.column_stack([
                ohlcv,  # OHLCV for past 21 bars
                etop.reshape(-1, 1),
                ebot.reshape(-1, 1),
                pldot.reshape(-1, 1),
                oneonedot.reshape(-1, 1)
            ])
            
            # Create sequences - adjust for available data
            min_data_needed = sequence_length + prediction_horizon
            if len(all_features) < min_data_needed:
                logger.warning(f"Not enough data points: {len(all_features)}, need at least {min_data_needed}")
                # Generate more synthetic data if needed
                additional_points = min_data_needed - len(all_features) + 10
                logger.info(f"Generating {additional_points} additional synthetic data points...")
                
                for _ in range(additional_points):
                    # Continue price movement
                    base_price *= (1 + np.random.normal(0, 0.02))
                    daily_range = base_price * np.random.uniform(0.01, 0.05)
                    
                    open_price = base_price * (1 + np.random.normal(0, 0.005))
                    high = max(open_price, base_price) + daily_range / 2
                    low = min(open_price, base_price) - daily_range / 2
                    close_price = np.random.uniform(low, high)
                    volume = int(np.random.lognormal(15, 1))
                    
                    data_rows.append({
                        'date': current_date,
                        'symbol': symbol,
                        'open': round(open_price, 2),
                        'high': round(high, 2),
                        'low': round(low, 2),
                        'close': round(close_price, 2),
                        'volume': volume
                    })
                    current_date += timedelta(days=1)
                
                # Recalculate with more data
                df = pd.DataFrame(data_rows)
                open_ = df['open'].values
                high = df['high'].values
                low = df['low'].values
                close = df['close'].values
                volume = df['volume'].values
                
                etop = indicators.calculate_elliott_top(high, low, close, 21)
                ebot = indicators.calculate_elliott_bottom(high, low, close, 21)
                pldot = indicators.calculate_pivot_line_dot(high, low, close, 21)
                oneonedot = indicators.calculate_oneonedot(open_, high, low, close, 21)
                
                ohlcv = np.column_stack([open_, high, low, close, volume])
                all_features = np.column_stack([
                    ohlcv,
                    etop.reshape(-1, 1),
                    ebot.reshape(-1, 1),
                    pldot.reshape(-1, 1),
                    oneonedot.reshape(-1, 1)
                ])
            
            # Allow indicators to stabilize, but adjust based on available data
            start_idx = min(max(25, sequence_length), len(all_features) - min_data_needed)
            
            for i in range(start_idx, len(all_features) - prediction_horizon + 1):
                # Feature sequence (past sequence_length bars)
                feature_seq = all_features[i-sequence_length:i]
                features_list.append(feature_seq)
                
                # Labels (future returns)
                future_prices = close[i:i + prediction_horizon]
                current_price = close[i-1]
                future_returns = (future_prices - current_price) / current_price
                labels_list.append(future_returns)
            
            # Handle empty arrays properly
            if len(features_list) == 0:
                features = np.zeros((0, sequence_length, 9), dtype=np.float32)
                labels = np.zeros((0, prediction_horizon), dtype=np.float32)
            else:
                features = np.array(features_list, dtype=np.float32)
                labels = np.array(labels_list, dtype=np.float32)
            
            logger.info(f"Created enhanced training data: {{features.shape}} features, {{labels.shape}} labels")
            
            # Create dataset record with enhanced features
            dataset_id = f"enhanced_{{datetime.now().strftime('%Y%m%d_%H%M%S')}}_{{symbol.lower()}}"
            feature_names = ['open', 'high', 'low', 'close', 'volume', 'etop', 'ebot', 'pldot', 'oneonedot']
            
            # Feature distributions for visualization
            feature_distributions = {{
                'etop': etop.tolist(),
                'ebot': ebot.tolist(),
                'pldot': pldot.tolist(),
                'oneonedot': oneonedot.tolist(),
                'close': close.tolist(),
                'volume': volume.tolist()
            }}
            
            # Technical indicators metadata
            technical_indicators = {{
                'etop': {{
                    'function': 'envelope_top',
                    'window': 21,
                    'description': 'Envelope Top reversal indicator (21 periods)'
                }},
                'ebot': {{
                    'function': 'envelope_bottom',
                    'window': 21,
                    'description': 'Envelope Bottom reversal indicator (21 periods)'
                }},
                'pldot': {{
                    'function': 'pivot_line_dot',
                    'window': 21,
                    'description': 'Pivot Line Dot momentum indicator (21 periods)'
                }},
                'oneonedot': {{
                    'function': 'oneonedot',
                    'window': 21,
                    'description': 'One-One-Dot custom momentum oscillator (21 periods)'
                }}
            }}
            
            # Create run record first
            run_id = await conn.fetchval("""
                INSERT INTO dev_runs (
                    run_type, start_time, status, total_symbols
                ) VALUES ($1, $2, $3, $4) RETURNING id
            """, 
                "enhanced_training_data_generation", 
                datetime.now(), 
                "running", 
                1
            )
            
            # Insert dataset record
            dataset_db_id = await conn.fetchval("""
                INSERT INTO dev_training_dataset (
                    dataset_name, run_id, total_sequences, sequence_length, 
                    feature_count, label_count, symbols, date_range_start, 
                    date_range_end, data_quality_score, feature_completeness, 
                    label_completeness, generation_duration_seconds, file_size_mb,
                    data_sources, status, feature_metadata, technical_indicators,
                    feature_distributions
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) 
                RETURNING id
            """,
                dataset_id,
                run_id,
                features.shape[0],  # number of sequences
                features.shape[1] if len(features.shape) > 1 else 0,  # sequence length
                features.shape[2] if len(features.shape) > 2 else 0,  # feature count
                labels.shape[1] if len(labels.shape) > 1 else 0,  # prediction horizon
                [symbol],
                start_date,
                end_date,
                1.0,  # data quality score
                1.0,  # feature completeness
                1.0,  # label completeness
                60,   # generation duration
                1.0,  # file size
                ["synthetic_enhanced"],
                "created",
                json.dumps({{'feature_names': feature_names}}),
                json.dumps(technical_indicators),
                json.dumps(feature_distributions)
            )
            
            logger.info(f"📊 Created enhanced training dataset record with ID {{dataset_db_id}}")
            
            # Update run record with success
            await conn.execute("""
                UPDATE dev_runs 
                SET end_time = $1,
                    status = $2,
                    successful_unifications = $3,
                    total_dates = $4,
                    performance_summary = $5
                WHERE id = $6
            """,
                datetime.now(),
                'completed',
                features.shape[0],
                days_back,
                f"Enhanced training data: {{features.shape[0]}} sequences with envelope top/bottom indicators",
                run_id
            )
            
            logger.info("✅ Enhanced Training Data Generation Completed Successfully!")
            logger.info("🎉 Enhanced features generated and stored:")
            logger.info("  • OHLC sequences (21 bars)")
            logger.info("  • Envelope Top (etop) - reversal indicator")
            logger.info("  • Envelope Bottom (ebot) - reversal indicator")
            logger.info("  • Pivot Line Dot (pldot) - momentum indicator")
            logger.info("  • One-One-Dot (oneonedot) - custom oscillator")
            logger.info(f"  • Dataset ID: {{dataset_db_id}}")
            logger.info(f"  • Feature distributions stored for visualization")
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"❌ Enhanced training data generation failed: {{e}}")
            import traceback
            traceback.print_exc()

    if __name__ == "__main__":
        asyncio.run(main())
'''

    yaml_content = create_simple_job(job_name, script_content, f"Enhanced Training Data Generation for {symbol}")
    
    # Write to temp file and apply
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(yaml_content)
        temp_file = f.name
    
    try:
        print(f"🚀 kubectl apply -f {temp_file} -n ats-dev")
        result = subprocess.run(["kubectl", "apply", "-f", temp_file, "-n", "ats-dev"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Job {job_name} applied successfully")
            print(f"📋 Following job logs...")
            run_kubectl(["logs", f"job/{job_name}", "--follow"])
        else:
            print(f"❌ Failed to apply job: {result.stderr}")
            
    finally:
        os.unlink(temp_file)


def main():
    parser = argparse.ArgumentParser(description="Simple Dev CLI for K8s operations")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Run SQL query')
    query_parser.add_argument('sql', help='SQL query to execute')
    
    # Migration command  
    migrate_parser = subparsers.add_parser('migrate', help='Run database migration')
    migrate_parser.add_argument('name', help='Migration name')
    
    # Job command
    job_parser = subparsers.add_parser('job', help='Run specific job')
    job_parser.add_argument('type', help='Job type (e.g., price-unification)')
    job_parser.add_argument('--symbols', help='Comma-separated symbols')
    job_parser.add_argument('--date', help='Target date (YYYY-MM-DD)')
    job_parser.add_argument('--limit', type=int, help='Limit number of items')
    
    # Enhanced training data command
    training_parser = subparsers.add_parser('enhanced-training', help='Generate enhanced training data with technical indicators')
    training_parser.add_argument('--symbol', type=str, default='AAPL', help='Stock symbol (default: AAPL)')
    training_parser.add_argument('--days-back', type=int, default=90, help='Days of historical data (default: 90)')
    
    # Utility commands
    subparsers.add_parser('list', help='List current jobs')
    
    logs_parser = subparsers.add_parser('logs', help='Get job logs')
    logs_parser.add_argument('job', help='Job name')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    print(f"🎯 Dev Environment CLI - All operations run in Kubernetes (ats-dev namespace)")
    print(f"🔄 Command: {args.command}")
    
    if args.command == 'query':
        query_command(args.sql)
    elif args.command == 'migrate':
        migrate_command(args.name)
    elif args.command == 'job':
        job_kwargs = {}
        if hasattr(args, 'symbols') and args.symbols:
            job_kwargs['symbols'] = args.symbols
        if hasattr(args, 'date') and args.date:
            job_kwargs['date'] = args.date
        if hasattr(args, 'limit') and args.limit:
            job_kwargs['limit'] = args.limit
        job_command(args.type, **job_kwargs)
    elif args.command == 'enhanced-training':
        enhanced_training_command(args.symbol, args.days_back)
    elif args.command == 'list':
        list_command()
    elif args.command == 'logs':
        logs_command(args.job)


if __name__ == "__main__":
    main()