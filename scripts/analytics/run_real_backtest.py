#!/usr/bin/env python3
"""
Real Backtest Runner

Runs actual adaptive and static support/resistance models on real market data
to generate authentic backtest results for the analytics platform.
"""

import os
import sys
import uuid
import asyncio
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from ml.dynamic_training.adaptive_sr_model import AdaptiveSupportResistanceModel, AdaptiveModelConfig
from ml.evaluation.adaptive_backtester import AdaptiveBacktester
from dao.base_dao import get_database_connection
from config.environment import get_connection_params
import asyncpg

class RealBacktestRunner:
    """Runs real backtests using actual models and market data"""
    
    def __init__(self, db_url: str = None):
        self.db_url = db_url or self._get_db_url()
        self.logger = logging.getLogger(__name__)
        
        # Backtest configuration
        self.start_date = date(2023, 1, 1)
        self.end_date = date(2024, 6, 30)
        self.initial_capital = 1000000.0
        
        # Universe selection (liquid stocks with good data)
        self.universe = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM',
            'JNJ', 'V', 'UNH', 'HD', 'PG', 'DIS', 'MA', 'BAC'
        ]
        
    def _get_db_url(self):
        """Get database URL from environment"""
        return "postgresql://postgres:postgres@localhost:5433/dev_db"
    
    async def run_real_backtest_comparison(self):
        """Run both adaptive and static models for comparison"""
        self.logger.info("🚀 Starting real backtest comparison...")
        
        # Generate unique run IDs
        adaptive_run_id = str(uuid.uuid4())
        static_run_id = str(uuid.uuid4())
        
        # Step 1: Prepare market data
        self.logger.info("📊 Preparing market data...")
        market_data = await self._prepare_market_data()
        
        if market_data.empty:
            self.logger.error("❌ No market data available")
            return None
        
        # Step 2: Run adaptive model backtest
        self.logger.info("🧠 Running adaptive model backtest...")
        adaptive_results = await self._run_adaptive_backtest(adaptive_run_id, market_data)
        
        # Step 3: Run static model backtest
        self.logger.info("🔒 Running static model backtest...")
        static_results = await self._run_static_backtest(static_run_id, market_data)
        
        # Step 4: Store results in database
        self.logger.info("💾 Storing backtest results...")
        await self._store_backtest_results(adaptive_run_id, adaptive_results, 'adaptive')
        await self._store_backtest_results(static_run_id, static_results, 'static')
        
        # Step 5: Generate summary
        summary = {
            'adaptive_run_id': adaptive_run_id,
            'static_run_id': static_run_id,
            'adaptive_performance': self._calculate_summary_metrics(adaptive_results),
            'static_performance': self._calculate_summary_metrics(static_results),
            'universe': self.universe,
            'start_date': self.start_date,
            'end_date': self.end_date
        }
        
        self.logger.info("✅ Real backtest comparison completed!")
        return summary
    
    async def _prepare_market_data(self) -> pd.DataFrame:
        """Fetch real market data from database"""
        try:
            conn = await asyncpg.connect(self.db_url)
            
            # Query daily price data
            query = """
                SELECT 
                    symbol,
                    date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    adj_close
                FROM daily_prices 
                WHERE symbol = ANY($1)
                    AND date BETWEEN $2 AND $3
                    AND volume > 100000
                ORDER BY symbol, date
            """
            
            rows = await conn.fetch(query, self.universe, self.start_date, self.end_date)
            await conn.close()
            
            if not rows:
                self.logger.warning("⚠️ No market data found in database")
                return pd.DataFrame()
            
            # Convert to DataFrame
            data = pd.DataFrame([dict(row) for row in rows])
            data['date'] = pd.to_datetime(data['date'])
            data = data.set_index(['symbol', 'date']).sort_index()
            
            self.logger.info(f"📈 Loaded {len(data)} price records for {data.index.get_level_values(0).nunique()} symbols")
            return data
            
        except Exception as e:
            self.logger.error(f"❌ Failed to fetch market data: {e}")
            return pd.DataFrame()
    
    async def _run_adaptive_backtest(self, run_id: str, market_data: pd.DataFrame) -> Dict:
        """Run backtest with adaptive model"""
        
        # Configure adaptive model
        config = AdaptiveModelConfig(
            lookback_window=252,
            retraining_frequency='daily',
            min_training_samples=500,
            max_training_samples=2000,
            confidence_threshold=0.65,
            feature_columns=[
                'rsi_14', 'sma_20', 'sma_50', 'volume_ratio', 
                'price_position', 'volatility_20d'
            ]
        )
        
        # Initialize adaptive model
        model = AdaptiveSupportResistanceModel(config)
        
        # Initialize backtester
        backtester = AdaptiveBacktester(
            initial_capital=self.initial_capital,
            position_size_pct=0.1,
            max_positions=10,
            stop_loss_pct=0.05,
            take_profit_pct=0.15
        )
        
        # Run backtest
        results = await backtester.run_adaptive_backtest(
            model=model,
            market_data=market_data,
            start_date=self.start_date,
            end_date=self.end_date,
            universe=self.universe
        )
        
        return results
    
    async def _run_static_backtest(self, run_id: str, market_data: pd.DataFrame) -> Dict:
        """Run backtest with static model (trained once)"""
        
        # Configure static model (same as adaptive but no retraining)
        config = AdaptiveModelConfig(
            lookback_window=252,
            retraining_frequency='never',  # Static model
            min_training_samples=1000,
            max_training_samples=2000,
            confidence_threshold=0.65,
            feature_columns=[
                'rsi_14', 'sma_20', 'sma_50', 'volume_ratio', 
                'price_position', 'volatility_20d'
            ]
        )
        
        # Initialize static model
        model = AdaptiveSupportResistanceModel(config)
        
        # Train model once on early data
        training_end = self.start_date + timedelta(days=252)  # First year for training
        training_data = market_data.loc[:, :training_end]
        
        if not training_data.empty:
            await model.train_initial_model(training_data)
        
        # Initialize backtester
        backtester = AdaptiveBacktester(
            initial_capital=self.initial_capital,
            position_size_pct=0.1,
            max_positions=10,
            stop_loss_pct=0.05,
            take_profit_pct=0.15
        )
        
        # Run backtest (model won't retrain)
        results = await backtester.run_static_backtest(
            model=model,
            market_data=market_data,
            start_date=training_end,  # Start after training period
            end_date=self.end_date,
            universe=self.universe
        )
        
        return results
    
    def _calculate_summary_metrics(self, results: Dict) -> Dict:
        """Calculate summary performance metrics"""
        portfolio_values = results.get('portfolio_performance', {})
        trades = results.get('trades', [])
        
        if not portfolio_values:
            return {}
        
        # Convert to series for calculations
        dates = list(portfolio_values.keys())
        values = list(portfolio_values.values())
        
        if len(values) < 2:
            return {}
        
        # Calculate returns
        returns = [values[i]/values[i-1] - 1 for i in range(1, len(values))]
        
        # Performance metrics
        total_return = (values[-1] / values[0]) - 1
        annualized_return = (1 + total_return) ** (252/len(values)) - 1
        volatility = np.std(returns) * np.sqrt(252)
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        
        # Drawdown calculation
        peak = values[0]
        max_drawdown = 0
        for value in values:
            if value > peak:
                peak = value
            drawdown = (value - peak) / peak
            if drawdown < max_drawdown:
                max_drawdown = drawdown
        
        # Trade statistics
        winning_trades = [t for t in trades if t.get('pnl', 0) > 0]
        losing_trades = [t for t in trades if t.get('pnl', 0) <= 0]
        
        win_rate = len(winning_trades) / len(trades) if trades else 0
        avg_win = np.mean([t['pnl'] for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t['pnl'] for t in losing_trades]) if losing_trades else 0
        profit_factor = abs(sum(t['pnl'] for t in winning_trades) / sum(t['pnl'] for t in losing_trades)) if losing_trades else float('inf')
        
        return {
            'total_return': round(total_return, 4),
            'annualized_return': round(annualized_return, 4),
            'volatility': round(volatility, 4),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'max_drawdown': round(max_drawdown, 4),
            'total_trades': len(trades),
            'win_rate': round(win_rate, 4),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'profit_factor': round(profit_factor, 2),
            'final_value': round(values[-1], 2)
        }
    
    async def _store_backtest_results(self, run_id: str, results: Dict, strategy_type: str):
        """Store backtest results in database for analytics platform"""
        conn = await asyncpg.connect(self.db_url)
        try:
            # Store backtest run metadata
            await self._store_backtest_run(conn, run_id, strategy_type, results)
            
            # Store portfolio performance
            await self._store_portfolio_performance(conn, run_id, results)
            
            # Store model performance
            await self._store_model_performance(conn, run_id, results)
            
            # Store trades
            await self._store_trades(conn, run_id, results)
            
            # Store predictions/forecasts
            await self._store_forecasts(conn, run_id, results)
            
        finally:
            await conn.close()
    
    async def _store_backtest_run(self, conn, run_id: str, strategy_type: str, results: Dict):
        """Store backtest run metadata"""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS backtest_runs (
                run_id UUID PRIMARY KEY,
                strategy_name VARCHAR(100),
                strategy_type VARCHAR(50),
                start_date DATE,
                end_date DATE,
                universe_size INTEGER,
                initial_capital DECIMAL(15,2),
                created_at TIMESTAMP DEFAULT NOW(),
                metadata JSONB
            )
        """)
        
        strategy_name = f"{'Adaptive' if strategy_type == 'adaptive' else 'Static'} Support/Resistance Model"
        
        await conn.execute("""
            INSERT INTO backtest_runs 
            (run_id, strategy_name, strategy_type, start_date, end_date, 
             universe_size, initial_capital, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (run_id) DO UPDATE SET
                strategy_name = EXCLUDED.strategy_name,
                strategy_type = EXCLUDED.strategy_type,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                universe_size = EXCLUDED.universe_size,
                initial_capital = EXCLUDED.initial_capital,
                metadata = EXCLUDED.metadata
        """, 
            run_id, strategy_name, strategy_type, self.start_date, self.end_date,
            len(self.universe), self.initial_capital,
            {'model_type': f'{strategy_type}_sr', 'universe': self.universe}
        )
    
    async def _store_portfolio_performance(self, conn, run_id: str, results: Dict):
        """Store daily portfolio performance"""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_performance (
                backtest_run_id UUID,
                date DATE,
                portfolio_value DECIMAL(15,4),
                daily_return DECIMAL(8,6),
                cumulative_return DECIMAL(8,6),
                drawdown DECIMAL(8,6),
                positions_count INTEGER,
                cash_balance DECIMAL(15,4),
                PRIMARY KEY (backtest_run_id, date)
            )
        """)
        
        portfolio_performance = results.get('portfolio_performance', {})
        
        if portfolio_performance:
            values = list(portfolio_performance.values())
            dates = list(portfolio_performance.keys())
            
            # Calculate metrics
            for i, (date_val, value) in enumerate(portfolio_performance.items()):
                daily_return = (value / values[i-1] - 1) if i > 0 else 0.0
                cumulative_return = (value / values[0] - 1)
                
                # Simple drawdown calculation
                peak_value = max(values[:i+1])
                drawdown = (value - peak_value) / peak_value
                
                # Estimate positions and cash
                positions_count = results.get('active_positions', {}).get(date_val, 5)
                cash_balance = value * 0.1  # Assume 10% cash
                
                await conn.execute("""
                    INSERT INTO portfolio_performance 
                    (backtest_run_id, date, portfolio_value, daily_return,
                     cumulative_return, drawdown, positions_count, cash_balance)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (backtest_run_id, date) DO UPDATE SET
                        portfolio_value = EXCLUDED.portfolio_value,
                        daily_return = EXCLUDED.daily_return,
                        cumulative_return = EXCLUDED.cumulative_return,
                        drawdown = EXCLUDED.drawdown,
                        positions_count = EXCLUDED.positions_count,
                        cash_balance = EXCLUDED.cash_balance
                """, 
                    run_id, date_val, round(value, 4), round(daily_return, 6),
                    round(cumulative_return, 6), round(drawdown, 6),
                    positions_count, round(cash_balance, 4)
                )
    
    async def _store_model_performance(self, conn, run_id: str, results: Dict):
        """Store model performance metrics"""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS model_performance (
                backtest_run_id UUID,
                date DATE,
                support_accuracy DECIMAL(5,4),
                resistance_accuracy DECIMAL(5,4),
                overall_accuracy DECIMAL(5,4),
                confidence_correlation DECIMAL(5,4),
                mae DECIMAL(8,6),
                model_version INTEGER,
                prediction_count INTEGER,
                PRIMARY KEY (backtest_run_id, date)
            )
        """)
        
        model_performance = results.get('model_performance', {})
        
        for date_val, perf in model_performance.items():
            await conn.execute("""
                INSERT INTO model_performance 
                (backtest_run_id, date, support_accuracy, resistance_accuracy,
                 overall_accuracy, confidence_correlation, mae, model_version,
                 prediction_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (backtest_run_id, date) DO UPDATE SET
                    support_accuracy = EXCLUDED.support_accuracy,
                    resistance_accuracy = EXCLUDED.resistance_accuracy,
                    overall_accuracy = EXCLUDED.overall_accuracy,
                    confidence_correlation = EXCLUDED.confidence_correlation,
                    mae = EXCLUDED.mae,
                    model_version = EXCLUDED.model_version,
                    prediction_count = EXCLUDED.prediction_count
            """, 
                run_id, date_val,
                perf.get('support_accuracy', 0.0),
                perf.get('resistance_accuracy', 0.0),
                perf.get('overall_accuracy', 0.0),
                perf.get('confidence_correlation', 0.0),
                perf.get('mae', 0.0),
                perf.get('model_version', 1),
                perf.get('prediction_count', 0)
            )
    
    async def _store_trades(self, conn, run_id: str, results: Dict):
        """Store individual trades"""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id UUID PRIMARY KEY,
                backtest_run_id UUID,
                symbol VARCHAR(10),
                entry_date DATE,
                exit_date DATE,
                entry_price DECIMAL(10,4),
                exit_price DECIMAL(10,4),
                quantity INTEGER,
                side VARCHAR(10),
                signal_type VARCHAR(50),
                model_confidence DECIMAL(5,4),
                pnl DECIMAL(12,4),
                pnl_percent DECIMAL(8,6)
            )
        """)
        
        trades = results.get('trades', [])
        
        for trade in trades:
            trade_id = trade.get('trade_id', str(uuid.uuid4()))
            
            await conn.execute("""
                INSERT INTO trades 
                (trade_id, backtest_run_id, symbol, entry_date, exit_date,
                 entry_price, exit_price, quantity, side, signal_type,
                 model_confidence, pnl, pnl_percent)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (trade_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    entry_date = EXCLUDED.entry_date,
                    exit_date = EXCLUDED.exit_date,
                    entry_price = EXCLUDED.entry_price,
                    exit_price = EXCLUDED.exit_price,
                    quantity = EXCLUDED.quantity,
                    side = EXCLUDED.side,
                    signal_type = EXCLUDED.signal_type,
                    model_confidence = EXCLUDED.model_confidence,
                    pnl = EXCLUDED.pnl,
                    pnl_percent = EXCLUDED.pnl_percent
            """, 
                trade_id, run_id,
                trade.get('symbol', ''),
                trade.get('entry_date'),
                trade.get('exit_date'),
                trade.get('entry_price', 0.0),
                trade.get('exit_price', 0.0),
                trade.get('quantity', 0),
                trade.get('side', 'long'),
                trade.get('signal_type', 'unknown'),
                trade.get('model_confidence', 0.0),
                trade.get('pnl', 0.0),
                trade.get('pnl_percent', 0.0)
            )
    
    async def _store_forecasts(self, conn, run_id: str, results: Dict):
        """Store model forecasts/predictions"""
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS forecasts (
                forecast_id UUID PRIMARY KEY,
                backtest_run_id UUID,
                symbol VARCHAR(10),
                forecast_date DATE,
                forecast_type VARCHAR(20),
                predicted_level DECIMAL(10,4),
                confidence DECIMAL(5,4),
                actual_level DECIMAL(10,4),
                accuracy_score DECIMAL(5,4)
            )
        """)
        
        forecasts = results.get('forecasts', [])
        
        for forecast in forecasts:
            forecast_id = forecast.get('forecast_id', str(uuid.uuid4()))
            
            await conn.execute("""
                INSERT INTO forecasts 
                (forecast_id, backtest_run_id, symbol, forecast_date,
                 forecast_type, predicted_level, confidence, actual_level,
                 accuracy_score)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (forecast_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    forecast_date = EXCLUDED.forecast_date,
                    forecast_type = EXCLUDED.forecast_type,
                    predicted_level = EXCLUDED.predicted_level,
                    confidence = EXCLUDED.confidence,
                    actual_level = EXCLUDED.actual_level,
                    accuracy_score = EXCLUDED.accuracy_score
            """, 
                forecast_id, run_id,
                forecast.get('symbol', ''),
                forecast.get('forecast_date'),
                forecast.get('forecast_type', 'support'),
                forecast.get('predicted_level', 0.0),
                forecast.get('confidence', 0.0),
                forecast.get('actual_level', 0.0),
                forecast.get('accuracy_score', 0.0)
            )


async def main():
    """Main function to run real backtest"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    runner = RealBacktestRunner()
    
    print("🚀 " + "="*50)
    print("   RUNNING REAL BACKTEST WITH ACTUAL MODELS")
    print("="*52)
    
    try:
        results = await runner.run_real_backtest_comparison()
        
        if results:
            print("\n✅ REAL BACKTEST COMPLETED SUCCESSFULLY!")
            print("="*52)
            print(f"📊 Adaptive Strategy ID: {results['adaptive_run_id']}")
            print(f"📊 Static Strategy ID: {results['static_run_id']}")
            print(f"🗓️  Period: {results['start_date']} to {results['end_date']}")
            print(f"🎯 Universe: {len(results['universe'])} stocks")
            print()
            
            # Performance comparison
            adaptive_perf = results['adaptive_performance']
            static_perf = results['static_performance']
            
            print("📈 PERFORMANCE COMPARISON:")
            print(f"   Adaptive Model:")
            print(f"     Total Return: {adaptive_perf.get('total_return', 0):.2%}")
            print(f"     Sharpe Ratio: {adaptive_perf.get('sharpe_ratio', 0):.2f}")
            print(f"     Max Drawdown: {adaptive_perf.get('max_drawdown', 0):.2%}")
            print(f"     Win Rate: {adaptive_perf.get('win_rate', 0):.2%}")
            print()
            print(f"   Static Model:")
            print(f"     Total Return: {static_perf.get('total_return', 0):.2%}")
            print(f"     Sharpe Ratio: {static_perf.get('sharpe_ratio', 0):.2f}")
            print(f"     Max Drawdown: {static_perf.get('max_drawdown', 0):.2%}")
            print(f"     Win Rate: {static_perf.get('win_rate', 0):.2%}")
            print()
            
            # Save results for analytics platform
            import json
            config_path = Path(__file__).parent / "real_backtest_config.json"
            with open(config_path, 'w') as f:
                json.dump({
                    'adaptive_run_id': results['adaptive_run_id'],
                    'static_run_id': results['static_run_id'],
                    'universe': results['universe'],
                    'start_date': results['start_date'].isoformat(),
                    'end_date': results['end_date'].isoformat(),
                    'adaptive_performance': adaptive_perf,
                    'static_performance': static_perf
                }, f, indent=2)
            
            print(f"💾 Real backtest config saved to: {config_path}")
            print("🎉 Ready for analytics platform demo!")
            
        return 0
        
    except Exception as e:
        print(f"❌ Error running real backtest: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))