#!/usr/bin/env python3
"""
AAPL/TSLA Custom Backtest Runner

Runs a backtest on a custom universe containing only AAPL and TSLA
with 2020-2023 warm-up period and 2023-2025 backtest period.
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
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dao.daily_price_dao import DailyPriceDAO
from dao.universe_dao import UniverseDAO
from config.database import get_database_connection
from config.environment import get_connection_params
import asyncpg

class AAPLTSLABacktestRunner:
    """Custom backtest runner for AAPL and TSLA"""
    
    def __init__(self, db_url: str = None):
        self.db_url = db_url or self._get_db_url()
        self.logger = logging.getLogger(__name__)
        
        # Initialize DAOs
        self.price_dao = DailyPriceDAO()
        self.universe_dao = UniverseDAO()
        
    def _get_db_url(self):
        """Get database URL from environment"""
        db_params = get_connection_params()
        return f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    
    async def run_custom_backtest(self, 
                                warmup_start: date = date(2020, 1, 1),
                                warmup_end: date = date(2022, 12, 31),
                                backtest_start: date = date(2023, 1, 1),
                                backtest_end: date = date.today(),
                                initial_capital: float = 100000.0) -> Dict:
        """Run AAPL/TSLA backtest with warm-up period"""
        
        self.logger.info(f"🚀 Starting AAPL/TSLA backtest")
        self.logger.info(f"📅 Warm-up: {warmup_start} to {warmup_end}")
        self.logger.info(f"📈 Backtest: {backtest_start} to {backtest_end}")
        
        # Use our custom universe
        universe = ['AAPL', 'TSLA']
        self.logger.info(f"🎯 Universe: {universe}")
        
        # Step 1: Fetch warm-up data 
        warmup_data = await self._fetch_market_data(universe, warmup_start, warmup_end)
        if warmup_data.empty:
            self.logger.error("❌ No warm-up data available")
            return {}
        self.logger.info(f"📊 Warm-up data: {len(warmup_data)} records")
        
        # Step 2: Fetch backtest data
        backtest_data = await self._fetch_market_data(universe, backtest_start, backtest_end) 
        if backtest_data.empty:
            self.logger.error("❌ No backtest data available")
            return {}
        self.logger.info(f"📈 Backtest data: {len(backtest_data)} records")
        
        # Step 3: Run simple momentum strategy
        run_id = str(uuid.uuid4())
        results = await self._run_momentum_strategy(
            run_id, warmup_data, backtest_data, backtest_start, backtest_end, initial_capital
        )
        
        # Step 4: Store results
        await self._store_results(run_id, results, "momentum")
        
        # Step 5: Generate summary
        summary = {
            'run_id': run_id,
            'universe': universe,
            'warmup_period': {'start': warmup_start, 'end': warmup_end},
            'backtest_period': {'start': backtest_start, 'end': backtest_end},
            'initial_capital': initial_capital,
            'metrics': self._calculate_metrics(results)
        }
        
        self.logger.info("✅ AAPL/TSLA backtest completed!")
        return summary
    
    async def _fetch_market_data(self, universe: List[str], start_date: date, end_date: date) -> pd.DataFrame:
        """Fetch market data using existing DAO"""
        try:
            all_data = []
            
            for symbol in universe:
                try:
                    # Fetch data for each symbol
                    price_data = await self.price_dao.get_prices_for_symbol_date_range(
                        symbol, start_date, end_date
                    )
                    
                    if price_data:
                        # Convert to DataFrame format
                        df = pd.DataFrame([
                            {
                                'symbol': symbol,
                                'date': record['date'],
                                'open': record['open_price'],
                                'high': record['high_price'],
                                'low': record['low_price'],
                                'close': record['close_price'],
                                'volume': record['volume'],
                                'adj_close': record.get('adj_close', record['close_price'])
                            }
                            for record in price_data
                        ])
                        all_data.append(df)
                        self.logger.info(f"📊 {symbol}: {len(df)} price records")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to fetch data for {symbol}: {e}")
                    continue
            
            if all_data:
                # Combine all data
                combined_data = pd.concat(all_data, ignore_index=True)
                combined_data['date'] = pd.to_datetime(combined_data['date'])
                
                # Add technical indicators
                combined_data = self._add_technical_indicators(combined_data)
                
                return combined_data
            
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error fetching market data: {e}")
            return pd.DataFrame()
    
    def _add_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add technical indicators"""
        
        result_dfs = []
        
        for symbol in df['symbol'].unique():
            symbol_df = df[df['symbol'] == symbol].copy().sort_values('date')
            
            if len(symbol_df) < 50:  # Need enough data for indicators
                continue
            
            # Returns
            symbol_df['returns'] = symbol_df['close'].pct_change()
            
            # Simple moving averages
            symbol_df['sma_10'] = symbol_df['close'].rolling(10).mean()
            symbol_df['sma_20'] = symbol_df['close'].rolling(20).mean()
            symbol_df['sma_50'] = symbol_df['close'].rolling(50).mean()
            
            # Momentum
            symbol_df['momentum_5'] = symbol_df['close'] / symbol_df['close'].shift(5) - 1
            symbol_df['momentum_10'] = symbol_df['close'] / symbol_df['close'].shift(10) - 1
            symbol_df['momentum_20'] = symbol_df['close'] / symbol_df['close'].shift(20) - 1
            
            # Volatility
            symbol_df['volatility_20d'] = symbol_df['returns'].rolling(20).std()
            
            # Volume indicators
            symbol_df['volume_sma_20'] = symbol_df['volume'].rolling(20).mean()
            symbol_df['volume_ratio'] = symbol_df['volume'] / symbol_df['volume_sma_20']
            
            # RSI
            symbol_df['rsi_14'] = self._calculate_rsi(symbol_df['close'], 14)
            
            result_dfs.append(symbol_df)
        
        if result_dfs:
            return pd.concat(result_dfs, ignore_index=True)
        return df
    
    def _calculate_rsi(self, prices: pd.Series, window: int = 14) -> pd.Series:
        """Calculate RSI indicator"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))
    
    async def _run_momentum_strategy(self, run_id: str, warmup_data: pd.DataFrame, 
                                   backtest_data: pd.DataFrame, start_date: date, 
                                   end_date: date, initial_capital: float) -> Dict:
        """Run simple momentum strategy"""
        
        self.logger.info("📈 Running momentum strategy...")
        
        # Combine data for full context
        all_data = pd.concat([warmup_data, backtest_data], ignore_index=True).sort_values(['symbol', 'date'])
        
        # Get trading data (backtest period only)
        trading_data = all_data[
            (all_data['date'] >= pd.Timestamp(start_date)) & 
            (all_data['date'] <= pd.Timestamp(end_date))
        ].sort_values('date')
        
        portfolio_value = initial_capital
        cash = initial_capital
        positions = {}
        trades = []
        daily_performance = {}
        
        trading_dates = sorted(trading_data['date'].unique())
        
        for i, current_date in enumerate(trading_dates):
            current_data = trading_data[trading_data['date'] == current_date]
            
            if current_data.empty:
                continue
            
            # Generate signals for each symbol
            for _, row in current_data.iterrows():
                symbol = row['symbol']
                current_price = row['close']
                
                # Skip if missing data
                if pd.isna(current_price) or pd.isna(row.get('momentum_20', 0)):
                    continue
                
                # Simple momentum strategy
                momentum_20 = row.get('momentum_20', 0)
                momentum_10 = row.get('momentum_10', 0) 
                rsi = row.get('rsi_14', 50)
                volume_ratio = row.get('volume_ratio', 1)
                
                # Entry signals
                if symbol not in positions:
                    # Long entry: Strong momentum + not overbought + volume confirmation
                    if (momentum_20 > 0.1 and momentum_10 > 0.05 and 
                        rsi < 70 and volume_ratio > 1.2):
                        
                        # Use 40% of available cash for each position
                        position_size = cash * 0.4
                        shares = int(position_size / current_price)
                        
                        if shares > 0 and cash >= shares * current_price:
                            cost = shares * current_price
                            positions[symbol] = {
                                'shares': shares,
                                'entry_price': current_price,
                                'entry_date': current_date.date(),
                                'signal_type': 'momentum_long',
                                'entry_momentum': momentum_20
                            }
                            cash -= cost
                            
                            self.logger.info(f"🟢 LONG {symbol}: {shares} shares @ ${current_price:.2f}")
                
                # Exit signals 
                elif symbol in positions:
                    position = positions[symbol]
                    pnl_pct = (current_price - position['entry_price']) / position['entry_price']
                    
                    should_exit = False
                    exit_reason = ""
                    
                    # Take profit
                    if pnl_pct > 0.20:
                        should_exit = True
                        exit_reason = "take_profit_20"
                    # Stop loss
                    elif pnl_pct < -0.10:
                        should_exit = True
                        exit_reason = "stop_loss_10"
                    # Momentum reversal
                    elif momentum_20 < -0.05:
                        should_exit = True
                        exit_reason = "momentum_reversal"
                    # Time-based exit (max 30 days)
                    elif (current_date.date() - position['entry_date']).days > 30:
                        should_exit = True
                        exit_reason = "time_exit_30d"
                    
                    if should_exit:
                        # Exit position
                        proceeds = position['shares'] * current_price
                        pnl = proceeds - (position['shares'] * position['entry_price'])
                        
                        trades.append({
                            'trade_id': str(uuid.uuid4()),
                            'symbol': symbol,
                            'entry_date': position['entry_date'],
                            'exit_date': current_date.date(),
                            'entry_price': position['entry_price'],
                            'exit_price': current_price,
                            'quantity': position['shares'],
                            'side': 'long',
                            'signal_type': position['signal_type'],
                            'pnl': pnl,
                            'pnl_percent': pnl_pct,
                            'exit_reason': exit_reason,
                            'days_held': (current_date.date() - position['entry_date']).days
                        })
                        
                        cash += proceeds
                        del positions[symbol]
                        
                        self.logger.info(f"🔴 EXIT {symbol}: {position['shares']} shares @ ${current_price:.2f} | PnL: ${pnl:.2f} ({pnl_pct*100:.1f}%) | {exit_reason}")
            
            # Calculate portfolio value
            position_value = sum(
                pos['shares'] * current_data[current_data['symbol'] == symbol]['close'].iloc[0]
                for symbol, pos in positions.items()
                if not current_data[current_data['symbol'] == symbol].empty
            )
            portfolio_value = cash + position_value
            
            # Store daily performance
            daily_performance[current_date.date()] = {
                'portfolio_value': portfolio_value,
                'cash': cash,
                'position_value': position_value,
                'positions_count': len(positions),
                'daily_return': 0.0  # Will calculate later
            }
            
            if i % 30 == 0:  # Log every 30 days
                self.logger.info(f"📊 {current_date.date()}: Portfolio ${portfolio_value:,.0f} | Cash ${cash:,.0f} | Positions: {len(positions)}")
        
        # Calculate daily returns
        prev_value = initial_capital
        for date_key in sorted(daily_performance.keys()):
            current_value = daily_performance[date_key]['portfolio_value']
            daily_return = (current_value / prev_value) - 1
            daily_performance[date_key]['daily_return'] = daily_return
            prev_value = current_value
        
        return {
            'portfolio_performance': daily_performance,
            'trades': trades,
            'final_portfolio_value': portfolio_value,
            'final_cash': cash,
            'final_positions': positions
        }
    
    def _calculate_metrics(self, results: Dict) -> Dict:
        """Calculate performance metrics"""
        
        portfolio_performance = results.get('portfolio_performance', {})
        trades = results.get('trades', [])
        
        if not portfolio_performance:
            return {}
        
        values = [perf['portfolio_value'] for perf in portfolio_performance.values()]
        returns = [perf['daily_return'] for perf in portfolio_performance.values()]
        
        if len(values) < 2:
            return {}
        
        # Performance metrics
        total_return = (values[-1] / values[0]) - 1
        annualized_return = (1 + total_return) ** (252/len(values)) - 1 if len(values) > 252 else total_return
        volatility = np.std(returns) * np.sqrt(252) if returns else 0
        sharpe_ratio = annualized_return / volatility if volatility > 0 else 0
        
        # Drawdown calculation
        peak = values[0]
        max_dd = 0
        for value in values:
            if value > peak:
                peak = value
            dd = (value - peak) / peak
            if dd < max_dd:
                max_dd = dd
        
        # Trade statistics
        winning_trades = [t for t in trades if t['pnl'] > 0]
        losing_trades = [t for t in trades if t['pnl'] <= 0]
        
        win_rate = len(winning_trades) / len(trades) if trades else 0
        avg_win = np.mean([t['pnl'] for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t['pnl'] for t in losing_trades]) if losing_trades else 0
        
        profit_factor = abs(sum(t['pnl'] for t in winning_trades) / sum(t['pnl'] for t in losing_trades)) if losing_trades else float('inf')
        
        # Additional stats
        avg_days_held = np.mean([t['days_held'] for t in trades]) if trades else 0
        best_trade = max([t['pnl'] for t in trades]) if trades else 0
        worst_trade = min([t['pnl'] for t in trades]) if trades else 0
        
        return {
            'total_return': round(total_return, 4),
            'annualized_return': round(annualized_return, 4),
            'volatility': round(volatility, 4),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'max_drawdown': round(max_dd, 4),
            'total_trades': len(trades),
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': round(win_rate, 4),
            'avg_win': round(avg_win, 2),
            'avg_loss': round(avg_loss, 2),
            'profit_factor': round(profit_factor, 2),
            'avg_days_held': round(avg_days_held, 1),
            'best_trade': round(best_trade, 2),
            'worst_trade': round(worst_trade, 2),
            'final_value': round(values[-1], 2),
            'total_pnl': round(sum(t['pnl'] for t in trades), 2)
        }
    
    async def _store_results(self, run_id: str, results: Dict, strategy_type: str):
        """Store backtest results in database"""
        conn = await asyncpg.connect(self.db_url)
        try:
            # Check if backtest tables exist
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'backtest_runs'
                )
            """)
            
            if not table_exists:
                self.logger.warning("⚠️  Backtest tables don't exist, skipping database storage")
                return
            
            # Store run metadata
            await conn.execute("""
                INSERT INTO backtest_runs 
                (run_id, strategy_name, strategy_type, start_date, end_date, 
                 universe_size, initial_capital, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (run_id) DO NOTHING
            """, 
                run_id, 
                "AAPL/TSLA Momentum Strategy",
                strategy_type,
                min(results.get('portfolio_performance', {}).keys()) if results.get('portfolio_performance') else date.today(),
                max(results.get('portfolio_performance', {}).keys()) if results.get('portfolio_performance') else date.today(),
                2,  # AAPL + TSLA
                results.get('final_portfolio_value', 100000.0),
                json.dumps({'strategy_type': strategy_type, 'universe': ['AAPL', 'TSLA']})
            )
            
            # Store trades
            for trade in results.get('trades', []):
                await conn.execute("""
                    INSERT INTO trades 
                    (trade_id, backtest_run_id, symbol, entry_date, exit_date,
                     entry_price, exit_price, quantity, side, signal_type,
                     pnl, pnl_percent)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (trade_id) DO NOTHING
                """, 
                    trade['trade_id'], run_id, trade['symbol'], trade['entry_date'], trade['exit_date'],
                    trade['entry_price'], trade['exit_price'], trade['quantity'], trade['side'],
                    trade['signal_type'], trade['pnl'], trade['pnl_percent']
                )
            
            self.logger.info("💾 Results stored in database")
            
        except Exception as e:
            self.logger.warning(f"⚠️  Could not store results in database: {e}")
        finally:
            await conn.close()


async def main():
    """Main function to run AAPL/TSLA backtest"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    runner = AAPLTSLABacktestRunner()
    
    print("🚀 " + "="*60)
    print("   AAPL/TSLA CUSTOM UNIVERSE BACKTEST")
    print("="*62)
    
    try:
        results = await runner.run_custom_backtest(
            warmup_start=date(2020, 1, 1),
            warmup_end=date(2022, 12, 31),
            backtest_start=date(2023, 1, 1),
            backtest_end=date.today(),
            initial_capital=100000.0
        )
        
        if results:
            print("\n✅ AAPL/TSLA BACKTEST COMPLETED!")
            print("="*62)
            print(f"🆔 Run ID: {results['run_id']}")
            print(f"🎯 Universe: {', '.join(results['universe'])}")
            print(f"🔥 Warm-up: {results['warmup_period']['start']} to {results['warmup_period']['end']}")
            print(f"📈 Backtest: {results['backtest_period']['start']} to {results['backtest_period']['end']}")
            print(f"💰 Initial Capital: ${results['initial_capital']:,.0f}")
            print()
            
            # Performance results
            metrics = results['metrics']
            
            print("📊 PERFORMANCE RESULTS:")
            print(f"   💵 Final Value: ${metrics.get('final_value', 0):,.0f}")
            print(f"   📈 Total Return: {metrics.get('total_return', 0)*100:.1f}%")
            print(f"   📆 Annualized Return: {metrics.get('annualized_return', 0)*100:.1f}%")
            print(f"   📉 Max Drawdown: {metrics.get('max_drawdown', 0)*100:.1f}%")
            print(f"   ⚡ Sharpe Ratio: {metrics.get('sharpe_ratio', 0):.2f}")
            print(f"   📊 Volatility: {metrics.get('volatility', 0)*100:.1f}%")
            print()
            
            print("🎯 TRADING STATISTICS:")
            print(f"   📊 Total Trades: {metrics.get('total_trades', 0)}")
            print(f"   ✅ Winning Trades: {metrics.get('winning_trades', 0)}")
            print(f"   ❌ Losing Trades: {metrics.get('losing_trades', 0)}")
            print(f"   🎯 Win Rate: {metrics.get('win_rate', 0)*100:.1f}%")
            print(f"   💰 Profit Factor: {metrics.get('profit_factor', 0):.2f}")
            print(f"   📈 Avg Win: ${metrics.get('avg_win', 0):.2f}")
            print(f"   📉 Avg Loss: ${metrics.get('avg_loss', 0):.2f}")
            print(f"   ⏱️  Avg Days Held: {metrics.get('avg_days_held', 0):.1f}")
            print(f"   🚀 Best Trade: ${metrics.get('best_trade', 0):.2f}")
            print(f"   💥 Worst Trade: ${metrics.get('worst_trade', 0):.2f}")
            print(f"   💵 Total P&L: ${metrics.get('total_pnl', 0):.2f}")
            print()
            
            # Save results
            results_file = Path(__file__).parent / "aapl_tsla_backtest_results.json"
            with open(results_file, 'w') as f:
                # Convert dates to strings for JSON serialization
                json_results = results.copy()
                json_results['warmup_period']['start'] = json_results['warmup_period']['start'].isoformat()
                json_results['warmup_period']['end'] = json_results['warmup_period']['end'].isoformat()
                json_results['backtest_period']['start'] = json_results['backtest_period']['start'].isoformat()
                json_results['backtest_period']['end'] = json_results['backtest_period']['end'].isoformat()
                
                json.dump(json_results, f, indent=2)
            
            print(f"💾 Results saved: {results_file}")
            print("🎉 Backtest completed successfully!")
            
        return 0
        
    except Exception as e:
        print(f"❌ Error running AAPL/TSLA backtest: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))