"""
Tiingo Fundamentals DAO

Handles fundamental data operations for Tiingo vendor data in the comprehensive
fundamentals table with vendor-specific filtering and validation.
"""

from typing import Dict, List, Optional
from datetime import date, datetime
from dataclasses import dataclass
import asyncpg
import logging
from shared.utils.environment import Environment


@dataclass
class TiingoFundamental:
    """Tiingo fundamental data container"""
    symbol: str
    date: date
    vendor: str = 'tiingo'
    fiscal_period: Optional[str] = None
    revenue: Optional[int] = None
    gross_profit: Optional[int] = None
    operating_income: Optional[int] = None
    net_income: Optional[int] = None
    ebitda: Optional[int] = None
    eps: Optional[float] = None
    total_assets: Optional[int] = None
    total_liabilities: Optional[int] = None
    shareholders_equity: Optional[int] = None
    current_assets: Optional[int] = None
    current_liabilities: Optional[int] = None
    total_debt: Optional[int] = None
    cash_and_equivalents: Optional[int] = None
    operating_cash_flow: Optional[int] = None
    investing_cash_flow: Optional[int] = None
    financing_cash_flow: Optional[int] = None
    free_cash_flow: Optional[int] = None
    market_cap: Optional[int] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    raw_data: Optional[Dict] = None


class FundamentalsTiingoDAO:
    """Data Access Object for Tiingo fundamental data"""
    
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('fundamentals_comprehensive')
        self.db_url = self.env.get_database_url()
        self.logger = logging.getLogger(__name__)
        self.vendor = 'tiingo'
    
    async def insert_fundamental(self, fundamental: TiingoFundamental) -> bool:
        """Insert or update Tiingo fundamental data"""
        self.logger.debug(f"Inserting Tiingo fundamental for {fundamental.symbol} on {fundamental.date}")
        
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (
                        symbol, date, vendor, fiscal_period, revenue, gross_profit,
                        operating_income, net_income, ebitda, eps, total_assets,
                        total_liabilities, shareholders_equity, current_assets,
                        current_liabilities, total_debt, cash_and_equivalents,
                        operating_cash_flow, investing_cash_flow, financing_cash_flow,
                        free_cash_flow, market_cap, pe_ratio, pb_ratio, debt_to_equity,
                        roe, roa, current_ratio, quick_ratio, raw_data, created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
                        $27, $28, $29, $30, $31, $32
                    )
                    ON CONFLICT (symbol, date, vendor) DO UPDATE SET
                        fiscal_period = EXCLUDED.fiscal_period,
                        revenue = EXCLUDED.revenue,
                        gross_profit = EXCLUDED.gross_profit,
                        operating_income = EXCLUDED.operating_income,
                        net_income = EXCLUDED.net_income,
                        ebitda = EXCLUDED.ebitda,
                        eps = EXCLUDED.eps,
                        total_assets = EXCLUDED.total_assets,
                        total_liabilities = EXCLUDED.total_liabilities,
                        shareholders_equity = EXCLUDED.shareholders_equity,
                        current_assets = EXCLUDED.current_assets,
                        current_liabilities = EXCLUDED.current_liabilities,
                        total_debt = EXCLUDED.total_debt,
                        cash_and_equivalents = EXCLUDED.cash_and_equivalents,
                        operating_cash_flow = EXCLUDED.operating_cash_flow,
                        investing_cash_flow = EXCLUDED.investing_cash_flow,
                        financing_cash_flow = EXCLUDED.financing_cash_flow,
                        free_cash_flow = EXCLUDED.free_cash_flow,
                        market_cap = EXCLUDED.market_cap,
                        pe_ratio = EXCLUDED.pe_ratio,
                        pb_ratio = EXCLUDED.pb_ratio,
                        debt_to_equity = EXCLUDED.debt_to_equity,
                        roe = EXCLUDED.roe,
                        roa = EXCLUDED.roa,
                        current_ratio = EXCLUDED.current_ratio,
                        quick_ratio = EXCLUDED.quick_ratio,
                        raw_data = EXCLUDED.raw_data,
                        updated_at = CURRENT_TIMESTAMP
                """, 
                fundamental.symbol, fundamental.date, self.vendor, fundamental.fiscal_period,
                fundamental.revenue, fundamental.gross_profit, fundamental.operating_income,
                fundamental.net_income, fundamental.ebitda, fundamental.eps,
                fundamental.total_assets, fundamental.total_liabilities, 
                fundamental.shareholders_equity, fundamental.current_assets,
                fundamental.current_liabilities, fundamental.total_debt,
                fundamental.cash_and_equivalents, fundamental.operating_cash_flow,
                fundamental.investing_cash_flow, fundamental.financing_cash_flow,
                fundamental.free_cash_flow, fundamental.market_cap, fundamental.pe_ratio,
                fundamental.pb_ratio, fundamental.debt_to_equity, fundamental.roe,
                fundamental.roa, fundamental.current_ratio, fundamental.quick_ratio,
                fundamental.raw_data, datetime.now(), datetime.now()
                )
                
                self.logger.info(f"Successfully inserted/updated Tiingo fundamental for {fundamental.symbol}")
                return True
                
        except Exception as e:
            self.logger.error(f"Error inserting Tiingo fundamental for {fundamental.symbol}: {e}")
            return False
        finally:
            await pool.close()
    
    async def get_fundamental(self, symbol: str, date: date) -> Optional[TiingoFundamental]:
        """Get Tiingo fundamental data for a specific symbol and date"""
        self.logger.debug(f"Getting Tiingo fundamental for {symbol} on {date}")
        
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"""
                    SELECT * FROM {self.table_name} 
                    WHERE symbol = $1 AND date = $2 AND vendor = $3
                """, symbol, date, self.vendor)
                
                if row:
                    return TiingoFundamental(
                        symbol=row['symbol'],
                        date=row['date'],
                        vendor=row['vendor'],
                        fiscal_period=row['fiscal_period'],
                        revenue=row['revenue'],
                        gross_profit=row['gross_profit'],
                        operating_income=row['operating_income'],
                        net_income=row['net_income'],
                        ebitda=row['ebitda'],
                        eps=row['eps'],
                        total_assets=row['total_assets'],
                        total_liabilities=row['total_liabilities'],
                        shareholders_equity=row['shareholders_equity'],
                        current_assets=row['current_assets'],
                        current_liabilities=row['current_liabilities'],
                        total_debt=row['total_debt'],
                        cash_and_equivalents=row['cash_and_equivalents'],
                        operating_cash_flow=row['operating_cash_flow'],
                        investing_cash_flow=row['investing_cash_flow'],
                        financing_cash_flow=row['financing_cash_flow'],
                        free_cash_flow=row['free_cash_flow'],
                        market_cap=row['market_cap'],
                        pe_ratio=row['pe_ratio'],
                        pb_ratio=row['pb_ratio'],
                        debt_to_equity=row['debt_to_equity'],
                        roe=row['roe'],
                        roa=row['roa'],
                        current_ratio=row['current_ratio'],
                        quick_ratio=row['quick_ratio'],
                        raw_data=row['raw_data']
                    )
                return None
                
        except Exception as e:
            self.logger.error(f"Error getting Tiingo fundamental for {symbol}: {e}")
            return None
        finally:
            await pool.close()
    
    async def list_fundamentals(self, symbol: str, start_date: Optional[date] = None, 
                               end_date: Optional[date] = None, limit: int = 100) -> List[TiingoFundamental]:
        """List Tiingo fundamentals for a symbol with optional date range"""
        self.logger.debug(f"Listing Tiingo fundamentals for {symbol}")
        
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                query = f"SELECT * FROM {self.table_name} WHERE symbol = $1 AND vendor = $2"
                params = [symbol, self.vendor]
                
                if start_date:
                    query += " AND date >= $3"
                    params.append(start_date)
                    if end_date:
                        query += " AND date <= $4"
                        params.append(end_date)
                elif end_date:
                    query += " AND date <= $3"
                    params.append(end_date)
                
                query += " ORDER BY date DESC"
                
                if limit:
                    query += f" LIMIT ${len(params) + 1}"
                    params.append(limit)
                
                rows = await conn.fetch(query, *params)
                
                fundamentals = []
                for row in rows:
                    fundamentals.append(TiingoFundamental(
                        symbol=row['symbol'],
                        date=row['date'],
                        vendor=row['vendor'],
                        fiscal_period=row['fiscal_period'],
                        revenue=row['revenue'],
                        gross_profit=row['gross_profit'],
                        operating_income=row['operating_income'],
                        net_income=row['net_income'],
                        ebitda=row['ebitda'],
                        eps=row['eps'],
                        total_assets=row['total_assets'],
                        total_liabilities=row['total_liabilities'],
                        shareholders_equity=row['shareholders_equity'],
                        current_assets=row['current_assets'],
                        current_liabilities=row['current_liabilities'],
                        total_debt=row['total_debt'],
                        cash_and_equivalents=row['cash_and_equivalents'],
                        operating_cash_flow=row['operating_cash_flow'],
                        investing_cash_flow=row['investing_cash_flow'],
                        financing_cash_flow=row['financing_cash_flow'],
                        free_cash_flow=row['free_cash_flow'],
                        market_cap=row['market_cap'],
                        pe_ratio=row['pe_ratio'],
                        pb_ratio=row['pb_ratio'],
                        debt_to_equity=row['debt_to_equity'],
                        roe=row['roe'],
                        roa=row['roa'],
                        current_ratio=row['current_ratio'],
                        quick_ratio=row['quick_ratio'],
                        raw_data=row['raw_data']
                    ))
                
                return fundamentals
                
        except Exception as e:
            self.logger.error(f"Error listing Tiingo fundamentals for {symbol}: {e}")
            return []
        finally:
            await pool.close()
    
    async def get_latest_fundamental(self, symbol: str) -> Optional[TiingoFundamental]:
        """Get the most recent Tiingo fundamental data for a symbol"""
        fundamentals = await self.list_fundamentals(symbol, limit=1)
        return fundamentals[0] if fundamentals else None
    
    async def delete_fundamental(self, symbol: str, date: date) -> bool:
        """Delete Tiingo fundamental data for a specific symbol and date"""
        self.logger.debug(f"Deleting Tiingo fundamental for {symbol} on {date}")
        
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(f"""
                    DELETE FROM {self.table_name} 
                    WHERE symbol = $1 AND date = $2 AND vendor = $3
                """, symbol, date, self.vendor)
                
                # Extract number of deleted rows from result string
                deleted_count = int(result.split()[1]) if result.startswith('DELETE') else 0
                
                if deleted_count > 0:
                    self.logger.info(f"Successfully deleted Tiingo fundamental for {symbol}")
                    return True
                else:
                    self.logger.warning(f"No Tiingo fundamental found to delete for {symbol} on {date}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error deleting Tiingo fundamental for {symbol}: {e}")
            return False
        finally:
            await pool.close()
    
    async def get_symbols_with_data(self, start_date: Optional[date] = None, 
                                   end_date: Optional[date] = None) -> List[str]:
        """Get list of symbols that have Tiingo fundamental data"""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                query = f"SELECT DISTINCT symbol FROM {self.table_name} WHERE vendor = $1"
                params = [self.vendor]
                
                if start_date:
                    query += " AND date >= $2"
                    params.append(start_date)
                    if end_date:
                        query += " AND date <= $3"
                        params.append(end_date)
                elif end_date:
                    query += " AND date <= $2"
                    params.append(end_date)
                
                query += " ORDER BY symbol"
                
                rows = await conn.fetch(query, *params)
                return [row['symbol'] for row in rows]
                
        except Exception as e:
            self.logger.error(f"Error getting Tiingo symbols: {e}")
            return []
        finally:
            await pool.close()