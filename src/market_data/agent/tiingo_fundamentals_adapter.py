import os
import requests
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from .polygon_fundamentals_adapter import FundamentalData
from .base_adapter import VendorAdapter
import logging

logger = logging.getLogger(__name__)


class TiingoFundamentalsAdapter(VendorAdapter):
    vendor_name = "tiingo"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("TIINGO_API_KEY")
        if not self.api_key:
            raise Exception("Please set your TIINGO_API_KEY environment variable or pass api_key explicitly.")
        
        # Rate limiting for Tiingo
        self.request_delay = 1.0  # Conservative rate limiting
        self.last_request_time = 0
    
    async def _rate_limit(self):
        """Ensure rate limiting compliance."""
        current_time = asyncio.get_event_loop().time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            sleep_time = self.request_delay - time_since_last
            await asyncio.sleep(sleep_time)
        self.last_request_time = asyncio.get_event_loop().time()

    def _make_request(self, url: str) -> Dict:
        """Make HTTP request with error handling."""
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 429:
                logger.warning(f"Rate limited by Tiingo API")
                raise Exception("Rate limited")
            elif response.status_code != 200:
                logger.error(f"Tiingo API error {response.status_code}: {response.text}")
                raise Exception(f"API error: {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    async def fetch_fundamentals(self, symbol: str, start_date: date, end_date: date) -> List[FundamentalData]:
        """Fetch fundamental data from Tiingo API."""
        fundamentals = []
        
        try:
            # Tiingo Fundamentals Daily API
            await self._rate_limit()
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            fundamentals_url = f"https://api.tiingo.com/tiingo/fundamentals/{symbol}/daily?startDate={start_str}&endDate={end_str}&token={self.api_key}"
            fundamentals_data = self._make_request(fundamentals_url)
            
            for item in fundamentals_data:
                try:
                    report_date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                    
                    # Skip if outside date range
                    if not (start_date <= report_date <= end_date):
                        continue
                    
                    # Extract fundamental metrics from statementData
                    statement_data = item.get('statementData', {})
                    
                    fundamental = FundamentalData(
                        symbol=symbol,
                        date=report_date,
                        vendor=self.vendor_name,
                        fiscal_period=item.get('quarter', 'FY'),
                        revenue=statement_data.get('totalRevenue'),
                        gross_profit=statement_data.get('grossProfit'),
                        operating_income=statement_data.get('operatingIncome'),
                        net_income=statement_data.get('netIncome'),
                        ebitda=statement_data.get('ebitda'),
                        eps=statement_data.get('basicEPS'),
                        total_assets=statement_data.get('totalAssets'),
                        total_liabilities=statement_data.get('totalLiabilities'),
                        shareholders_equity=statement_data.get('totalStockholdersEquity'),
                        current_assets=statement_data.get('totalCurrentAssets'),
                        current_liabilities=statement_data.get('totalCurrentLiabilities'),
                        total_debt=statement_data.get('longTermDebt', 0) + statement_data.get('shortTermDebt', 0) if statement_data.get('longTermDebt') and statement_data.get('shortTermDebt') else None,
                        operating_cash_flow=statement_data.get('operatingCashFlow'),
                        investing_cash_flow=statement_data.get('investingCashFlow'),
                        financing_cash_flow=statement_data.get('financingCashFlow'),
                        free_cash_flow=statement_data.get('freeCashFlow'),
                        market_cap=statement_data.get('marketCap'),
                        raw_data=item
                    )
                    
                    # Calculate derived ratios if data is available
                    if fundamental.current_assets and fundamental.current_liabilities and fundamental.current_liabilities > 0:
                        fundamental.current_ratio = fundamental.current_assets / fundamental.current_liabilities
                        
                    if fundamental.net_income and fundamental.shareholders_equity and fundamental.shareholders_equity > 0:
                        fundamental.roe = fundamental.net_income / fundamental.shareholders_equity
                        
                    if fundamental.net_income and fundamental.total_assets and fundamental.total_assets > 0:
                        fundamental.roa = fundamental.net_income / fundamental.total_assets
                        
                    if fundamental.total_debt and fundamental.shareholders_equity and fundamental.shareholders_equity > 0:
                        fundamental.debt_to_equity = fundamental.total_debt / fundamental.shareholders_equity
                    
                    fundamentals.append(fundamental)
                    
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Error parsing Tiingo fundamental data for {symbol}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to fetch Tiingo fundamentals for {symbol}: {e}")
        
        logger.info(f"Fetched {len(fundamentals)} fundamental records for {symbol} from Tiingo")
        return fundamentals

    def fetch_instruments(self):
        """Not implemented for fundamentals adapter."""
        raise NotImplementedError("Use fetch_fundamentals for fundamental data")
    
    def fetch_eod(self, symbols, start_date, end_date):
        """Not implemented for fundamentals adapter."""
        raise NotImplementedError("Use fetch_fundamentals for fundamental data")
    
    def fetch_ticks(self, symbol, start_dt, end_dt):
        """Not implemented for fundamentals adapter."""
        raise NotImplementedError("Use fetch_fundamentals for fundamental data")
    
    def fetch_interval(self, symbol, interval, start_dt, end_dt):
        """Not implemented for fundamentals adapter.""" 
        raise NotImplementedError("Use fetch_fundamentals for fundamental data")