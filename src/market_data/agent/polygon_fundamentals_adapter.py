import os
import requests
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from .base_adapter import VendorAdapter
import logging

logger = logging.getLogger(__name__)

@dataclass
class FundamentalData:
    symbol: str
    date: date
    vendor: str
    fiscal_period: str
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


class PolygonFundamentalsAdapter(VendorAdapter):
    vendor_name = "polygon"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise Exception("Please set your POLYGON_API_KEY environment variable or pass api_key explicitly.")
        
        # Rate limiting
        self.request_delay = 12.0  # 5 requests per minute = 12 second delay
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
                logger.warning(f"Rate limited by Polygon API")
                raise Exception("Rate limited")
            elif response.status_code != 200:
                logger.error(f"Polygon API error {response.status_code}: {response.text}")
                raise Exception(f"API error: {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    async def fetch_fundamentals(self, symbol: str, start_date: date, end_date: date) -> List[FundamentalData]:
        """Fetch fundamental data from Polygon API."""
        fundamentals = []
        
        try:
            # Get financials (income statement, balance sheet, cash flow)
            await self._rate_limit()
            financials_url = f"https://api.polygon.io/vX/reference/financials?ticker={symbol}&timeframe=annual&apikey={self.api_key}&limit=100"
            financials_data = self._make_request(financials_url)
            
            for result in financials_data.get('results', []):
                try:
                    # Parse date from filing
                    filing_date = datetime.strptime(result['filing_date'], '%Y-%m-%d').date()
                    
                    # Skip if outside date range
                    if not (start_date <= filing_date <= end_date):
                        continue
                    
                    # Extract financial metrics
                    financials = result.get('financials', {})
                    income_statement = financials.get('income_statement', {})
                    balance_sheet = financials.get('balance_sheet', {})
                    cash_flow = financials.get('cash_flow_statement', {})
                    
                    fundamental = FundamentalData(
                        symbol=symbol,
                        date=filing_date,
                        vendor=self.vendor_name,
                        fiscal_period='FY',  # Annual data
                        revenue=income_statement.get('revenues', {}).get('value'),
                        gross_profit=income_statement.get('gross_profit', {}).get('value'),
                        operating_income=income_statement.get('operating_income_loss', {}).get('value'),
                        net_income=income_statement.get('net_income_loss', {}).get('value'),
                        total_assets=balance_sheet.get('assets', {}).get('value'),
                        total_liabilities=balance_sheet.get('liabilities', {}).get('value'),
                        shareholders_equity=balance_sheet.get('equity', {}).get('value'),
                        current_assets=balance_sheet.get('current_assets', {}).get('value'),
                        current_liabilities=balance_sheet.get('current_liabilities', {}).get('value'),
                        operating_cash_flow=cash_flow.get('net_cash_flow_from_operating_activities', {}).get('value'),
                        investing_cash_flow=cash_flow.get('net_cash_flow_from_investing_activities', {}).get('value'),
                        financing_cash_flow=cash_flow.get('net_cash_flow_from_financing_activities', {}).get('value'),
                        raw_data=result
                    )
                    
                    # Calculate derived metrics
                    if fundamental.current_assets and fundamental.current_liabilities and fundamental.current_liabilities > 0:
                        fundamental.current_ratio = fundamental.current_assets / fundamental.current_liabilities
                    
                    if fundamental.net_income and fundamental.total_assets and fundamental.total_assets > 0:
                        fundamental.roa = fundamental.net_income / fundamental.total_assets
                    
                    if fundamental.net_income and fundamental.shareholders_equity and fundamental.shareholders_equity > 0:
                        fundamental.roe = fundamental.net_income / fundamental.shareholders_equity
                    
                    fundamentals.append(fundamental)
                    
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Error parsing Polygon fundamental data for {symbol}: {e}")
                    continue
            
            # Quarterly data (if available)
            await self._rate_limit()
            quarterly_url = f"https://api.polygon.io/vX/reference/financials?ticker={symbol}&timeframe=quarterly&apikey={self.api_key}&limit=120"
            quarterly_data = self._make_request(quarterly_url)
            
            for result in quarterly_data.get('results', []):
                try:
                    filing_date = datetime.strptime(result['filing_date'], '%Y-%m-%d').date()
                    
                    if not (start_date <= filing_date <= end_date):
                        continue
                    
                    financials_q = result.get('financials', {})
                    income_statement_q = financials_q.get('income_statement', {})
                    balance_sheet_q = financials_q.get('balance_sheet', {})
                    
                    fundamental_q = FundamentalData(
                        symbol=symbol,
                        date=filing_date,
                        vendor=self.vendor_name,
                        fiscal_period='Q',  # Quarterly data
                        revenue=income_statement_q.get('revenues', {}).get('value'),
                        gross_profit=income_statement_q.get('gross_profit', {}).get('value'),
                        operating_income=income_statement_q.get('operating_income_loss', {}).get('value'),
                        net_income=income_statement_q.get('net_income_loss', {}).get('value'),
                        total_assets=balance_sheet_q.get('assets', {}).get('value'),
                        total_liabilities=balance_sheet_q.get('liabilities', {}).get('value'),
                        shareholders_equity=balance_sheet_q.get('equity', {}).get('value'),
                        raw_data=result
                    )
                    
                    fundamentals.append(fundamental_q)
                    
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Error parsing Polygon quarterly data for {symbol}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to fetch Polygon fundamentals for {symbol}: {e}")
        
        logger.info(f"Fetched {len(fundamentals)} fundamental records for {symbol} from Polygon")
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