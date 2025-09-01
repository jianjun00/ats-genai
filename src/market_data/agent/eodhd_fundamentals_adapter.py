import os
import requests
import asyncio
from datetime import datetime, date
from typing import List, Optional, Dict
from .polygon_fundamentals_adapter import FundamentalData
from .base_adapter import VendorAdapter
import logging

logger = logging.getLogger(__name__)


class EODHDFundamentalsAdapter(VendorAdapter):
    vendor_name = "eodhd"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("EODHD_API_KEY")
        if not self.api_key:
            raise Exception("Please set your EODHD_API_KEY environment variable or pass api_key explicitly.")
        
        # Rate limiting for EODHD
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
                logger.warning(f"Rate limited by EODHD API")
                raise Exception("Rate limited")
            elif response.status_code != 200:
                logger.error(f"EODHD API error {response.status_code}: {response.text}")
                raise Exception(f"API error: {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    async def fetch_fundamentals(self, symbol: str, start_date: date, end_date: date) -> List[FundamentalData]:
        """Fetch fundamental data from EODHD API."""
        fundamentals = []
        
        try:
            # EODHD Fundamentals API
            await self._rate_limit()
            fundamentals_url = f"https://eodhd.com/api/fundamentals/{symbol}.US?api_token={self.api_key}"
            fundamentals_data = self._make_request(fundamentals_url)
            
            # Extract annual financials
            financials = fundamentals_data.get('Financials', {})
            
            # Process Income Statements
            income_statements = financials.get('Income_Statement', {})
            if income_statements:
                for year, data in income_statements.get('yearly', {}).items():
                    try:
                        report_date = datetime.strptime(f"{year}-12-31", '%Y-%m-%d').date()
                        
                        # Skip if outside date range
                        if not (start_date <= report_date <= end_date):
                            continue
                        
                        # Get corresponding balance sheet and cash flow data
                        balance_sheet = financials.get('Balance_Sheet', {}).get('yearly', {}).get(year, {})
                        cash_flow = financials.get('Cash_Flow', {}).get('yearly', {}).get(year, {})
                        
                        fundamental = FundamentalData(
                            symbol=symbol,
                            date=report_date,
                            vendor=self.vendor_name,
                            fiscal_period='FY',
                            revenue=data.get('totalRevenue'),
                            gross_profit=data.get('grossProfit'),
                            operating_income=data.get('operatingIncome'),
                            net_income=data.get('netIncome'),
                            ebitda=data.get('ebitda'),
                            total_assets=balance_sheet.get('totalAssets'),
                            total_liabilities=balance_sheet.get('totalLiabilities'),
                            shareholders_equity=balance_sheet.get('totalStockholdersEquity'),
                            current_assets=balance_sheet.get('totalCurrentAssets'),
                            current_liabilities=balance_sheet.get('totalCurrentLiabilities'),
                            total_debt=balance_sheet.get('totalDebt'),
                            operating_cash_flow=cash_flow.get('operatingCashFlow'),
                            investing_cash_flow=cash_flow.get('investingCashFlow'),
                            financing_cash_flow=cash_flow.get('financingCashFlow'),
                            free_cash_flow=cash_flow.get('freeCashFlow'),
                            raw_data={
                                'income': data,
                                'balance': balance_sheet,
                                'cashflow': cash_flow,
                                'year': year
                            }
                        )
                        
                        # Calculate derived metrics
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
                        logger.warning(f"Error parsing EODHD fundamental data for {symbol} year {year}: {e}")
                        continue
            
            # Process quarterly data if available
            quarterly_income = income_statements.get('quarterly', {})
            if quarterly_income:
                for quarter, data in quarterly_income.items():
                    try:
                        # Parse quarter date (format: YYYY-MM-DD)
                        report_date = datetime.strptime(quarter, '%Y-%m-%d').date()
                        
                        # Skip if outside date range
                        if not (start_date <= report_date <= end_date):
                            continue
                        
                        fundamental_q = FundamentalData(
                            symbol=symbol,
                            date=report_date,
                            vendor=self.vendor_name,
                            fiscal_period='Q',
                            revenue=data.get('totalRevenue'),
                            gross_profit=data.get('grossProfit'),
                            operating_income=data.get('operatingIncome'),
                            net_income=data.get('netIncome'),
                            ebitda=data.get('ebitda'),
                            raw_data=data
                        )
                        
                        fundamentals.append(fundamental_q)
                        
                    except (KeyError, ValueError, TypeError) as e:
                        logger.warning(f"Error parsing EODHD quarterly data for {symbol} quarter {quarter}: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Failed to fetch EODHD fundamentals for {symbol}: {e}")
        
        logger.info(f"Fetched {len(fundamentals)} fundamental records for {symbol} from EODHD")
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