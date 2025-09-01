import os
import requests
import asyncio
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
from .polygon_fundamentals_adapter import FundamentalData
from .base_adapter import VendorAdapter
import logging

logger = logging.getLogger(__name__)


class FMPFundamentalsAdapter(VendorAdapter):
    vendor_name = "fmp"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FMP_API_KEY")
        if not self.api_key:
            raise Exception("Please set your FMP_API_KEY environment variable or pass api_key explicitly.")
        
        # Rate limiting for FMP
        self.request_delay = 1.2  # 250 requests per minute = 1.2 second delay
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
                logger.warning(f"Rate limited by FMP API")
                raise Exception("Rate limited")
            elif response.status_code != 200:
                logger.error(f"FMP API error {response.status_code}: {response.text}")
                raise Exception(f"API error: {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    async def fetch_fundamentals(self, symbol: str, start_date: date, end_date: date) -> List[FundamentalData]:
        """Fetch fundamental data from Financial Modeling Prep API."""
        fundamentals = []
        
        try:
            # Income Statement
            await self._rate_limit()
            income_url = f"https://financialmodelingprep.com/api/v3/income-statement/{symbol}?period=annual&apikey={self.api_key}&limit=30"
            income_data = self._make_request(income_url)
            
            # Balance Sheet
            await self._rate_limit()
            balance_url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{symbol}?period=annual&apikey={self.api_key}&limit=30"
            balance_data = self._make_request(balance_url)
            
            # Cash Flow Statement
            await self._rate_limit()
            cashflow_url = f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{symbol}?period=annual&apikey={self.api_key}&limit=30"
            cashflow_data = self._make_request(cashflow_url)
            
            # Key Metrics
            await self._rate_limit()
            metrics_url = f"https://financialmodelingprep.com/api/v3/key-metrics/{symbol}?period=annual&apikey={self.api_key}&limit=30"
            metrics_data = self._make_request(metrics_url)
            
            # Combine data by date
            income_dict = {item['date']: item for item in income_data} if income_data else {}
            balance_dict = {item['date']: item for item in balance_data} if balance_data else {}
            cashflow_dict = {item['date']: item for item in cashflow_data} if cashflow_data else {}
            metrics_dict = {item['date']: item for item in metrics_data} if metrics_data else {}
            
            # Process all available dates
            all_dates = set()
            all_dates.update(income_dict.keys())
            all_dates.update(balance_dict.keys())
            all_dates.update(cashflow_dict.keys())
            all_dates.update(metrics_dict.keys())
            
            for date_str in all_dates:
                try:
                    report_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    
                    # Skip if outside date range
                    if not (start_date <= report_date <= end_date):
                        continue
                    
                    income = income_dict.get(date_str, {})
                    balance = balance_dict.get(date_str, {})
                    cashflow = cashflow_dict.get(date_str, {})
                    metrics = metrics_dict.get(date_str, {})
                    
                    fundamental = FundamentalData(
                        symbol=symbol,
                        date=report_date,
                        vendor=self.vendor_name,
                        fiscal_period='FY',
                        revenue=income.get('revenue'),
                        gross_profit=income.get('grossProfit'),
                        operating_income=income.get('operatingIncome'),
                        net_income=income.get('netIncome'),
                        ebitda=income.get('ebitda'),
                        eps=income.get('eps'),
                        total_assets=balance.get('totalAssets'),
                        total_liabilities=balance.get('totalLiabilities'),
                        shareholders_equity=balance.get('totalStockholdersEquity'),
                        current_assets=balance.get('totalCurrentAssets'),
                        current_liabilities=balance.get('totalCurrentLiabilities'),
                        total_debt=balance.get('totalDebt'),
                        operating_cash_flow=cashflow.get('operatingCashFlow'),
                        investing_cash_flow=cashflow.get('netCashUsedForInvestingActivites'),
                        financing_cash_flow=cashflow.get('netCashUsedProvidedByFinancingActivities'),
                        free_cash_flow=cashflow.get('freeCashFlow'),
                        market_cap=metrics.get('marketCap'),
                        pe_ratio=metrics.get('peRatio'),
                        pb_ratio=metrics.get('pbRatio'),
                        debt_to_equity=metrics.get('debtToEquity'),
                        roe=metrics.get('roe'),
                        roa=metrics.get('returnOnAssets'),
                        current_ratio=metrics.get('currentRatio'),
                        raw_data={
                            'income': income,
                            'balance': balance,
                            'cashflow': cashflow,
                            'metrics': metrics
                        }
                    )
                    
                    fundamentals.append(fundamental)
                    
                except (KeyError, ValueError, TypeError) as e:
                    logger.warning(f"Error parsing FMP fundamental data for {symbol} on {date_str}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to fetch FMP fundamentals for {symbol}: {e}")
        
        logger.info(f"Fetched {len(fundamentals)} fundamental records for {symbol} from FMP")
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