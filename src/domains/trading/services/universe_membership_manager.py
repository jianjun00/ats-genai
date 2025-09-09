#!/usr/bin/env python3
"""
Universe Membership Manager
Implements correct business logic for universe membership tracking
with proper start_at/end_at dates based on actual qualification criteria
"""

import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import logging

sys.path.append('/home/jianjun/ats-genai-admin/src')

from core.platform.database.connection_manager import get_raw_connection
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

class UniverseMembershipManager:
    """Manages universe membership with correct entry/exit tracking"""
    
    def __init__(self, environment: str = 'dev'):
        """Initialize with environment configuration"""
        self.environment = environment
        self.universe_table = f"{environment}_universe"
        self.membership_table = f"{environment}_universe_membership"
        self.instruments_table = f"{environment}_instruments"
        self.daily_prices_table = f"{environment}_daily_prices_polygon"
        
        # Volume qualification threshold ($100M)
        self.volume_threshold = 100_000_000
        
        # Rolling window for volume calculation (50 trading days)
        self.rolling_window_days = 50
    
    def evaluate_daily_membership(self, evaluation_date: datetime, universe_id: int = 2) -> Dict[str, any]:
        """
        Daily evaluation process to update universe memberships
        This is the CORRECT implementation that should run daily
        """
        logger.info(f"Evaluating universe membership for {evaluation_date.date()}")
        
        with get_raw_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                # Step 1: Calculate current qualifiers based on 50-day rolling volume
                current_qualifiers = self._get_current_qualifiers(cursor, evaluation_date)
                logger.info(f"Found {len(current_qualifiers)} stocks meeting volume criteria")
                
                # Step 2: Get currently active universe members
                active_members = self._get_active_members(cursor, universe_id)
                active_symbols = set(member['symbol'] for member in active_members)
                logger.info(f"Currently {len(active_members)} active universe members")
                
                # Step 3: Process exits (active members who no longer qualify)
                exit_events = []
                for member in active_members:
                    if member['symbol'] not in current_qualifiers:
                        self._process_member_exit(cursor, member, evaluation_date)
                        exit_events.append({
                            'symbol': member['symbol'],
                            'action': 'EXIT',
                            'date': evaluation_date,
                            'reason': 'Volume below threshold'
                        })
                
                # Step 4: Process entries (new qualifiers not currently active)
                entry_events = []
                for symbol, volume_data in current_qualifiers.items():
                    if symbol not in active_symbols:
                        self._process_member_entry(cursor, universe_id, symbol, volume_data, evaluation_date)
                        entry_events.append({
                            'symbol': symbol,
                            'action': 'ENTRY', 
                            'date': evaluation_date,
                            'reason': f'Volume exceeded threshold: ${volume_data["avg_volume"]:,.0f}'
                        })
                
                # Commit changes
                conn.commit()
                
                result = {
                    'evaluation_date': evaluation_date,
                    'universe_id': universe_id,
                    'entries': entry_events,
                    'exits': exit_events,
                    'total_qualifiers': len(current_qualifiers),
                    'total_active_after': len(active_members) - len(exit_events) + len(entry_events)
                }
                
                logger.info(f"Membership update complete: {len(entry_events)} entries, {len(exit_events)} exits")
                return result
    
    def _get_current_qualifiers(self, cursor, evaluation_date: datetime) -> Dict[str, Dict]:
        """Get stocks that currently qualify based on volume criteria"""
        
        # Calculate date range for 50-day rolling average
        start_date = evaluation_date - timedelta(days=70)  # Extra buffer for weekends/holidays
        
        cursor.execute(f"""
            WITH daily_volumes AS (
                SELECT 
                    symbol,
                    date,
                    close * volume as dollar_volume
                FROM {self.daily_prices_table}
                WHERE date BETWEEN %s AND %s
                    AND volume > 0  -- Exclude non-trading days
                ORDER BY symbol, date
            ),
            rolling_averages AS (
                SELECT 
                    symbol,
                    date,
                    dollar_volume,
                    AVG(dollar_volume) OVER (
                        PARTITION BY symbol 
                        ORDER BY date 
                        ROWS BETWEEN %s PRECEDING AND CURRENT ROW
                    ) as rolling_avg,
                    COUNT(*) OVER (
                        PARTITION BY symbol 
                        ORDER BY date 
                        ROWS BETWEEN %s PRECEDING AND CURRENT ROW
                    ) as window_size
                FROM daily_volumes
            ),
            latest_qualifiers AS (
                SELECT DISTINCT ON (symbol)
                    symbol,
                    rolling_avg,
                    window_size,
                    date as last_evaluation_date
                FROM rolling_averages
                WHERE date <= %s
                    AND window_size >= %s  -- Require minimum trading days
                    AND rolling_avg >= %s  -- Volume threshold
                ORDER BY symbol, date DESC
            )
            SELECT 
                lq.symbol,
                lq.rolling_avg as avg_volume,
                lq.window_size,
                lq.last_evaluation_date,
                i.id as instrument_id
            FROM latest_qualifiers lq
            INNER JOIN {self.instruments_table} i ON lq.symbol = i.symbol
        """, (
            start_date, evaluation_date,
            self.rolling_window_days - 1, self.rolling_window_days - 1,  # ROWS BETWEEN params
            evaluation_date,
            30,  # Minimum 30 trading days required
            self.volume_threshold
        ))
        
        qualifiers = {}
        for row in cursor.fetchall():
            qualifiers[row['symbol']] = {
                'avg_volume': row['avg_volume'],
                'window_size': row['window_size'],
                'instrument_id': row['instrument_id'],
                'last_evaluation_date': row['last_evaluation_date']
            }
        
        return qualifiers
    
    def _get_active_members(self, cursor, universe_id: int) -> List[Dict]:
        """Get currently active universe members"""
        cursor.execute(f"""
            SELECT 
                universe_id,
                symbol,
                start_at,
                instrument_id
            FROM {self.membership_table}
            WHERE universe_id = %s 
                AND end_at IS NULL
        """, (universe_id,))
        
        return cursor.fetchall()
    
    def _process_member_exit(self, cursor, member: Dict, evaluation_date: datetime):
        """Process member exit - set end_at date"""
        cursor.execute(f"""
            UPDATE {self.membership_table}
            SET end_at = %s
            WHERE universe_id = %s 
                AND symbol = %s 
                AND end_at IS NULL
        """, (evaluation_date, member['universe_id'], member['symbol']))
        
        logger.info(f"Member exit: {member['symbol']} (was active since {member['start_at']})")
    
    def _process_member_entry(self, cursor, universe_id: int, symbol: str, 
                            volume_data: Dict, evaluation_date: datetime):
        """Process member entry - create new membership record"""
        cursor.execute(f"""
            INSERT INTO {self.membership_table} 
            (universe_id, symbol, start_at, end_at, instrument_id)
            VALUES (%s, %s, %s, NULL, %s)
        """, (
            universe_id, 
            symbol, 
            evaluation_date, 
            volume_data['instrument_id']
        ))
        
        logger.info(f"Member entry: {symbol} (volume: ${volume_data['avg_volume']:,.0f})")
    
    def analyze_historical_qualification_events(self, symbol: str, 
                                               start_date: datetime, 
                                               end_date: datetime) -> List[Dict]:
        """
        Analyze historical qualification events for a specific stock
        Returns list of entry/exit events with dates
        """
        with get_raw_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                
                cursor.execute(f"""
                    WITH daily_volumes AS (
                        SELECT 
                            symbol,
                            date,
                            close * volume as dollar_volume
                        FROM {self.daily_prices_table}
                        WHERE symbol = %s 
                            AND date BETWEEN %s AND %s
                            AND volume > 0
                        ORDER BY date
                    ),
                    rolling_averages AS (
                        SELECT 
                            symbol,
                            date,
                            dollar_volume,
                            AVG(dollar_volume) OVER (
                                ORDER BY date 
                                ROWS BETWEEN %s PRECEDING AND CURRENT ROW
                            ) as rolling_avg,
                            COUNT(*) OVER (
                                ORDER BY date 
                                ROWS BETWEEN %s PRECEDING AND CURRENT ROW
                            ) as window_size
                        FROM daily_volumes
                    ),
                    qualification_changes AS (
                        SELECT 
                            date,
                            rolling_avg,
                            window_size,
                            CASE WHEN rolling_avg >= %s AND window_size >= 30 THEN 1 ELSE 0 END as qualifies,
                            LAG(CASE WHEN rolling_avg >= %s AND window_size >= 30 THEN 1 ELSE 0 END) 
                                OVER (ORDER BY date) as prev_qualifies
                        FROM rolling_averages
                        WHERE window_size >= 30  -- Require minimum data
                    )
                    SELECT 
                        date,
                        rolling_avg,
                        qualifies,
                        prev_qualifies,
                        CASE 
                            WHEN qualifies = 1 AND (prev_qualifies IS NULL OR prev_qualifies = 0) THEN 'ENTRY'
                            WHEN qualifies = 0 AND prev_qualifies = 1 THEN 'EXIT'
                            ELSE NULL
                        END as event_type
                    FROM qualification_changes
                    WHERE qualifies != COALESCE(prev_qualifies, qualifies)  -- Only changes
                    ORDER BY date
                """, (
                    symbol, start_date, end_date,
                    self.rolling_window_days - 1, self.rolling_window_days - 1,
                    self.volume_threshold, self.volume_threshold
                ))
                
                events = []
                for row in cursor.fetchall():
                    if row['event_type']:
                        events.append({
                            'symbol': symbol,
                            'date': row['date'],
                            'event_type': row['event_type'],
                            'rolling_volume': row['rolling_avg'],
                            'qualifies_after': row['qualifies'] == 1
                        })
                
                return events
    
    def generate_correct_membership_data(self, universe_id: int = 2, 
                                       start_date: datetime = None, 
                                       end_date: datetime = None) -> Dict:
        """
        Generate what the universe membership SHOULD look like
        based on historical volume analysis (for comparison with current flawed data)
        """
        if start_date is None:
            start_date = datetime(2019, 1, 1)
        if end_date is None:
            end_date = datetime.now()
        
        logger.info(f"Generating correct membership data for {start_date.date()} to {end_date.date()}")
        
        # Key stocks to analyze based on research
        key_stocks = ['SMCI', 'MSTR', 'PTON', 'BYND', 'ARKB', 'NVDA', 'AAPL', 'MSFT', 'GOOGL', 'TSLA']
        
        correct_memberships = {}
        
        for symbol in key_stocks:
            try:
                events = self.analyze_historical_qualification_events(symbol, start_date, end_date)
                
                # Convert events to membership periods
                memberships = []
                current_membership = None
                
                for event in events:
                    if event['event_type'] == 'ENTRY':
                        if current_membership:
                            # This shouldn't happen, but handle gracefully
                            current_membership['end_at'] = event['date']
                            memberships.append(current_membership)
                        
                        current_membership = {
                            'symbol': symbol,
                            'start_at': event['date'],
                            'end_at': None,
                            'entry_volume': event['rolling_volume']
                        }
                    
                    elif event['event_type'] == 'EXIT' and current_membership:
                        current_membership['end_at'] = event['date']
                        current_membership['exit_volume'] = event['rolling_volume']
                        memberships.append(current_membership)
                        current_membership = None
                
                # Handle ongoing membership
                if current_membership:
                    memberships.append(current_membership)
                
                correct_memberships[symbol] = {
                    'events': events,
                    'memberships': memberships,
                    'total_periods': len(memberships)
                }
                
            except Exception as e:
                logger.error(f"Error analyzing {symbol}: {e}")
                correct_memberships[symbol] = {'error': str(e)}
        
        return {
            'analysis_period': f"{start_date.date()} to {end_date.date()}",
            'stocks_analyzed': key_stocks,
            'correct_memberships': correct_memberships,
            'summary': self._summarize_correct_data(correct_memberships)
        }
    
    def _summarize_correct_data(self, correct_memberships: Dict) -> Dict:
        """Summarize the correct membership analysis"""
        total_events = 0
        total_periods = 0
        stocks_with_multiple_periods = 0
        
        for symbol, data in correct_memberships.items():
            if 'events' in data:
                total_events += len(data['events'])
                total_periods += data['total_periods']
                if data['total_periods'] > 1:
                    stocks_with_multiple_periods += 1
        
        return {
            'total_qualification_events': total_events,
            'total_membership_periods': total_periods,
            'stocks_with_multiple_periods': stocks_with_multiple_periods,
            'average_periods_per_stock': total_periods / len(correct_memberships) if correct_memberships else 0
        }