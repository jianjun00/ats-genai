import pandas as pd
from typing import List, Optional, Dict, Any
from enum import Enum

class CorporateActionType(Enum):
    SPLIT = "split"
    DIVIDEND = "dividend"
    MERGER = "merger"
    SPINOFF = "spinoff"
    DELISTING = "delisting"

class CorporateAction:
    def __init__(self, symbol: str, action_type: CorporateActionType, effective_date: str, ratio: Optional[float] = None, amount: Optional[float] = None, new_symbol: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None):
        self.symbol = symbol
        self.action_type = action_type
        self.effective_date = effective_date
        self.ratio = ratio
        self.amount = amount
        self.new_symbol = new_symbol
        self.metadata = metadata or {}

class SecurityMaster:
    def update_for_eod(self, current_time):
        """
        End-of-day hook for SecurityMaster. Implement EOD security updates or logging if needed.
        """
        # Add EOD logic if needed
        pass

    def __init__(self, env=None):
        self.env = env
    @staticmethod
    def apply_corporate_actions(universe_data: pd.DataFrame, corporate_actions: List[CorporateAction]) -> pd.DataFrame:
        """
        Apply a list of corporate actions to the universe state DataFrame.
        """
        result_data = universe_data.copy()
        for action in corporate_actions:
            result_data = SecurityMaster.apply_single_corporate_action(result_data, action)
        return result_data

    @staticmethod
    def apply_single_corporate_action(universe_data: pd.DataFrame, action: CorporateAction) -> pd.DataFrame:
        result_data = universe_data.copy()
        if action.action_type == CorporateActionType.SPLIT:
            mask = result_data['symbol'] == action.symbol
            if mask.any():
                if 'close_price' in result_data.columns:
                    result_data.loc[mask, 'close_price'] /= action.ratio
                if 'volume' in result_data.columns:
                    result_data.loc[mask, 'volume'] *= action.ratio
        elif action.action_type == CorporateActionType.DELISTING:
            result_data = result_data[result_data['symbol'] != action.symbol]
        elif action.action_type == CorporateActionType.MERGER:
            if action.new_symbol:
                mask = result_data['symbol'] == action.symbol
                result_data.loc[mask, 'symbol'] = action.new_symbol
        # Extend for DIVIDEND, SPINOFF, etc. as needed
        return result_data
