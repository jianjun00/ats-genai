import pandas as pd
from datetime import datetime, date
from typing import List, Optional

class SPYUniverse:
    def __init__(self, csv_path: str = 'spy_membership.csv'):
        self.df = pd.read_csv(csv_path, parse_dates=['effective_date', 'removal_date'])

    def get_universe(self, as_of: Optional[date] = None) -> List[str]:
        if as_of is None:
            as_of = datetime.utcnow().date()
        # Ensure as_of is a date
        as_of = pd.Timestamp(as_of)
        df = self.df
        mask = (df['effective_date'] <= as_of) & \
               ((df['removal_date'].isna()) | (df['removal_date'] > as_of))
        return df.loc[mask, 'symbol'].tolist()
