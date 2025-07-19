import requests
from datetime import datetime
from events.schemas import EventIn

def fetch_yahoo_earnings(symbol: str):
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules=calendarEvents"
    r = requests.get(url)
    data = r.json()
    earnings = data['quoteSummary']['result'][0]['calendarEvents']['earnings']
    if 'earningsDate' in earnings and earnings['earningsDate']:
        for ed in earnings['earningsDate']:
            event_time = datetime.fromtimestamp(ed['raw'])
            yield EventIn(
                event_type='earnings',
                symbol=symbol,
                event_time=event_time,
                reported_time=None,
                source='yahoo',
                data=ed
            )
# Example usage:
# for event in fetch_yahoo_earnings('AAPL'):
#     print(event)
