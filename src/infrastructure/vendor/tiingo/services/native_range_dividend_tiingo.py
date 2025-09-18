import argparse
import requests
from datetime import datetime, timedelta
from src.core.shared.utils.environment import Environment, EnvironmentType
from src.infrastructure.database.repositories.dividend_tiingo_dao import DividendTiingoDAO


def parse_date(val):
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        return datetime.strptime(val, "%Y-%m-%d").date()
    return val


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def fetch_tiingo_distributions(api_key, ex_date):
    url = f"https://api.tiingo.com/tiingo/corporate-actions/distributions?exDate={ex_date}&token={api_key}"
    headers = {"Authorization": f"Token {api_key}"}
    print(f"[DEBUG] Requesting distributions for exDate={ex_date}: {url}")
    resp = requests.get(url, headers=headers)
    print(f"[DEBUG] Response status: {resp.status_code}")
    if resp.status_code == 404:
        print(f"No distributions found for exDate {ex_date} (404 Not Found)")
        return []
    if resp.status_code != 200:
        print(f"Failed to fetch distributions for exDate {ex_date}: {resp.status_code} {resp.text}")
        return []
    return resp.json()


def map_tiingo_distribution(dist):
    # Example fields, adjust as needed for your schema/DAO
    def pd(val):
        if not val:
            return None
        return parse_date(val)
    return {
        'symbol': dist.get('ticker'),
        'ex_dividend_date': pd(dist.get('exDate')),
        'cash_amount': dist.get('amount'),
        'declaration_date': pd(dist.get('declaredDate')),
        'payment_date': pd(dist.get('payDate')),
        'record_date': pd(dist.get('recordDate')),
        'description': dist.get('description'),
        'refid': dist.get('id'),
        'qualified': dist.get('qualified'),
        'flag': dist.get('flag'),
        'currency': dist.get('currency'),
        'frequency': dist.get('frequency'),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    env = Environment(env_type=EnvironmentType(args.environment))
    api_key = env.get_api_key('tiingo')
    if not api_key:
        raise Exception("Please set your TIINGO_API_KEY in your environment or config.")
    dao = DividendTiingoDAO(env)
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    total_inserted = 0
    for d in daterange(start_date, end_date):
        ex_date_str = d.strftime("%Y-%m-%d")
        distributions = fetch_tiingo_distributions(api_key, ex_date_str)
        inserted = 0
        for dist in distributions:
            mapped = map_tiingo_distribution(dist)
            if mapped['symbol'] and mapped['ex_dividend_date'] and mapped['cash_amount'] is not None:
                dao.insert_dividend_sync(mapped)
                inserted += 1
        print(f"Inserted {inserted} distributions for exDate {ex_date_str}.")
        total_inserted += inserted
    print(f"Total inserted distributions: {total_inserted}")


if __name__ == "__main__":
    main()
