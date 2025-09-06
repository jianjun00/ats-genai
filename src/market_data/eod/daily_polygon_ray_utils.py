import ray
from vendor.polygon.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO

@ray.remote
def ray_ingest_polygon_instrument(gin_config_path, ticker, instrument_id, shares_outstanding, start_date, end_date, api_key, logging, log_tickers, log_dir):
    import asyncio
    return asyncio.run(ray_ingest_polygon_instrument_async(gin_config_path, ticker, instrument_id, shares_outstanding, start_date, end_date, api_key, logging, log_tickers, log_dir))

async def ray_ingest_polygon_instrument_async(gin_config_path, ticker, instrument_id, shares_outstanding, start_date, end_date, api_key, logging, log_tickers, log_dir):
    from config.environment import Environment
    from market_data.eod.daily_price_polygon import download_prices_polygon, insert_prices
    env = Environment(gin_config_path=gin_config_path)
    prices_dao = DailyPricesPolygonDAO(env)
    try:
        # Download and insert prices for this ticker
        prices = download_prices_polygon(
            ticker,
            start_date,
            end_date,
            api_key,
            logging=logging,
            log_tickers=log_tickers,
            log_dir=log_dir
        )
        await insert_prices(prices, instrument_id, shares_outstanding, prices_dao, env=env)
        return ticker, len(prices)
    except Exception as e:
        return ticker, f"error: {e}"
