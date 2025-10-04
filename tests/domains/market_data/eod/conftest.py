import os
import json
import pytest
from core.platform.config_env.environment import Environment
from core.dao.infrastructure.vendors_dao import VendorsDAO

@pytest.fixture(scope="function")
async def polygon_vendor_id(unit_test_db):
    """
    Ensure the Polygon vendor exists in the vendors table and return its vendor_id.
    Uses the test DB for unit tests or constructs Environment as needed.
    """
    # Determine the db_url
    db_url = None
    if unit_test_db is not None:
        db_url = unit_test_db
    else:
        # Try to get from environment or fallback
        db_url = os.environ.get("TEST_DB_URL")
        if db_url is None:
            # Fallback: try integration DB or raise
            db_url = os.environ.get("INTG_DB_URL")
    assert db_url, "Could not determine test database URL for Polygon vendor fixture."
    env = Environment(db_url=db_url)
    dao = VendorsDAO(env)
    row = await dao.get_vendor_by_name("polygon")
    if row:
        return row["id"]
    else:
        return await dao.create_vendor("polygon", description="Polygon.io", api_key_env_var="POLYGON_API_KEY")


@pytest.fixture(scope="session")
def log_fixture():
    """Generic loader for API log data from any test/data/{log_dir} directory."""
    # Ensure project_root points to the market-forecast-app directory
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    def load(log_dir, ticker, date):
        full_log_dir = os.path.join(project_root, 'tests', 'data', log_dir)
        # Support both Polygon and Tiingo logs
        if 'polygon' in log_dir:
            prefix = 'polygon'
        elif 'tiingo' in log_dir:
            prefix = 'tiingo'
        else:
            raise ValueError(f'Unknown log_dir: {log_dir}')
        req_path = os.path.join(full_log_dir, f"{prefix}_{ticker.lower()}_request.json")
        resp_path = os.path.join(full_log_dir, f"{prefix}_{ticker.lower()}_response.json")
        with open(req_path) as f:
            req = json.load(f)
        with open(resp_path) as f:
            resp = json.load(f)
        return req, resp
    return load
