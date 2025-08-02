import os
import json
import pytest

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
        req_path = os.path.join(full_log_dir, f"{prefix}_{ticker.lower()}_{date}_request.json")
        resp_path = os.path.join(full_log_dir, f"{prefix}_{ticker.lower()}_{date}_response.json")
        with open(req_path) as f:
            req = json.load(f)
        with open(resp_path) as f:
            resp = json.load(f)
        return req, resp
    return load
