import pytest

def pytest_addoption(parser):
    """Add command-line option for updating golden files."""
    parser.addoption(
        "--update-golden",
        action="store_true", 
        default=False,
        help="Update golden files with current test results"
    )

@pytest.fixture
def update_golden(request):
    """Fixture to get the update_golden command-line option."""
    return request.config.getoption("--update-golden")