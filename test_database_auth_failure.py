#!/usr/bin/env python3
"""
Test that reproduces the database authentication failure
This test SHOULD FAIL initially, then pass after we implement mock mode
"""

import pytest
import asyncio
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_database_auth_failure_reproduction():
    """Test that reproduces the exact database auth failure we're seeing"""
    
    # This should fail with the current setup
    try:
        import asyncpg
        
        async def test_failed_connection():
            # These are the exact configs that are failing
            configs = [
                {"host": "localhost", "port": 5432, "user": "postgres", "password": "dev_password", "database": "dev_db"},
                {"host": "localhost", "port": 5432, "user": "postgres", "password": "postgres", "database": "dev_db"},
                {"host": "localhost", "port": 5433, "user": "postgres", "password": "dev_password", "database": "dev_db"},
            ]
            
            for config in configs:
                try:
                    conn = await asyncpg.connect(**config)
                    await conn.close()
                    return True
                except Exception as e:
                    print(f"Expected auth failure: {e}")
                    continue
            
            # If we get here, all connections failed as expected
            return False
        
        connected = asyncio.run(test_failed_connection())
        
        # This should fail currently 
        assert not connected, "Database connections should fail with current config"
        print("✅ Test correctly reproduces database auth failure")
        
    except ImportError:
        pytest.skip("asyncpg not available")

def test_analytics_engine_fails_without_database():
    """Test that analytics engine fails when database unavailable"""
    
    try:
        from analytics.portfolio_analytics import PortfolioAnalyticsEngine
        
        # This should fail with current implementation
        async def test_engine_failure():
            # Bad database URL should cause failure
            bad_url = "postgresql://postgres:wrongpassword@localhost:5432/nonexistent"
            engine = PortfolioAnalyticsEngine(db_url=bad_url)
            
            try:
                await engine.initialize()
                return True  # Should not reach here
            except Exception as e:
                print(f"Expected engine failure: {e}")
                return False
        
        success = asyncio.run(test_engine_failure())
        
        # This should fail currently
        assert not success, "Analytics engine should fail with bad database config"
        print("✅ Test correctly reproduces analytics engine failure")
        
    except ImportError as e:
        pytest.fail(f"Cannot import analytics engine: {e}")

def test_mock_mode_not_implemented_yet():
    """Test that mock mode doesn't exist yet (this should fail initially)"""
    
    try:
        from analytics.portfolio_analytics import PortfolioAnalyticsEngine
        
        # Try to use mock mode - this should fail initially
        async def test_mock_mode():
            engine = PortfolioAnalyticsEngine(mock_mode=True)
            await engine.initialize()
            return True
        
        # This should fail because mock mode doesn't exist yet
        try:
            asyncio.run(test_mock_mode())
            pytest.fail("Mock mode should not be implemented yet")
        except TypeError as e:
            # Expected - mock_mode parameter doesn't exist
            print(f"✅ Mock mode correctly not implemented yet: {e}")
            assert "unexpected keyword argument" in str(e)
        
    except ImportError as e:
        pytest.fail(f"Cannot import analytics engine: {e}")

if __name__ == "__main__":
    # Run the reproduction tests
    test_database_auth_failure_reproduction()
    test_analytics_engine_fails_without_database()
    test_mock_mode_not_implemented_yet()
    print("🧪 All reproduction tests passed - issues correctly identified")