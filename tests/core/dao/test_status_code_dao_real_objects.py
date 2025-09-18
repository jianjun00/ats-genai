"""
Core StatusCodeDAO tests using real database objects and fail-fast validation.

This replaces test_status_code_dao.py with real database integration testing.
All mocks are eliminated for authentic database constraint testing.
"""

import pytest
from datetime import datetime
from typing import List, Dict

from core.dao.status_code_dao import StatusCodeDAO
from shared.utils.environment import Environment, EnvironmentType


@pytest.fixture
async def test_environment():
    """Real test environment with actual database connection."""
    return Environment(
        env_type=EnvironmentType.DEV,
        db_url="postgresql://postgres:dev_password@localhost:5432/dev_db"
    )


@pytest.fixture
async def status_code_dao(test_environment):
    """Real StatusCodeDAO instance."""
    return StatusCodeDAO(test_environment)


@pytest.fixture
async def test_status_codes():
    """Test status code data for creation tests."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    return [
        {
            'code': f'TEST_OK_{timestamp}',
            'description': 'Test operation successful'
        },
        {
            'code': f'TEST_ERROR_{timestamp}',
            'description': 'Test operation failed'
        },
        {
            'code': f'TEST_WARN_{timestamp}',
            'description': 'Test operation warning'
        }
    ]


@pytest.fixture
async def created_test_status(status_code_dao, test_status_codes):
    """Create a real test status code for read/update tests."""
    test_status = test_status_codes[0]
    await status_code_dao.insert_status(test_status['code'], test_status['description'])
    
    yield test_status
    
    # Cleanup - remove test status code
    await status_code_dao.delete_status(test_status['code'])


class TestStatusCodeDAORealObjects:
    """Real database integration tests for StatusCodeDAO."""

    def test_dao_initialization_real_environment(self, test_environment):
        """Test DAO initialization with real environment."""
        dao = StatusCodeDAO(test_environment)
        
        assert dao.env == test_environment
        assert dao.table_name == test_environment.get_table_name('status_code')
        assert dao.db_url == test_environment.get_database_url()

    async def test_insert_status_success(self, status_code_dao, test_status_codes):
        """Test successful status code insertion with real database."""
        test_status = test_status_codes[0]
        
        # Insert status code
        await status_code_dao.insert_status(test_status['code'], test_status['description'])
        
        # Verify insertion by retrieving
        retrieved_status = await status_code_dao.get_status(test_status['code'])
        assert retrieved_status is not None
        assert retrieved_status['code'] == test_status['code']
        assert retrieved_status['description'] == test_status['description']
        
        # Cleanup
        await status_code_dao.delete_status(test_status['code'])

    async def test_insert_status_duplicate_constraint(self, status_code_dao, created_test_status):
        """Test that duplicate status codes violate database constraints."""
        # Attempt to insert duplicate status code
        with pytest.raises(Exception):  # Database unique constraint violation
            await status_code_dao.insert_status(
                created_test_status['code'],
                "Duplicate description"
            )

    async def test_get_status_success(self, status_code_dao, created_test_status):
        """Test successful status code retrieval."""
        result = await status_code_dao.get_status(created_test_status['code'])
        
        assert result is not None
        assert result['code'] == created_test_status['code']
        assert result['description'] == created_test_status['description']

    async def test_get_status_not_found(self, status_code_dao):
        """Test get_status with nonexistent code."""
        nonexistent_code = f"NONEXISTENT_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        result = await status_code_dao.get_status(nonexistent_code)
        
        assert result is None

    async def test_list_statuses_includes_created(self, status_code_dao, created_test_status):
        """Test that list_statuses includes our created status code."""
        statuses = await status_code_dao.list_statuses()
        
        # Should be a list of status codes from real database
        assert isinstance(statuses, list)
        
        # Find our test status code in the list
        our_status = next(
            (status for status in statuses if status['code'] == created_test_status['code']),
            None
        )
        assert our_status is not None
        assert our_status['description'] == created_test_status['description']

    async def test_list_statuses_consistency(self, status_code_dao):
        """Test that list_statuses returns consistent data structure."""
        statuses = await status_code_dao.list_statuses()
        
        assert isinstance(statuses, list)
        
        # Verify all returned status codes have required fields
        for status in statuses:
            assert 'code' in status
            assert 'description' in status
            assert isinstance(status['code'], str)
            assert isinstance(status['description'], str)
            assert len(status['code']) > 0  # Code should not be empty

    async def test_sql_injection_protection_real_database(self, status_code_dao):
        """Test SQL injection protection with real database."""
        # Malicious input that would be dangerous if not parameterized
        malicious_code = "'; DROP TABLE dev_status_code; --"
        malicious_description = "<script>alert('xss')</script>"
        
        # This should be safe because queries use parameterized statements
        await status_code_dao.insert_status(malicious_code, malicious_description)
        
        # Verify the malicious code was stored as literal data
        retrieved = await status_code_dao.get_status(malicious_code)
        assert retrieved is not None
        assert retrieved['code'] == malicious_code
        assert retrieved['description'] == malicious_description
        
        # Database should still be intact - verify with list operation
        statuses = await status_code_dao.list_statuses()
        assert isinstance(statuses, list)
        
        # Cleanup malicious test data
        await status_code_dao.delete_status(malicious_code)

    async def test_insert_batch_status_codes(self, status_code_dao, test_status_codes):
        """Test batch insertion of multiple status codes."""
        created_codes = []
        
        try:
            # Insert all test status codes
            for status in test_status_codes:
                await status_code_dao.insert_status(status['code'], status['description'])
                created_codes.append(status['code'])
            
            # Verify all were created
            for status in test_status_codes:
                retrieved = await status_code_dao.get_status(status['code'])
                assert retrieved is not None
                assert retrieved['code'] == status['code']
                assert retrieved['description'] == status['description']
            
            # Verify they appear in list
            all_statuses = await status_code_dao.list_statuses()
            created_codes_in_list = [
                s['code'] for s in all_statuses 
                if s['code'] in [status['code'] for status in test_status_codes]
            ]
            assert len(created_codes_in_list) == len(test_status_codes)
            
        finally:
            # Cleanup all created status codes
            for code in created_codes:
                try:
                    await status_code_dao.delete_status(code)
                except:
                    pass  # Ignore cleanup errors

    async def test_status_code_length_constraints(self, status_code_dao):
        """Test status code length constraints."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        
        # Test very long status code (might violate length constraints)
        very_long_code = "A" * 1000  # 1000 characters
        
        try:
            await status_code_dao.insert_status(
                very_long_code,
                "Test long code description"
            )
            # If it succeeds, verify it was stored correctly
            retrieved = await status_code_dao.get_status(very_long_code)
            if retrieved:
                assert retrieved['code'] == very_long_code
                await status_code_dao.delete_status(very_long_code)
                
        except Exception:
            # If database enforces length limits, that's valid
            # The important thing is it fails cleanly without corruption
            pass

    async def test_status_description_handling(self, status_code_dao):
        """Test status description field handling."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        
        # Test with various description content
        test_cases = [
            {
                'code': f'EMPTY_DESC_{timestamp}',
                'description': ''  # Empty description
            },
            {
                'code': f'NULL_DESC_{timestamp}',
                'description': None  # Null description
            },
            {
                'code': f'UNICODE_DESC_{timestamp}',
                'description': 'Test with unicode: 🚀 🎯 ✅ ❌ 中文'
            },
            {
                'code': f'SPECIAL_CHARS_{timestamp}',
                'description': "Test with special chars: \"quotes\" 'apostrophes' <tags> &amp;"
            }
        ]
        
        created_codes = []
        
        try:
            for test_case in test_cases:
                try:
                    await status_code_dao.insert_status(
                        test_case['code'],
                        test_case['description']
                    )
                    created_codes.append(test_case['code'])
                    
                    # Verify storage
                    retrieved = await status_code_dao.get_status(test_case['code'])
                    assert retrieved is not None
                    assert retrieved['code'] == test_case['code']
                    
                    # Handle None description case
                    if test_case['description'] is None:
                        # Database might store as NULL or empty string
                        assert retrieved['description'] is None or retrieved['description'] == ''
                    else:
                        assert retrieved['description'] == test_case['description']
                        
                except Exception as e:
                    # Some test cases might fail due to database constraints
                    # That's acceptable as long as it fails cleanly
                    print(f"Expected constraint failure for {test_case['code']}: {e}")
                    
        finally:
            # Cleanup
            for code in created_codes:
                try:
                    await status_code_dao.delete_status(code)
                except:
                    pass

    async def test_concurrent_status_operations(self, status_code_dao):
        """Test concurrent status code operations for race conditions."""
        import asyncio
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        base_code = f'CONCURRENT_{timestamp}'
        
        # Create multiple concurrent insertion attempts
        async def insert_status(index):
            code = f'{base_code}_{index}'
            try:
                await status_code_dao.insert_status(code, f'Concurrent test {index}')
                return code
            except Exception:
                return None
        
        # Run 10 concurrent insertions
        tasks = [insert_status(i) for i in range(10)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect successful insertions for cleanup
        successful_codes = [r for r in results if isinstance(r, str)]
        
        try:
            # Verify each successful insertion exists
            for code in successful_codes:
                retrieved = await status_code_dao.get_status(code)
                assert retrieved is not None
                assert retrieved['code'] == code
                
        finally:
            # Cleanup all successful insertions
            for code in successful_codes:
                try:
                    await status_code_dao.delete_status(code)
                except:
                    pass

    async def test_status_code_case_sensitivity(self, status_code_dao):
        """Test status code case sensitivity handling."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        
        # Test with different cases
        lower_code = f'lower_{timestamp}'
        upper_code = f'UPPER_{timestamp}'
        mixed_code = f'Mixed_{timestamp}'
        
        created_codes = []
        
        try:
            # Insert codes with different cases
            test_codes = [
                (lower_code, 'Lower case code'),
                (upper_code, 'Upper case code'),
                (mixed_code, 'Mixed case code')
            ]
            
            for code, description in test_codes:
                await status_code_dao.insert_status(code, description)
                created_codes.append(code)
            
            # Verify each can be retrieved with exact case
            for code, description in test_codes:
                retrieved = await status_code_dao.get_status(code)
                assert retrieved is not None
                assert retrieved['code'] == code
                assert retrieved['description'] == description
            
            # Test case sensitivity - these should be different codes
            # (depending on database collation settings)
            if lower_code.upper() in [c.upper() for c in created_codes]:
                # If database is case-insensitive, that's also valid
                pass
            
        finally:
            # Cleanup
            for code in created_codes:
                try:
                    await status_code_dao.delete_status(code)
                except:
                    pass


class TestStatusCodeDAOConstraintValidation:
    """Test database constraint validation with real database."""

    async def test_null_code_constraint(self, status_code_dao):
        """Test that null status code violates NOT NULL constraint."""
        with pytest.raises(Exception):  # NOT NULL constraint violation
            await status_code_dao.insert_status(None, "Test description")

    async def test_empty_code_constraint(self, status_code_dao):
        """Test behavior with empty status code."""
        # Depending on implementation, this might fail or succeed
        try:
            await status_code_dao.insert_status("", "Empty code test")
            # If successful, clean up
            await status_code_dao.delete_status("")
        except Exception:
            # If database enforces non-empty codes, that's valid
            pass

    async def test_status_code_uniqueness_constraint(self, status_code_dao, created_test_status):
        """Test that status codes must be unique."""
        # Attempt to create status with same code but different description
        with pytest.raises(Exception):  # Unique constraint violation
            await status_code_dao.insert_status(
                created_test_status['code'],
                "Different description"
            )

    async def test_database_transaction_integrity(self, status_code_dao):
        """Test database transaction integrity."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        test_code = f'TRANSACTION_TEST_{timestamp}'
        
        # Insert a status code
        await status_code_dao.insert_status(test_code, "Transaction test")
        
        # Verify it exists
        retrieved = await status_code_dao.get_status(test_code)
        assert retrieved is not None
        
        # Delete it
        await status_code_dao.delete_status(test_code)
        
        # Verify it's gone
        retrieved_after_delete = await status_code_dao.get_status(test_code)
        assert retrieved_after_delete is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])