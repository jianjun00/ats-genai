import pytest
import asyncio
import time
import logging
from unittest.mock import AsyncMock, MagicMock, patch
from shared.utils.db_retry import retry_async, retry_sync


class TestRetryAsync:
    """Comprehensive test coverage for retry_async function."""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_success_first_attempt(self):
        """Test retry_async succeeds on first attempt."""
        mock_func = AsyncMock(return_value="success")
        
        result = await retry_async(mock_func, "arg1", "arg2", kwarg1="value1")
        
        assert result == "success"
        mock_func.assert_called_once_with("arg1", "arg2", kwarg1="value1")
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_success_after_retries(self):
        """Test retry_async succeeds after multiple failures."""
        mock_func = AsyncMock()
        # Fail twice, then succeed
        mock_func.side_effect = [Exception("error1"), Exception("error2"), "success"]
        
        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            with patch('config.db_retry.logging') as mock_logging:
                result = await retry_async(mock_func, retries=3, delay=0.5)
        
        assert result == "success"
        assert mock_func.call_count == 3
        
        # Verify exponential backoff delays
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(0.5)   # First retry delay
        mock_sleep.assert_any_call(1.0)   # Second retry delay (0.5 * 2.0)
        
        # Verify logging calls
        assert mock_logging.info.call_count == 2
        assert mock_logging.warning.call_count >= 2
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_all_attempts_fail(self):
        """Test retry_async when all attempts fail."""
        mock_func = AsyncMock()
        test_exception = ValueError("persistent error")
        mock_func.side_effect = test_exception
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            with patch('config.db_retry.logging') as mock_logging:
                with pytest.raises(ValueError, match="persistent error"):
                    await retry_async(mock_func, retries=2, delay=0.1)
        
        assert mock_func.call_count == 3  # Original + 2 retries
        
        # Verify error logging
        mock_logging.error.assert_called()
        error_calls = [str(call) for call in mock_logging.error.call_args_list]
        assert any("All 3 attempts failed" in call for call in error_calls)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_custom_exceptions(self):
        """Test retry_async with custom exception filtering."""
        mock_func = AsyncMock()
        # First call raises ConnectionError (should retry), second raises ValueError (should not retry)
        mock_func.side_effect = [ConnectionError("connection failed"), ValueError("bad value")]
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(ValueError, match="bad value"):
                await retry_async(mock_func, retries=3, exceptions=(ConnectionError,))
        
        assert mock_func.call_count == 2
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_zero_retries(self):
        """Test retry_async with zero retries."""
        mock_func = AsyncMock(side_effect=Exception("immediate failure"))
        
        with pytest.raises(Exception, match="immediate failure"):
            await retry_async(mock_func, retries=0)
        
        mock_func.assert_called_once()
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_custom_backoff_factor(self):
        """Test retry_async with custom backoff factor."""
        mock_func = AsyncMock()
        mock_func.side_effect = [Exception("error1"), Exception("error2"), "success"]
        
        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            result = await retry_async(mock_func, retries=3, delay=1.0, backoff_factor=3.0)
        
        assert result == "success"
        
        # Verify custom backoff delays
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1.0)   # First retry delay
        mock_sleep.assert_any_call(3.0)   # Second retry delay (1.0 * 3.0)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_detailed_logging(self):
        """Test retry_async logs detailed exception information."""
        mock_func = AsyncMock()
        test_exception = ConnectionError("Database connection timeout")
        mock_func.side_effect = test_exception
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            with patch('config.db_retry.logging') as mock_logging:
                with pytest.raises(ConnectionError):
                    await retry_async(mock_func, retries=1, delay=0.1)
        
        # Verify detailed exception logging
        warning_calls = [str(call) for call in mock_logging.warning.call_args_list]
        assert any("ConnectionError" in call for call in warning_calls)
        assert any("Database connection timeout" in call for call in warning_calls)
        
        error_calls = [str(call) for call in mock_logging.error.call_args_list]
        assert any("Final exception type: ConnectionError" in call for call in error_calls)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_function_name_in_logs(self):
        """Test retry_async includes function name in log messages."""
        @pytest.mark.asyncio
        async def test_database_operation():
            raise Exception("test error")
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            with patch('config.db_retry.logging') as mock_logging:
                with pytest.raises(Exception):
                    await retry_async(test_database_operation, retries=1)
        
        # Verify function name appears in logs
        all_log_calls = (
            mock_logging.info.call_args_list +
            mock_logging.warning.call_args_list +
            mock_logging.error.call_args_list
        )
        log_messages = [str(call) for call in all_log_calls]
        assert any("test_database_operation" in msg for msg in log_messages)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_preserves_return_types(self):
        """Test retry_async preserves different return types."""
        # Test different return types
        test_cases = [
            ("string", str),
            (42, int),
            ([1, 2, 3], list),
            ({"key": "value"}, dict),
            (None, type(None))
        ]
        
        for expected_value, expected_type in test_cases:
            mock_func = AsyncMock(return_value=expected_value)
            result = await retry_async(mock_func)
            assert result == expected_value
            assert type(result) == expected_type


class TestRetrySync:
    """Comprehensive test coverage for retry_sync function."""
    
    def test_retry_sync_success_first_attempt(self):
        """Test retry_sync succeeds on first attempt."""
        mock_func = MagicMock(return_value="success")
        
        result = retry_sync(mock_func, "arg1", "arg2", kwarg1="value1")
        
        assert result == "success"
        mock_func.assert_called_once_with("arg1", "arg2", kwarg1="value1")
    
    def test_retry_sync_success_after_retries(self):
        """Test retry_sync succeeds after multiple failures."""
        mock_func = MagicMock()
        mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
        # Fail twice, then succeed
        mock_func.side_effect = [Exception("error1"), Exception("error2"), "success"]
        
        with patch('time.sleep') as mock_sleep:
            with patch('config.db_retry.logging') as mock_logging:
                result = retry_sync(mock_func, retries=3, delay=0.5)
        
        assert result == "success"
        assert mock_func.call_count == 3
        
        # Verify exponential backoff delays
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(0.5)   # First retry delay
        mock_sleep.assert_any_call(1.0)   # Second retry delay (0.5 * 2.0)
        
        # Verify logging calls
        assert mock_logging.info.call_count == 2
        assert mock_logging.warning.call_count >= 2
    
    def test_retry_sync_all_attempts_fail(self):
        """Test retry_sync when all attempts fail."""
        mock_func = MagicMock()
        mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
        test_exception = ValueError("persistent error")
        mock_func.side_effect = test_exception
        
        with patch('time.sleep'):
            with patch('config.db_retry.logging') as mock_logging:
                with pytest.raises(ValueError, match="persistent error"):
                    retry_sync(mock_func, retries=2, delay=0.1)
        
        assert mock_func.call_count == 3  # Original + 2 retries
        
        # Verify error logging
        mock_logging.error.assert_called()
        error_calls = [str(call) for call in mock_logging.error.call_args_list]
        assert any("All 3 attempts failed" in call for call in error_calls)
    
    def test_retry_sync_custom_exceptions(self):
        """Test retry_sync with custom exception filtering."""
        mock_func = MagicMock()
        mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
        # First call raises ConnectionError (should retry), second raises ValueError (should not retry)
        mock_func.side_effect = [ConnectionError("connection failed"), ValueError("bad value")]
        
        with patch('time.sleep'):
            with pytest.raises(ValueError, match="bad value"):
                retry_sync(mock_func, retries=3, exceptions=(ConnectionError,))
        
        assert mock_func.call_count == 2
    
    def test_retry_sync_zero_retries(self):
        """Test retry_sync with zero retries."""
        mock_func = MagicMock(side_effect=Exception("immediate failure"))
        mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
        
        with pytest.raises(Exception, match="immediate failure"):
            retry_sync(mock_func, retries=0)
        
        mock_func.assert_called_once()
    
    def test_retry_sync_custom_backoff_factor(self):
        """Test retry_sync with custom backoff factor."""
        mock_func = MagicMock()
        mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
        mock_func.side_effect = [Exception("error1"), Exception("error2"), "success"]
        
        with patch('time.sleep') as mock_sleep:
            result = retry_sync(mock_func, retries=3, delay=1.0, backoff_factor=3.0)
        
        assert result == "success"
        
        # Verify custom backoff delays
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1.0)   # First retry delay
        mock_sleep.assert_any_call(3.0)   # Second retry delay (1.0 * 3.0)
    
    def test_retry_sync_detailed_logging(self):
        """Test retry_sync logs detailed exception information."""
        mock_func = MagicMock()
        mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
        test_exception = ConnectionError("Database connection timeout")
        mock_func.side_effect = test_exception
        
        with patch('time.sleep'):
            with patch('config.db_retry.logging') as mock_logging:
                with pytest.raises(ConnectionError):
                    retry_sync(mock_func, retries=1, delay=0.1)
        
        # Verify detailed exception logging
        warning_calls = [str(call) for call in mock_logging.warning.call_args_list]
        assert any("ConnectionError" in call for call in warning_calls)
        assert any("Database connection timeout" in call for call in warning_calls)
        
        error_calls = [str(call) for call in mock_logging.error.call_args_list]
        assert any("Final exception type: ConnectionError" in call for call in error_calls)
    
    def test_retry_sync_function_name_in_logs(self):
        """Test retry_sync includes function name in log messages."""
        def test_database_operation():
            raise Exception("test error")
        
        with patch('time.sleep'):
            with patch('config.db_retry.logging') as mock_logging:
                with pytest.raises(Exception):
                    retry_sync(test_database_operation, retries=1)
        
        # Verify function name appears in logs
        all_log_calls = (
            mock_logging.info.call_args_list +
            mock_logging.warning.call_args_list +
            mock_logging.error.call_args_list
        )
        log_messages = [str(call) for call in all_log_calls]
        assert any("test_database_operation" in msg for msg in log_messages)
    
    def test_retry_sync_preserves_return_types(self):
        """Test retry_sync preserves different return types."""
        # Test different return types
        test_cases = [
            ("string", str),
            (42, int),
            ([1, 2, 3], list),
            ({"key": "value"}, dict),
            (None, type(None))
        ]
        
        for expected_value, expected_type in test_cases:
            mock_func = MagicMock(return_value=expected_value)
            mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
            result = retry_sync(mock_func)
            assert result == expected_value
            assert type(result) == expected_type


class TestRetryUtilitiesEdgeCases:
    """Test edge cases and integration scenarios for retry utilities."""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_with_coroutine_exceptions(self):
        """Test retry_async handles coroutine-specific exceptions."""
        async def failing_coroutine():
            raise asyncio.TimeoutError("Operation timed out")
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(asyncio.TimeoutError):
                await retry_async(failing_coroutine, retries=1, exceptions=(asyncio.TimeoutError,))
    
    def test_retry_sync_with_multiple_exception_types(self):
        """Test retry_sync with multiple exception types."""
        mock_func = MagicMock()
        mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
        mock_func.side_effect = [
            ConnectionError("connection error"),
            TimeoutError("timeout error"),
            "success"
        ]
        
        with patch('time.sleep'):
            result = retry_sync(
                mock_func, 
                retries=3, 
                exceptions=(ConnectionError, TimeoutError)
            )
        
        assert result == "success"
        assert mock_func.call_count == 3
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_large_delay_values(self):
        """Test retry_async with large delay values."""
        mock_func = AsyncMock(side_effect=[Exception("error"), "success"])
        
        with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            result = await retry_async(mock_func, retries=1, delay=100.0)
        
        assert result == "success"
        mock_sleep.assert_called_once_with(100.0)
    
    def test_retry_sync_with_complex_return_values(self):
        """Test retry_sync with complex return values."""
        class CustomObject:
            def __init__(self, value):
                self.value = value
            def __eq__(self, other):
                return isinstance(other, CustomObject) and self.value == other.value
        
        expected_obj = CustomObject("test_value")
        mock_func = MagicMock(return_value=expected_obj)
        mock_func.__name__ = "test_function"  # Add __name__ attribute for logging
        
        result = retry_sync(mock_func)
        assert result == expected_obj
        assert result.value == "test_value"
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_exception_chaining(self):
        """Test retry_async preserves exception information."""
        original_exception = ValueError("root cause")
        
        async def chained_exception_func():
            try:
                raise original_exception
            except ValueError as e:
                raise ConnectionError("wrapper error") from e
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            with pytest.raises(ConnectionError) as exc_info:
                await retry_async(chained_exception_func, retries=0)
        
        # Verify exception chaining is preserved
        assert exc_info.value.__cause__ == original_exception
    
    def test_retry_sync_memory_efficiency(self):
        """Test retry_sync doesn't accumulate memory with many retries."""
        call_count = 0
        
        def memory_test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 50:
                raise Exception(f"error_{call_count}")
            return f"success_after_{call_count}_calls"
        
        with patch('time.sleep'):
            result = retry_sync(memory_test_func, retries=100, delay=0.001)
        
        assert result == "success_after_50_calls"
        assert call_count == 50
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_retry_async_concurrent_calls(self):
        """Test retry_async works correctly with concurrent calls."""
        call_counts = {"func1": 0, "func2": 0}
        
        async def concurrent_func(name):
            call_counts[name] += 1
            if call_counts[name] < 3:
                raise Exception(f"{name}_error_{call_counts[name]}")
            return f"{name}_success"
        
        with patch('asyncio.sleep', new_callable=AsyncMock):
            # Run two retry operations concurrently
            results = await asyncio.gather(
                retry_async(concurrent_func, "func1", retries=5),
                retry_async(concurrent_func, "func2", retries=5)
            )
        
        assert results == ["func1_success", "func2_success"]
        assert call_counts["func1"] == 3
        assert call_counts["func2"] == 3