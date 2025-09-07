"""Database connection retry utilities."""
import asyncio
import logging
import time
from typing import Any, Callable, TypeVar

T = TypeVar('T')

async def retry_async(
    func: Callable[..., Any],
    *args: Any,
    retries: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    **kwargs: Any
) -> Any:
    """
    Retry an async function with exponential backoff.
    
    Args:
        func: The async function to retry
        *args: Positional arguments to pass to the function
        retries: Number of times to retry before giving up
        delay: Initial delay between retries in seconds
        backoff_factor: Backoff multiplier (e.g. value of 2 will double the delay each retry)
        exceptions: Tuple of exceptions to catch and retry on
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The return value of the function
        
    Raises:
        The last exception raised by the function
    """
    last_exception = None
    current_delay = delay
    
    for attempt in range(retries + 1):
        try:
            if attempt > 0:
                logging.info(f"Retry attempt {attempt}/{retries} for {func.__name__}")
            return await func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            if attempt < retries:
                # Print detailed exception information
                logging.warning(f"Connection attempt {attempt + 1} failed with exception type: {type(e).__name__}")
                logging.warning(f"Exception message: {str(e)}")
                logging.warning(f"Exception details: {repr(e)}")
                logging.warning(f"Retrying in {current_delay:.1f}s...")
                await asyncio.sleep(current_delay)
                current_delay *= backoff_factor
            else:
                logging.error(f"All {retries + 1} attempts failed for {func.__name__}")
                logging.error(f"Final exception type: {type(last_exception).__name__}")
                logging.error(f"Final exception message: {str(last_exception)}")
                logging.error(f"Final exception details: {repr(last_exception)}")
                raise last_exception

def retry_sync(
    func: Callable[..., T],
    *args: Any,
    retries: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (Exception,),
    **kwargs: Any
) -> T:
    """
    Retry a synchronous function with exponential backoff.
    
    Args:
        func: The function to retry
        *args: Positional arguments to pass to the function
        retries: Number of times to retry before giving up
        delay: Initial delay between retries in seconds
        backoff_factor: Backoff multiplier (e.g. value of 2 will double the delay each retry)
        exceptions: Tuple of exceptions to catch and retry on
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The return value of the function
        
    Raises:
        The last exception raised by the function
    """
    last_exception = None
    current_delay = delay
    
    for attempt in range(retries + 1):
        try:
            if attempt > 0:
                logging.info(f"Retry attempt {attempt}/{retries} for {func.__name__}")
            return func(*args, **kwargs)
        except exceptions as e:
            last_exception = e
            if attempt < retries:
                # Print detailed exception information
                logging.warning(f"Connection attempt {attempt + 1} failed with exception type: {type(e).__name__}")
                logging.warning(f"Exception message: {str(e)}")
                logging.warning(f"Exception details: {repr(e)}")
                logging.warning(f"Retrying in {current_delay:.1f}s...")
                time.sleep(current_delay)
                current_delay *= backoff_factor
            else:
                logging.error(f"All {retries + 1} attempts failed for {func.__name__}")
                logging.error(f"Final exception type: {type(last_exception).__name__}")
                logging.error(f"Final exception message: {str(last_exception)}")
                logging.error(f"Final exception details: {repr(last_exception)}")
                raise last_exception
