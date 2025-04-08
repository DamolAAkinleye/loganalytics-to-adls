# retry_utils.py

```python
import asyncio
import functools
import logging
from typing import Any, Callable, Optional, Type, TypeVar, Union, List

from retry import retry as retry_decorator
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError
)

# Type variables for better typing
T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])

# Logger for this module
logger = logging.getLogger(__name__)


def async_retry(
    exceptions: Union[Type[Exception], List[Type[Exception]]],
    max_attempts: int = 3,
    delay: int = 1,
    backoff: int = 2,
    logger_name: Optional[str] = None
) -> Callable[[F], F]:
    """
    A decorator for async functions to implement retry logic.
    
    Args:
        exceptions: Exception(s) that trigger retry
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier e.g. 2 will double the delay each retry
        logger_name: Logger name for retry logs
    
    Returns:
        Decorated function with retry logic
    """
    local_logger = logging.getLogger(logger_name or __name__)
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            retry_count = 0
            current_delay = delay
            
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    retry_count += 1
                    if retry_count >= max_attempts:
                        local_logger.error(
                            f"Failed after {max_attempts} attempts: {func.__name__}, error: {str(e)}"
                        )
                        raise
                    
                    local_logger.warning(
                        f"Retry {retry_count}/{max_attempts} for {func.__name__} after error: {str(e)}"
                        f", waiting {current_delay}s"
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        
        return wrapper  # type: ignore
    
    return decorator


def sync_retry(
    exceptions: Union[Type[Exception], List[Type[Exception]]],
    tries: int = 3,
    delay: int = 1,
    backoff: int = 2,
    logger_name: Optional[str] = None
) -> Callable[[F], F]:
    """
    A decorator for sync functions to implement retry logic using the retry package.
    
    Args:
        exceptions: Exception(s) that trigger retry
        tries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier e.g. 2 will double the delay each retry
        logger_name: Logger name for retry logs
    
    Returns:
        Decorated function with retry logic
    """
    local_logger = logging.getLogger(logger_name or __name__)
    
    def log_retry(func_name: str, tries_remaining: int, exception: Exception) -> None:
        local_logger.warning(
            f"Retrying {func_name}, {tries_remaining} tries remaining after error: {str(exception)}"
        )
    
    return retry_decorator(
        exceptions,
        tries=tries,
        delay=delay,
        backoff=backoff,
        logger=local_logger,
        on_retry=log_retry
    )


def tenacity_retry(
    exceptions: Union[Type[Exception], List[Type[Exception]]],
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 10.0,
    logger_name: Optional[str] = None
) -> Callable[[F], F]:
    """
    A decorator using tenacity for more advanced retry capabilities.
    
    Args:
        exceptions: Exception(s) that trigger retry
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time between retries
        max_wait: Maximum wait time between retries
        logger_name: Logger name for retry logs
    
    Returns:
        Decorated function with retry logic
    """
    local_logger = logging.getLogger(logger_name or __name__)
    
    def before_retry(retry_state):
        local_logger.warning(
            f"Retrying {retry_state.fn.__name__}, attempt {retry_state.attempt_number}/{max_attempts} "
            f"after error: {retry_state.outcome.exception()}"
        )
    
    return retry(
        retry=retry_if_exception_type(exceptions),
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        before=before_retry,
        reraise=True
    ) 
```
