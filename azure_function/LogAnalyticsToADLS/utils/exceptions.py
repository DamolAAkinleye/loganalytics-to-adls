"""Exception handling utilities and custom exceptions."""
import functools
import logging
import traceback
from typing import Any, Callable, TypeVar, Dict, Optional, Type, Union, List, Tuple

# Type variables for better typing
T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])

# Logger for this module
logger = logging.getLogger(__name__)


# Custom exceptions
class LogAnalyticsToADLSError(Exception):
    """Base exception for all application-specific errors."""


class ConfigurationError(LogAnalyticsToADLSError):
    """Raised when there is an issue with application configuration."""


class AuthenticationError(LogAnalyticsToADLSError):
    """Raised when there is an issue with authentication."""


class LogAnalyticsQueryError(LogAnalyticsToADLSError):
    """Raised when there is an issue querying Log Analytics."""


class StorageError(LogAnalyticsToADLSError):
    """Raised when there is an issue with Azure Storage operations."""


class KeyVaultError(LogAnalyticsToADLSError):
    """Raised when there is an issue with Azure KeyVault operations."""


class DataProcessingError(LogAnalyticsToADLSError):
    """Raised when there is an issue processing data."""


# Exception mapping to provide more specific error information
EXCEPTION_MAPPING: Dict[Type[Exception], Type[LogAnalyticsToADLSError]] = {
    ConnectionError: StorageError,
    TimeoutError: LogAnalyticsQueryError,
    PermissionError: AuthenticationError,
    ValueError: ConfigurationError,
}


def map_exception(
    exception: Exception, 
    default_exception_cls: Type[LogAnalyticsToADLSError] = LogAnalyticsToADLSError
) -> Exception:
    """
    Maps standard exceptions to application-specific exceptions.
    
    Args:
        exception: The original exception
        default_exception_cls: Default exception class to use if no mapping exists
    
    Returns:
        Mapped application-specific exception
    """
    for exception_cls, mapped_cls in EXCEPTION_MAPPING.items():
        if isinstance(exception, exception_cls):
            return mapped_cls(str(exception))
    
    return default_exception_cls(str(exception))


def handle_exceptions(
    reraise: bool = True,
    exception_handler: Optional[Callable[[Exception], None]] = None,
    mapped_exceptions: Optional[Dict[Type[Exception], Type[Exception]]] = None,
    logger_name: Optional[str] = None
) -> Callable[[F], F]:
    """
    A decorator for handling exceptions in a consistent way.
    
    Args:
        reraise: Whether to reraise the exception after handling
        exception_handler: Optional function to handle exceptions
        mapped_exceptions: Optional mapping of exception types to more specific types
        logger_name: Logger name for exception logs
    
    Returns:
        Decorated function with exception handling
    """
    local_logger = logging.getLogger(logger_name or __name__)
    local_mapped_exceptions = mapped_exceptions or EXCEPTION_MAPPING
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                local_logger.error(
                    f"Exception in {func.__name__}: {str(e)}\n{traceback.format_exc()}"
                )
                
                # Map the exception if possible
                mapped_exception = None
                for exc_type, target_exc_type in local_mapped_exceptions.items():
                    if isinstance(e, exc_type):
                        mapped_exception = target_exc_type(str(e))
                        break
                
                # Use the original if no mapping
                mapped_exception = mapped_exception or e
                
                # Call exception handler if provided
                if exception_handler:
                    exception_handler(mapped_exception)
                
                if reraise:
                    raise mapped_exception
        
        return wrapper  # type: ignore
    
    return decorator 