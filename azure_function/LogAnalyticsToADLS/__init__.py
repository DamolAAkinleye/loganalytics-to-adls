# Main package initialization
"""
Azure Function to extract Log Analytics data to Azure Data Lake Storage Gen2.
"""

from .services.auth_service import AuthenticationService
from .services.log_analytics_service import LogAnalyticsService
from .services.storage_service import StorageService
from .services.processor import LogAnalyticsToADLSProcessor
from .config import get_settings, Settings 