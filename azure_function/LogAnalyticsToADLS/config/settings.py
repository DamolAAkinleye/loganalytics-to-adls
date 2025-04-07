import os
import json
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseSettings, Field


class QueryConfig:
    """Configuration for a single query."""
    
    def __init__(
        self,
        query: str,
        output_pattern: str,
        days: int = 1,
        file_format: str = "parquet",
        workspace_ids: Optional[List[str]] = None
    ):
        self.query = query
        self.output_pattern = output_pattern
        self.days = days
        self.file_format = file_format
        self.workspace_ids = workspace_ids  # List of workspace IDs or None to use default


class Settings(BaseSettings):
    """Application settings that can be loaded from environment variables."""
    
    # Azure KeyVault Settings
    KEYVAULT_NAME: str = Field(..., env="KEYVAULT_NAME")
    
    # Log Analytics Settings
    LOG_ANALYTICS_WORKSPACE_ID: str = Field(..., env="LOG_ANALYTICS_WORKSPACE_ID")
    LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS: str = Field("", env="LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS")
    SPN_CLIENT_ID_SECRET_NAME: str = Field(..., env="SPN_CLIENT_ID_SECRET_NAME")
    SPN_CLIENT_SECRET_SECRET_NAME: str = Field(..., env="SPN_CLIENT_SECRET_SECRET_NAME")
    SPN_TENANT_ID_SECRET_NAME: str = Field(..., env="SPN_TENANT_ID_SECRET_NAME")
    
    # Storage Account Settings
    STORAGE_ACCOUNT_NAME: str = Field(..., env="STORAGE_ACCOUNT_NAME")
    STORAGE_CONTAINER_NAME: str = Field(..., env="STORAGE_CONTAINER_NAME")
    STAGING_DIRECTORY: str = Field("staging", env="STAGING_DIRECTORY")
    FINAL_DIRECTORY: str = Field("processed", env="FINAL_DIRECTORY")
    USE_SPN_FOR_STORAGE: bool = Field(
        False, 
        env="USE_SPN_FOR_STORAGE",
        description="Whether to use SPN credentials for storage instead of managed identity"
    )
    
    # Development Settings
    USE_ENV_VARS_FOR_SECRETS: bool = Field(
        True,
        env="USE_ENV_VARS_FOR_SECRETS",
        description="Whether to use environment variables for secrets during local development"
    )
    
    # Query Settings
    QUERIES_CONFIG: str = Field(
        '[{"query": "AzureActivity | where TimeGenerated >= ago(1d)", "output_pattern": "azure_activity", "days": 1, "file_format": "parquet"}]',
        env="QUERIES_CONFIG"
    )
    MAX_ROWS_PER_FILE: int = Field(100000, env="MAX_ROWS_PER_FILE")
    
    # Timer trigger settings
    TIMER_SCHEDULE: str = Field("0 0 * * * *", env="TIMER_SCHEDULE")  # Default: Run hourly at 0 minutes
    
    # Processing settings
    CHUNK_SIZE: int = Field(100000, env="CHUNK_SIZE")
    ROW_PROCESS_SIZE: int = Field(10000, env="ROW_PROCESS_SIZE")
    DELETE_STAGING_FILES: bool = Field(False, env="DELETE_STAGING_FILES")
    
    class Config:
        case_sensitive = True
        env_file = ".env"
    
    def get_all_workspace_ids(self) -> List[str]:
        """Return all configured workspace IDs."""
        workspace_ids = [self.LOG_ANALYTICS_WORKSPACE_ID]
        
        # Add additional workspace IDs if configured
        if self.LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS:
            try:
                additional_ids = json.loads(self.LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS)
                if isinstance(additional_ids, list):
                    workspace_ids.extend(additional_ids)
                else:
                    workspace_ids.append(str(additional_ids))
            except json.JSONDecodeError:
                # Treat as comma-separated string if not valid JSON
                additional_ids = [
                    workspace_id.strip() 
                    for workspace_id in self.LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS.split(",")
                    if workspace_id.strip()
                ]
                workspace_ids.extend(additional_ids)
        
        # Remove duplicates while preserving order
        unique_ids = []
        for workspace_id in workspace_ids:
            if workspace_id not in unique_ids:
                unique_ids.append(workspace_id)
                
        return unique_ids
    
    def get_queries(self) -> List[QueryConfig]:
        """Parse and return the queries configuration."""
        try:
            queries_data = json.loads(self.QUERIES_CONFIG)
            return [
                QueryConfig(
                    query=item.get("query", ""),
                    output_pattern=item.get("output_pattern", "query_result"),
                    days=item.get("days", 1),
                    file_format=item.get("file_format", "parquet"),
                    workspace_ids=item.get("workspace_ids")
                )
                for item in queries_data
            ]
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid QUERIES_CONFIG format: {e}")
        except Exception as e:
            raise ValueError(f"Error parsing QUERIES_CONFIG: {e}")


_SETTINGS = None


def get_settings() -> Settings:
    """
    Get the application settings singleton.
    
    Returns:
        Settings: The application settings
    """
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = Settings()
    return _SETTINGS


# Storage helper functions
def get_storage_account_name() -> str:
    """Get the configured storage account name."""
    return get_settings().STORAGE_ACCOUNT_NAME


def get_storage_container_name() -> str:
    """Get the configured storage container name."""
    return get_settings().STORAGE_CONTAINER_NAME


def get_local_storage_connection_string() -> str:
    """
    Get the local development connection string for Azurite blob emulator.
    
    Returns:
        str: The connection string for the local Azurite emulator
    """
    return "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"


def is_local_development() -> bool:
    """
    Check if the application is running in a local development environment.
    
    Returns:
        bool: True if running in a local development environment
    """
    # Check environment variable set by Azure Functions
    env_setting = os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT", "").lower() == "development"
    
    # Try connecting to local emulator
    import socket
    try:
        # Try to connect to Azurite blob emulator
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)  # Short timeout
        s.connect(("127.0.0.1", 10000))
        s.close()
        emulator_available = True
    except (socket.error, socket.timeout):
        emulator_available = False
    
    return env_setting or emulator_available


# Log Analytics helper functions
def get_log_analytics_workspace_id() -> str:
    """Get the configured Log Analytics workspace ID."""
    return get_settings().LOG_ANALYTICS_WORKSPACE_ID


def get_log_analytics_spn_client_id() -> str:
    """Get the SPN client ID secret name."""
    return get_settings().SPN_CLIENT_ID_SECRET_NAME


def get_log_analytics_spn_client_secret() -> str:
    """Get the SPN client secret secret name."""
    return get_settings().SPN_CLIENT_SECRET_SECRET_NAME


def get_log_analytics_spn_tenant_id() -> str:
    """Get the SPN tenant ID secret name."""
    return get_settings().SPN_TENANT_ID_SECRET_NAME


def use_spn_for_storage() -> bool:
    """
    Check if SPN credentials should be used for storage access instead of managed identity.
    
    Returns:
        bool: True if SPN should be used for storage
    """
    return get_settings().USE_SPN_FOR_STORAGE


def use_env_vars_for_secrets() -> bool:
    """
    Check if environment variables should be used for secrets during local development.
    
    Returns:
        bool: True if environment variables should be used for secrets
    """
    return get_settings().USE_ENV_VARS_FOR_SECRETS and is_local_development()


def get_log_analytics_additional_workspace_ids() -> List[str]:
    """Get the configured Log Analytics additional workspace IDs."""
    return get_settings().LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS.split(",") if get_settings().LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS else [] 