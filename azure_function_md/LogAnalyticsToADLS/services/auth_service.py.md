# auth_service.py

```python
"""Authentication service for retrieving secrets and managing authentication."""
import logging
from typing import Dict, Optional, Any, Union
import os

from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import ResourceNotFoundError, ClientAuthenticationError

from ..config import get_settings
from ..utils.retry_utils import async_retry, tenacity_retry
from ..utils.exceptions import (
    AuthenticationError, 
    KeyVaultError,
    handle_exceptions
)
from ..config.settings import use_env_vars_for_secrets

# Logger for this module
logger = logging.getLogger(__name__)


class AuthenticationService:
    """Service for managing authentication to Azure services."""
    
    def __init__(self):
        """Initialize the auth service using settings."""
        self.settings = get_settings()
        self.keyvault_url = f"https://{self.settings.KEYVAULT_NAME}.vault.azure.net/"
        self._managed_identity_credential = None
        self._spn_credential = None
        self._keyvault_client = None
        self._secret_cache: Dict[str, str] = {}
    
    @property
    def managed_identity_credential(self) -> DefaultAzureCredential:
        """
        Get or create the managed identity credential.
        
        Returns:
            DefaultAzureCredential: The managed identity credential
        """
        if self._managed_identity_credential is None:
            try:
                self._managed_identity_credential = DefaultAzureCredential(
                    exclude_interactive_browser_credential=True,
                    exclude_shared_token_cache_credential=True,
                    exclude_visual_studio_credential=True,
                    exclude_environment_credential=True,
                    exclude_cli_credential=True
                )
                logger.info("Created managed identity credential")
            except Exception as ex:
                logger.error(f"Failed to create managed identity credential: {str(ex)}")
                raise AuthenticationError(f"Failed to create managed identity credential: {str(ex)}")
                
        return self._managed_identity_credential
    
    @property
    def keyvault_client(self) -> SecretClient:
        """
        Get or create the KeyVault client using managed identity.
        
        Returns:
            SecretClient: The KeyVault client
        """
        if self._keyvault_client is None:
            try:
                self._keyvault_client = SecretClient(
                    vault_url=self.keyvault_url,
                    credential=self.managed_identity_credential
                )
                logger.info(f"Created KeyVault client for {self.keyvault_url}")
            except Exception as ex:
                logger.error(f"Failed to create KeyVault client: {str(ex)}")
                raise KeyVaultError(f"Failed to create KeyVault client: {str(ex)}")
                
        return self._keyvault_client
    
    @tenacity_retry((ResourceNotFoundError, ClientAuthenticationError), max_attempts=3)
    @handle_exceptions(reraise=True, logger_name=__name__)
    def get_secret(self, secret_name: str) -> str:
        """
        Get a secret from Azure KeyVault or environment variables.
        During local development, it will check for environment variables first if USE_ENV_VARS_FOR_SECRETS is True.
        
        Args:
            secret_name: The name of the secret to retrieve
            
        Returns:
            str: The secret value
            
        Raises:
            KeyVaultError: If the secret cannot be retrieved
        """
        # Check cache first
        if secret_name in self._secret_cache:
            return self._secret_cache[secret_name]
        
        # Check if we should try to get the secret from environment variables
        if use_env_vars_for_secrets():
            # Convert secret name to environment variable name format
            # Example: Convert "loganalytics-spn-client-id" to "SPN_CLIENT_ID"
            env_var_name = secret_name.replace("loganalytics-", "").replace("-", "_").upper()
            secret_value = os.environ.get(env_var_name)
            
            # Also check for direct mapping of secret name as environment variable
            if not secret_value:
                secret_value = os.environ.get(secret_name)
                
            if secret_value:
                logger.info(f"Retrieved secret {secret_name} from environment variable")
                # Cache the secret
                self._secret_cache[secret_name] = secret_value
                return secret_value
            
            logger.warning(f"Secret {secret_name} not found in environment variables, falling back to Key Vault")
        
        try:
            logger.info(f"Retrieving secret {secret_name} from KeyVault")
            secret = self.keyvault_client.get_secret(secret_name)
            secret_value = secret.value
            
            # Cache the secret
            self._secret_cache[secret_name] = secret_value
            
            return secret_value
        except ResourceNotFoundError:
            logger.error(f"Secret {secret_name} not found in KeyVault")
            raise KeyVaultError(f"Secret {secret_name} not found in KeyVault")
        except Exception as ex:
            logger.error(f"Error retrieving secret {secret_name}: {str(ex)}")
            raise KeyVaultError(f"Error retrieving secret {secret_name}: {str(ex)}")
    
    async def get_spn_credential(self) -> ClientSecretCredential:
        """
        Get or create the SPN credential for Log Analytics using details from KeyVault.
        
        Returns:
            ClientSecretCredential: The SPN credential
            
        Raises:
            AuthenticationError: If the SPN credential cannot be created
        """
        if self._spn_credential is None:
            try:
                # Get SPN details from KeyVault
                client_id = self.get_secret(self.settings.SPN_CLIENT_ID_SECRET_NAME)
                client_secret = self.get_secret(self.settings.SPN_CLIENT_SECRET_SECRET_NAME)
                tenant_id = self.get_secret(self.settings.SPN_TENANT_ID_SECRET_NAME)
                
                self._spn_credential = ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret
                )
                logger.info("Created SPN credential for Log Analytics")
            except KeyVaultError as ex:
                logger.error(f"Failed to retrieve SPN credentials from KeyVault: {str(ex)}")
                raise AuthenticationError(f"Failed to retrieve SPN credentials from KeyVault: {str(ex)}")
            except Exception as ex:
                logger.error(f"Failed to create SPN credential: {str(ex)}")
                raise AuthenticationError(f"Failed to create SPN credential: {str(ex)}")
                
        return self._spn_credential
        
    def get_storage_credential(self):
        """
        Get the appropriate credential for Azure Storage access.
        Returns either managed identity or SPN credential based on settings.
        
        Returns:
            Union[DefaultAzureCredential, ClientSecretCredential]: The credential to use for storage
            
        Raises:
            AuthenticationError: If the credential cannot be created
        """
        # Check if we should use SPN for storage - handle both boolean and string values
        use_spn_setting = self.settings.USE_SPN_FOR_STORAGE
        use_spn = use_spn_setting if isinstance(use_spn_setting, bool) else (
            str(use_spn_setting).lower() in ('true', 't', 'yes', 'y', '1')
        )
        
        if use_spn:
            logger.info("Using SPN credential for Azure Storage access")
            # Use SPN credential - this is async so we need to handle it
            import asyncio
            try:
                # If we're already in an event loop, use get_event_loop
                loop = asyncio.get_event_loop()
                return loop.run_until_complete(self.get_spn_credential())
            except RuntimeError:
                # If no event loop is available, create a new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(self.get_spn_credential())
                finally:
                    loop.close()
        else:
            logger.info("Using managed identity for Azure Storage access")
            # Use managed identity
            return self.managed_identity_credential 
```
