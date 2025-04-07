#!/usr/bin/env python
"""
Local Secrets Example

This example demonstrates how to use environment variables for secrets during local development
instead of connecting to Azure Key Vault. This makes local development easier, especially
when working offline or without access to Azure resources.

Usage:
1. Set environment variables for your secrets or add them to local.settings.json
2. Make sure USE_ENV_VARS_FOR_SECRETS is set to "true" in your local.settings.json
3. Run this script: python local_secrets_example.py
"""

import os
import logging
from pprint import pprint

# Import services from the Azure Function
import sys
sys.path.append("..")  # Add the parent directory to the Python path
from LogAnalyticsToADLS.services.auth_service import AuthenticationService
from LogAnalyticsToADLS.config.settings import get_settings, use_env_vars_for_secrets, is_local_development

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("local_secrets_example")

# Set environment variables for secrets if they don't exist already
if not os.environ.get("SPN_CLIENT_ID"):
    os.environ["SPN_CLIENT_ID"] = "example-client-id-from-script"
    
if not os.environ.get("SPN_CLIENT_SECRET"):
    os.environ["SPN_CLIENT_SECRET"] = "example-client-secret-from-script"
    
if not os.environ.get("SPN_TENANT_ID"):
    os.environ["SPN_TENANT_ID"] = "example-tenant-id-from-script"

# Set environment variables for local development
os.environ["AZURE_FUNCTIONS_ENVIRONMENT"] = "Development"
os.environ["USE_ENV_VARS_FOR_SECRETS"] = "true"


def local_secrets_example():
    """Run the local secrets example."""
    logger.info("Starting local secrets example")
    
    # Check if we're in local development mode
    local_dev = is_local_development()
    logger.info(f"Running in local development mode: {local_dev}")
    
    # Check if we're using environment variables for secrets
    using_env_vars = use_env_vars_for_secrets()
    logger.info(f"Using environment variables for secrets: {using_env_vars}")
    
    if not local_dev:
        logger.warning("Not running in local development mode. This example may not work as expected.")
        
    if not using_env_vars:
        logger.warning("Not using environment variables for secrets. This example may not work as expected.")
    
    # Initialize the authentication service
    auth_service = AuthenticationService()
    
    # Try to get the secrets
    try:
        # Get the SPN credentials from environment variables
        client_id_secret_name = get_settings().SPN_CLIENT_ID_SECRET_NAME
        client_secret_secret_name = get_settings().SPN_CLIENT_SECRET_SECRET_NAME
        tenant_id_secret_name = get_settings().SPN_TENANT_ID_SECRET_NAME
        
        logger.info(f"Secret names configured in settings:")
        logger.info(f"  Client ID secret name: {client_id_secret_name}")
        logger.info(f"  Client Secret secret name: {client_secret_secret_name}")
        logger.info(f"  Tenant ID secret name: {tenant_id_secret_name}")
        
        # Get the secrets
        client_id = auth_service.get_secret(client_id_secret_name)
        client_secret = auth_service.get_secret(client_secret_secret_name)
        tenant_id = auth_service.get_secret(tenant_id_secret_name)
        
        # Check if we got the secrets from environment variables
        # (we'll mask the actual values for security)
        logger.info("Successfully retrieved secrets:")
        logger.info(f"  Client ID: {client_id[:4]}...{client_id[-4:] if len(client_id) > 8 else ''}")
        logger.info(f"  Client Secret: {'*' * min(len(client_secret), 10)}")
        logger.info(f"  Tenant ID: {tenant_id[:4]}...{tenant_id[-4:] if len(tenant_id) > 8 else ''}")
        
        # Show where each secret was retrieved from (KeyVault or environment variable)
        logger.info("Created SPN credential successfully!")
    except Exception as e:
        logger.error(f"Error getting secrets: {str(e)}")
    logger.info("Local secrets example completed")


if __name__ == "__main__":
    # Run the example
    local_secrets_example() 