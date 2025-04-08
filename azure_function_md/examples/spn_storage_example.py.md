# spn_storage_example.py

```python
#!/usr/bin/env python
"""
SPN Storage Example

This example demonstrates how to use Service Principal (SPN) credentials for accessing
Azure Data Lake Storage Gen2 instead of using Managed Identity.

Usage:
1. Make sure SPN_CLIENT_ID_SECRET_NAME, SPN_CLIENT_SECRET_SECRET_NAME, and SPN_TENANT_ID_SECRET_NAME 
   are configured correctly in your local.settings.json
2. Ensure the SPN has the necessary permissions on the ADLS Gen2 storage account
3. Run this script: python spn_storage_example.py
"""

import asyncio
import os
import pandas as pd
import logging
from datetime import datetime

# Import services from the Azure Function
import sys
sys.path.append("..")  # Add the parent directory to the Python path
from LogAnalyticsToADLS.services.auth_service import AuthenticationService
from LogAnalyticsToADLS.services.storage_service import StorageService
from LogAnalyticsToADLS.config.settings import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("spn_storage_example")

# Set environment variable to use SPN for storage
os.environ["USE_SPN_FOR_STORAGE"] = "true"


async def spn_storage_example():
    """Run the SPN Storage example."""
    logger.info("Starting SPN Storage example")
    
    # Initialize services
    auth_service = AuthenticationService()
    
    # Verify that we're using SPN for storage
    settings = get_settings()
    logger.info(f"Using SPN for Storage: {settings.USE_SPN_FOR_STORAGE}")
    
    if not settings.USE_SPN_FOR_STORAGE:
        logger.error("USE_SPN_FOR_STORAGE is not set to true. Please set the environment variable.")
        return
    
    # Initialize storage service
    storage_service = StorageService(auth_service)
    
    # Create test data
    test_data = pd.DataFrame({
        'id': range(1, 11),
        'name': [f'Item {i}' for i in range(1, 11)],
        'value': [i * 10 for i in range(1, 11)],
        'timestamp': [datetime.now().isoformat() for _ in range(10)]
    })
    
    logger.info(f"Created test dataframe with {len(test_data)} rows.")
    
    # Create test directory
    test_dir = f"spn_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Ensure the test directory exists
    logger.info(f"Creating test directory: {test_dir}")
    storage_service.ensure_directory_exists(test_dir)
    
    # Test file upload with SPN credentials
    try:
        # Upload CSV file
        csv_path = f"{test_dir}/test_data.csv.gz"
        logger.info(f"Uploading test dataframe as CSV to {csv_path}")
        await storage_service.upload_dataframe_to_csv_gz(test_data, csv_path)
        
        # Upload Parquet file
        parquet_path = f"{test_dir}/test_data.parquet.gz"
        logger.info(f"Uploading test dataframe as Parquet to {parquet_path}")
        await storage_service.upload_dataframe_to_parquet_gz(test_data, parquet_path)
        
        # List files in the test directory
        logger.info(f"Listing files in {test_dir}")
        files = await storage_service.list_files(test_dir)
        for file in files:
            logger.info(f"Found file: {file}")
        
        # Verify the uploads were successful
        if len(files) == 2:
            logger.info("SPN authentication for Storage was successful!")
        else:
            logger.warning("Expected 2 files in the test directory, but found {len(files)}")
            
    except Exception as e:
        logger.error(f"Error during SPN storage example: {str(e)}")
        raise
    
    logger.info("SPN Storage example completed successfully")


if __name__ == "__main__":
    # Run the async example
    asyncio.run(spn_storage_example()) 
```
