#!/usr/bin/env python
"""
Local Development Example with Blob Emulator

This example demonstrates how to use the Azure Function with the local Azurite blob emulator.
It shows how to set up the environment, initialize the services, and test file operations
using the emulator instead of ADLS Gen2.

Prerequisites:
1. Azurite emulator installed and running (npm install -g azurite)
2. Azure credentials for Log Analytics (can use local.settings.json file)

Usage:
1. Start Azurite: azurite --silent --location azurite-data --debug azurite-debug.log
2. Run this script: python local_dev_example.py
"""

import asyncio
import os
import pandas as pd
import logging
import tempfile
from datetime import datetime

# Import services from the Azure Function
import sys
sys.path.append("..")  # Add the parent directory to the Python path
from LogAnalyticsToADLS.services.auth_service import AuthService
from LogAnalyticsToADLS.services.log_analytics_service import LogAnalyticsService
from LogAnalyticsToADLS.services.storage_service import StorageService
from LogAnalyticsToADLS.config.settings import (
    get_storage_account_name,
    get_storage_container_name,
    get_log_analytics_spn_client_id,
    get_log_analytics_spn_client_secret,
    get_log_analytics_spn_tenant_id,
    get_log_analytics_workspace_id
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("local_dev_example")

# Set environment variable to trigger local dev detection
os.environ["AZURE_FUNCTIONS_ENVIRONMENT"] = "Development"


async def local_dev_example():
    """Run the local development example with blob emulator."""
    logger.info("Starting local development example with blob emulator")
    
    # Get configuration
    storage_account_name = get_storage_account_name()
    container_name = get_storage_container_name()
    
    # Initialize services
    auth_service = AuthService(
        client_id=get_log_analytics_spn_client_id(),
        client_secret=get_log_analytics_spn_client_secret(),
        tenant_id=get_log_analytics_spn_tenant_id()
    )
    
    log_analytics_service = LogAnalyticsService(
        auth_service=auth_service,
        workspace_id=get_log_analytics_workspace_id()
    )
    
    storage_service = StorageService(
        auth_service=auth_service,
        storage_account_name=storage_account_name,
        container_name=container_name
    )
    
    # Check if using local emulator
    logger.info(f"Using local blob emulator: {storage_service.use_local_emulator}")
    if not storage_service.use_local_emulator:
        logger.warning("Not using local emulator! Make sure Azurite is running on port 10000.")
        return
    
    # Create test data
    test_data = pd.DataFrame({
        'id': range(1, 11),
        'name': [f'Item {i}' for i in range(1, 11)],
        'value': [i * 10 for i in range(1, 11)],
        'timestamp': [datetime.now().isoformat() for _ in range(10)]
    })
    
    logger.info(f"Created test dataframe with {len(test_data)} rows.")
    
    # Create testing directories
    test_dir = f"test_local_dev/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    staging_dir = f"{test_dir}/staging"
    processed_dir = f"{test_dir}/processed"
    
    # Ensure directories exist
    logger.info(f"Creating test directories: {staging_dir} and {processed_dir}")
    storage_service.ensure_directory_exists(staging_dir)
    storage_service.ensure_directory_exists(processed_dir)
    
    # Test operations
    try:
        # 1. Upload CSV file
        csv_path = f"{staging_dir}/test_data.csv.gz"
        logger.info(f"Uploading test dataframe as CSV to {csv_path}")
        await storage_service.upload_dataframe_to_csv_gz(test_data, csv_path)
        
        # 2. Upload Parquet file
        parquet_path = f"{staging_dir}/test_data.parquet.gz"
        logger.info(f"Uploading test dataframe as Parquet to {parquet_path}")
        await storage_service.upload_dataframe_to_parquet_gz(test_data, parquet_path)
        
        # 3. List files in staging directory
        logger.info(f"Listing files in {staging_dir}")
        files = await storage_service.list_files(staging_dir)
        for file in files:
            logger.info(f"Found file: {file}")
        
        # 4. Copy a file
        copy_path = f"{staging_dir}/test_data_copy.csv.gz"
        logger.info(f"Copying {csv_path} to {copy_path}")
        await storage_service.copy_file(csv_path, copy_path)
        
        # 5. Rename a file
        rename_path = f"{staging_dir}/test_data_renamed.csv.gz"
        logger.info(f"Renaming {copy_path} to {rename_path}")
        await storage_service.rename_file(copy_path, rename_path)
        
        # 6. List files again to confirm operations
        logger.info(f"Listing files in {staging_dir} after operations")
        files = await storage_service.list_files(staging_dir)
        for file in files:
            logger.info(f"Found file: {file}")
        
        # 7. Move files to processed directory
        logger.info(f"Moving all files from {staging_dir} to {processed_dir}")
        moved_files = await storage_service.move_all_files(staging_dir, processed_dir)
        logger.info(f"Moved files: {moved_files}")
        
        # 8. Verify files in processed directory
        logger.info(f"Listing files in {processed_dir}")
        processed_files = await storage_service.list_files(processed_dir)
        for file in processed_files:
            logger.info(f"Found file: {file}")
        
        # 9. Create temp file and upload
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as temp_file:
            temp_file.write("Test content for direct file upload")
            temp_file_path = temp_file.name
        
        upload_path = f"{processed_dir}/uploaded_file.txt"
        logger.info(f"Uploading local file {temp_file_path} to {upload_path}")
        
        with open(temp_file_path, 'rb') as file_data:
            await storage_service.upload_file(file_data, upload_path)
        
        # Clean up temp file
        os.unlink(temp_file_path)
        
        # 10. Verify all files in the test directory
        logger.info(f"Final file listing for {test_dir}")
        all_files = []
        # List staging
        staging_files = await storage_service.list_files(staging_dir)
        all_files.extend(staging_files)
        # List processed
        processed_files = await storage_service.list_files(processed_dir)
        all_files.extend(processed_files)
        
        logger.info(f"Total files: {len(all_files)}")
        for file in all_files:
            logger.info(f"File: {file}")
            
    except Exception as e:
        logger.error(f"Error during local development example: {str(e)}")
        raise
    
    logger.info("Local development example completed successfully")


if __name__ == "__main__":
    # Run the async example
    asyncio.run(local_dev_example()) 