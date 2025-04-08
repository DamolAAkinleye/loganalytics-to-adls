# storage_service.py

```python
"""Service for managing Azure Storage operations (both ADLS Gen2 and Blob Storage)."""
import asyncio
import gzip
import io
import logging
import time
import os
import socket
from datetime import datetime
from typing import Dict, List, Optional, Any, Set, Tuple, BinaryIO, Union

import pandas as pd
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    FileSystemClient,
    DataLakeDirectoryClient,
    DataLakeFileClient
)
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    BlobClient
)
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError, ServiceRequestError

from ..config import get_settings
from ..utils.retry_utils import async_retry
from ..utils.exceptions import (
    StorageError,
    handle_exceptions
)
from .auth_service import AuthenticationService
from ..config.settings import is_local_development, get_local_storage_connection_string

# Logger for this module
logger = logging.getLogger(__name__)


class StorageService:
    """
    Service for managing Azure Storage operations.
    
    Supports both ADLS Gen2 in Azure and Blob Storage in Azurite emulator for local development.
    For ADLS Gen2 access, can use either Managed Identity (default) or Service Principal (SPN) credentials
    based on the USE_SPN_FOR_STORAGE setting.
    """
    
    def __init__(self, auth_service: AuthenticationService):
        """
        Initialize the storage service.
        
        Args:
            auth_service: The authentication service to use for retrieving credentials
        """
        self.settings = get_settings()
        self.auth_service = auth_service
        self.storage_account_name = self.settings.STORAGE_ACCOUNT_NAME
        self.container_name = self.settings.STORAGE_CONTAINER_NAME
        self.staging_directory = self.settings.STAGING_DIRECTORY
        self.final_directory = self.settings.FINAL_DIRECTORY
        
        # Determine if we're in a local development environment
        self.use_local_emulator = self._is_local_development()
        
        if self.use_local_emulator:
            # Use Azurite local emulator connection
            self._storage_url = "http://127.0.0.1:10000/devstoreaccount1"
            logger.info(f"Using local Azurite Blob Storage emulator at {self._storage_url}")
        else:
            # Use ADLS Gen2 in Azure
            self._storage_url = f"https://{self.storage_account_name}.dfs.core.windows.net"
            logger.info(f"Using Azure Data Lake Storage Gen2 at {self._storage_url}")
            
        self._service_client = None
        self._file_system_client = None
        self._blob_service_client = None
        self._blob_container_client = None
        
    @property
    def use_local_emulator(self) -> bool:
        """
        Check if the service should use the local blob emulator.
        
        Returns:
            bool: True if local emulator should be used
        """
        return is_local_development()
    
    def _is_local_development(self) -> bool:
        """
        Deprecated method - use use_local_emulator property instead.
        Check if we're running in a local development environment.
        
        Returns:
            bool: True if running locally
        """
        return is_local_development()
    
    @property
    def service_client(self) -> DataLakeServiceClient:
        """
        Get or create the ADLS Gen2 service client.
        Used for Azure environments.
        
        Returns:
            DataLakeServiceClient: The service client
        """
        if self.use_local_emulator:
            raise StorageError("Cannot use ADLS Gen2 client with local emulator. Use blob_service_client instead.")
            
        if self._service_client is None:
            try:
                self._service_client = DataLakeServiceClient(
                    account_url=self._storage_url,
                    credential=self.auth_service.get_storage_credential()
                )
                logger.info(f"Created ADLS Gen2 service client for {self._storage_url}")
            except Exception as ex:
                logger.error(f"Failed to create ADLS Gen2 service client: {str(ex)}")
                raise StorageError(f"Failed to create ADLS Gen2 service client: {str(ex)}")
                
        return self._service_client
    
    @property
    def blob_service_client(self) -> BlobServiceClient:
        """
        Get or create the Blob Storage service client.
        Used for local emulator environments.
        
        Returns:
            BlobServiceClient: The blob service client
        """
        if not self.use_local_emulator:
            raise StorageError("Cannot use Blob Storage client in Azure. Use service_client instead.")
            
        if self._blob_service_client is None:
            try:
                # Connect to local Azurite emulator
                # Note: Azurite uses a fixed development connection string
                conn_str = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
                self._blob_service_client = BlobServiceClient.from_connection_string(conn_str)
                logger.info("Created Blob Storage client for local emulator")
            except Exception as ex:
                logger.error(f"Failed to create Blob Storage client: {str(ex)}")
                raise StorageError(f"Failed to create Blob Storage client: {str(ex)}")
                
        return self._blob_service_client
    
    @property
    def file_system_client(self) -> Union[FileSystemClient, ContainerClient]:
        """
        Get or create the file system (container) client.
        Works with both ADLS Gen2 and Blob Storage.
        
        Returns:
            Union[FileSystemClient, ContainerClient]: The file system or container client
        """
        if self._file_system_client is None:
            try:
                if self.use_local_emulator:
                    # Create container client for blob storage
                    self._file_system_client = self.blob_service_client.get_container_client(self.container_name)
                    
                    # Create container if it doesn't exist
                    try:
                        self._file_system_client.get_container_properties()
                    except ResourceNotFoundError:
                        self._file_system_client.create_container()
                        
                    logger.info(f"Created blob container client for {self.container_name}")
                else:
                    # Use ADLS Gen2 file system client
                    self._file_system_client = self.service_client.get_file_system_client(self.container_name)
                    logger.info(f"Created ADLS file system client for {self.container_name}")
                    
            except Exception as ex:
                logger.error(f"Failed to create file system client: {str(ex)}")
                raise StorageError(f"Failed to create file system client: {str(ex)}")
                
        return self._file_system_client
    
    def ensure_directory_exists(self, directory_path: str) -> Union[DataLakeDirectoryClient, None]:
        """
        Ensure a directory exists, creating it if necessary.
        Works with both ADLS Gen2 and Blob Storage (Blob Storage doesn't have real directories).
        
        Args:
            directory_path: The path of the directory to ensure
            
        Returns:
            Union[DataLakeDirectoryClient, None]: The directory client (or None for blob storage)
            
        Raises:
            StorageError: If the directory cannot be created
        """
        try:
            if self.use_local_emulator:
                # Blob Storage doesn't have actual directories, 
                # but we can create a placeholder blob to simulate directory
                if directory_path and not directory_path.endswith('/'):
                    directory_path = directory_path + '/'
                    
                # Create placeholder blob if needed
                placeholder_blob = self.file_system_client.get_blob_client(directory_path + '.placeholder')
                try:
                    placeholder_blob.get_blob_properties()
                    logger.info(f"Directory {directory_path} already exists in blob storage")
                except ResourceNotFoundError:
                    placeholder_blob.upload_blob(b'', overwrite=True)
                    logger.info(f"Created directory placeholder {directory_path} in blob storage")
                    
                return None  # No directory client in blob storage
            else:
                # Use ADLS Gen2 directory client
                directory_client = self.file_system_client.get_directory_client(directory_path)
                
                # Create the directory if it doesn't exist
                try:
                    directory_properties = directory_client.get_directory_properties()
                    logger.info(f"Directory {directory_path} already exists")
                except ResourceNotFoundError:
                    directory_client.create_directory()
                    logger.info(f"Created directory {directory_path}")
                    
                return directory_client
                
        except Exception as ex:
            logger.error(f"Error ensuring directory {directory_path} exists: {str(ex)}")
            raise StorageError(f"Error ensuring directory {directory_path} exists: {str(ex)}")
    
    @async_retry((HttpResponseError, ConnectionError, ServiceRequestError), max_attempts=3)
    async def upload_dataframe_to_csv_gz(
        self, 
        df: pd.DataFrame, 
        directory_path: str,
        file_name: str,
        overwrite: bool = True,
        chunksize: int = 10000
    ) -> str:
        """
        Upload a Pandas DataFrame to a gzipped CSV file.
        Works with both ADLS Gen2 and Blob Storage.
        
        Args:
            df: The DataFrame to upload
            directory_path: The directory path in the container
            file_name: The name of the file to create (will have .gz appended if not present)
            overwrite: Whether to overwrite an existing file
            chunksize: Number of rows to process at a time for memory efficiency
            
        Returns:
            str: The path of the uploaded file
            
        Raises:
            StorageError: If the file cannot be uploaded
        """
        if df.empty:
            logger.warning("DataFrame is empty, not uploading")
            return ""
            
        # Ensure the file has a .gz extension
        if not file_name.endswith('.gz'):
            file_name = f"{file_name}.gz"
            
        # Ensure the directory exists
        self.ensure_directory_exists(directory_path)
        
        # Create the file path
        file_path = f"{directory_path}/{file_name}"
        
        try:
            if self.use_local_emulator:
                # Use Blob Storage for local development
                blob_client = self.file_system_client.get_blob_client(file_path)
                
                # Check if file exists and handle overwrite
                try:
                    blob_client.get_blob_properties()
                    if not overwrite:
                        logger.info(f"File {file_path} already exists, not overwriting")
                        return file_path
                    logger.info(f"File {file_path} already exists, overwriting")
                except ResourceNotFoundError:
                    pass
                
                # For blob storage with small files, we'll process all at once to keep it simple
                # For larger files in production, the chunked approach with ADLS Gen2 is better
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_content = csv_buffer.getvalue().encode('utf-8')
                compressed_content = gzip.compress(csv_content)
                
                # Upload the compressed file
                blob_client.upload_blob(compressed_content, overwrite=True)
                logger.info(f"Successfully uploaded {len(df)} rows to {file_path} in blob storage")
                return file_path
                
            else:
                # Use ADLS Gen2 for production with chunked uploads
                file_client = self.file_system_client.get_file_client(file_path)
                
                # Check if file exists and handle overwrite
                try:
                    file_properties = file_client.get_file_properties()
                    if not overwrite:
                        logger.info(f"File {file_path} already exists, not overwriting")
                        return file_path
                    logger.info(f"File {file_path} already exists, overwriting")
                except ResourceNotFoundError:
                    pass
                
                # Create file and set up for streaming
                file_client.create_file(overwrite=overwrite)
                
                # Process DataFrame in chunks to save memory
                offset = 0
                total_bytes_written = 0
                total_rows_processed = 0
                
                # Calculate the approximate number of chunks needed
                total_rows = len(df)
                n_chunks = (total_rows + chunksize - 1) // chunksize  # Ceiling division
                
                for chunk_idx in range(n_chunks):
                    start_idx = chunk_idx * chunksize
                    end_idx = min(start_idx + chunksize, total_rows)
                    
                    # Get the DataFrame chunk
                    df_chunk = df.iloc[start_idx:end_idx]
                    
                    # Convert to CSV, compress with gzip
                    csv_buffer = io.StringIO()
                    
                    # First chunk includes header, subsequent chunks don't
                    df_chunk.to_csv(csv_buffer, index=False, header=(chunk_idx == 0))
                    
                    # Get the CSV content and compress it
                    csv_content = csv_buffer.getvalue().encode('utf-8')
                    compressed_content = gzip.compress(csv_content)
                    
                    # Upload the compressed chunk
                    length = len(compressed_content)
                    if length > 0:
                        file_client.append_data(data=compressed_content, offset=offset, length=length)
                        offset += length
                        total_bytes_written += length
                        total_rows_processed += len(df_chunk)
                        
                        logger.debug(f"Uploaded chunk {chunk_idx+1}/{n_chunks} ({len(df_chunk)} rows, {length} bytes)")
                
                # Flush and close the file
                file_client.flush_data(offset)
                logger.info(f"Successfully uploaded {total_rows_processed} rows to {file_path} ({total_bytes_written} bytes compressed)")
                
                return file_path
        except Exception as ex:
            logger.error(f"Error uploading DataFrame to {file_path}: {str(ex)}")
            raise StorageError(f"Error uploading DataFrame to {file_path}: {str(ex)}")
    
    @async_retry((HttpResponseError, ConnectionError, ServiceRequestError), max_attempts=3)
    async def upload_dataframe_to_parquet_gz(
        self, 
        df: pd.DataFrame, 
        directory_path: str,
        file_name: str,
        overwrite: bool = True,
        compression: str = 'gzip',
        row_group_size: int = 10000
    ) -> str:
        """
        Upload a Pandas DataFrame to a gzipped Parquet file.
        Works with both ADLS Gen2 and Blob Storage.
        
        Args:
            df: The DataFrame to upload
            directory_path: The directory path in the container
            file_name: The name of the file to create
            overwrite: Whether to overwrite an existing file
            compression: Compression codec for Parquet (gzip, snappy, etc.)
            row_group_size: Number of rows per row group in Parquet for memory efficiency
            
        Returns:
            str: The path of the uploaded file
            
        Raises:
            StorageError: If the file cannot be uploaded
        """
        if df.empty:
            logger.warning("DataFrame is empty, not uploading")
            return ""
            
        # Ensure the file has a .parquet extension
        if not file_name.endswith('.parquet'):
            file_name = f"{file_name}.parquet"
            
        # Ensure the directory exists
        self.ensure_directory_exists(directory_path)
        
        # Create the file path
        file_path = f"{directory_path}/{file_name}"
        
        try:
            # Create an in-memory buffer for the Parquet data
            parquet_buffer = io.BytesIO()
            
            # Write the DataFrame to Parquet format with compression
            df.to_parquet(
                parquet_buffer, 
                index=False, 
                compression=compression,
                row_group_size=row_group_size
            )
            
            # Reset buffer to the beginning
            parquet_buffer.seek(0)
            parquet_data = parquet_buffer.read()
            
            if self.use_local_emulator:
                # Use Blob Storage for local development
                blob_client = self.file_system_client.get_blob_client(file_path)
                
                # Check if file exists and handle overwrite
                try:
                    blob_client.get_blob_properties()
                    if not overwrite:
                        logger.info(f"File {file_path} already exists, not overwriting")
                        return file_path
                    logger.info(f"File {file_path} already exists, overwriting")
                except ResourceNotFoundError:
                    pass
                
                # Upload the parquet data
                blob_client.upload_blob(parquet_data, overwrite=True)
                logger.info(f"Successfully uploaded {len(df)} rows to {file_path} in Parquet format to blob storage")
                
                return file_path
                
            else:
                # Use ADLS Gen2 for production
                file_client = self.file_system_client.get_file_client(file_path)
                
                # Check if file exists and handle overwrite
                try:
                    file_properties = file_client.get_file_properties()
                    if not overwrite:
                        logger.info(f"File {file_path} already exists, not overwriting")
                        return file_path
                    logger.info(f"File {file_path} already exists, overwriting")
                except ResourceNotFoundError:
                    pass
                
                # Upload the parquet data
                file_client.upload_data(parquet_data, overwrite=overwrite)
                
                logger.info(f"Successfully uploaded {len(df)} rows to {file_path} in Parquet format with {compression} compression")
                
                return file_path
                
        except Exception as ex:
            logger.error(f"Error uploading DataFrame to {file_path}: {str(ex)}")
            raise StorageError(f"Error uploading DataFrame to {file_path}: {str(ex)}")
    
    @async_retry((HttpResponseError, ResourceNotFoundError, ServiceRequestError), max_attempts=3)
    async def rename_file(
        self, 
        source_path: str, 
        destination_path: str,
        overwrite: bool = True
    ) -> bool:
        """
        Rename a file using metadata-only operation (much faster than copying).
        Works with both ADLS Gen2 and Blob Storage.
        
        Args:
            source_path: The source file path
            destination_path: The destination file path
            overwrite: Whether to overwrite an existing file
            
        Returns:
            bool: True if the rename was successful
            
        Raises:
            StorageError: If the file cannot be renamed
        """
        # Ensure the destination directory exists
        destination_dir = os.path.dirname(destination_path)
        self.ensure_directory_exists(destination_dir)
        
        try:
            if self.use_local_emulator:
                # Blob Storage approach
                source_blob_client = self.file_system_client.get_blob_client(source_path)
                destination_blob_client = self.file_system_client.get_blob_client(destination_path)
                
                # Check if the source blob exists
                try:
                    source_properties = source_blob_client.get_blob_properties()
                except ResourceNotFoundError:
                    logger.error(f"Source file {source_path} does not exist in blob storage")
                    raise StorageError(f"Source file {source_path} does not exist in blob storage")
                
                # Check if destination exists and handle overwrite
                try:
                    destination_properties = destination_blob_client.get_blob_properties()
                    if not overwrite:
                        logger.info(f"Destination file {destination_path} already exists in blob storage, not overwriting")
                        return False
                    # Delete existing blob if overwrite is True
                    destination_blob_client.delete_blob()
                    logger.info(f"Deleted existing destination blob {destination_path} for overwrite")
                except ResourceNotFoundError:
                    pass
                
                # For Blob Storage, we have to do a copy operation since direct rename is not available
                # Start the copy operation from source URL
                copy_source_url = source_blob_client.url
                
                # Add SAS token for local development if needed
                if '?' not in copy_source_url:
                    # For Azurite, we can use the account key directly for simplicity
                    copy_source_url += "?sig=devstoreaccount1key"
                
                destination_blob_client.start_copy_from_url(copy_source_url)
                logger.info(f"Started copy from {source_path} to {destination_path} in blob storage")
                
                # Delete the source blob after copy is complete
                source_blob_client.delete_blob()
                logger.info(f"Deleted source blob {source_path} after copying")
                
                return True
            else:
                # ADLS Gen2 approach with metadata-only rename
                source_file_client = self.file_system_client.get_file_client(source_path)
                destination_file_client = self.file_system_client.get_file_client(destination_path)
                
                # Check if the source file exists
                try:
                    source_properties = source_file_client.get_file_properties()
                except ResourceNotFoundError:
                    logger.error(f"Source file {source_path} does not exist")
                    raise StorageError(f"Source file {source_path} does not exist")
                
                # Check if the destination file exists and delete it if overwrite is True
                try:
                    dest_properties = destination_file_client.get_file_properties()
                    if not overwrite:
                        logger.info(f"Destination file {destination_path} already exists, not overwriting")
                        return False
                    # Delete the existing file if overwrite is True
                    logger.info(f"Destination file {destination_path} already exists, deleting for overwrite")
                    destination_file_client.delete_file()
                except ResourceNotFoundError:
                    pass
                
                # Now perform the rename operation
                logger.info(f"Renaming {source_path} to {destination_path}")
                # Get just the filename portion for the rename operation
                destination_filename = os.path.basename(destination_path)
                source_file_client.rename_file(f"{self.container_name}/{destination_path}")
                
                logger.info(f"Successfully renamed {source_path} to {destination_path}")
                return True
                
        except Exception as ex:
            if not isinstance(ex, StorageError):
                logger.error(f"Error renaming {source_path} to {destination_path}: {str(ex)}")
                raise StorageError(f"Error renaming {source_path} to {destination_path}: {str(ex)}")
            raise
    
    @async_retry((HttpResponseError, ResourceNotFoundError, ServiceRequestError), max_attempts=3)
    async def copy_file(
        self, 
        source_path: str, 
        destination_path: str,
        overwrite: bool = True
    ) -> bool:
        """
        Copy a file from one location to another.
        Works with both ADLS Gen2 and Blob Storage.
        
        Args:
            source_path: The source file path
            destination_path: The destination file path
            overwrite: Whether to overwrite an existing file
            
        Returns:
            bool: True if the copy was successful
            
        Raises:
            StorageError: If the file cannot be copied
        """
        # Ensure the destination directory exists
        destination_dir = os.path.dirname(destination_path)
        self.ensure_directory_exists(destination_dir)
        
        try:
            if self.use_local_emulator:
                # Blob Storage approach
                source_blob_client = self.file_system_client.get_blob_client(source_path)
                destination_blob_client = self.file_system_client.get_blob_client(destination_path)
                
                # Check if the source blob exists
                try:
                    source_properties = source_blob_client.get_blob_properties()
                except ResourceNotFoundError:
                    logger.error(f"Source file {source_path} does not exist in blob storage")
                    raise StorageError(f"Source file {source_path} does not exist in blob storage")
                
                # Check if destination exists and handle overwrite
                try:
                    destination_properties = destination_blob_client.get_blob_properties()
                    if not overwrite:
                        logger.info(f"Destination file {destination_path} already exists in blob storage, not overwriting")
                        return False
                    logger.info(f"Destination file {destination_path} already exists in blob storage, overwriting")
                except ResourceNotFoundError:
                    pass
                
                # For blob storage, start a copy operation
                copy_source_url = source_blob_client.url
                
                # Add SAS token for local development if needed
                if '?' not in copy_source_url:
                    # For Azurite, we can use the account key directly for simplicity
                    copy_source_url += "?sig=devstoreaccount1key"
                
                destination_blob_client.start_copy_from_url(copy_source_url)
                logger.info(f"Successfully copied {source_path} to {destination_path} in blob storage")
                
                return True
            else:
                # ADLS Gen2 approach
                source_file_client = self.file_system_client.get_file_client(source_path)
                destination_file_client = self.file_system_client.get_file_client(destination_path)
                
                # Check if the source file exists
                try:
                    source_properties = source_file_client.get_file_properties()
                except ResourceNotFoundError:
                    logger.error(f"Source file {source_path} does not exist")
                    raise StorageError(f"Source file {source_path} does not exist")
                
                # Check if the destination file exists
                try:
                    dest_properties = destination_file_client.get_file_properties()
                    if not overwrite:
                        logger.info(f"Destination file {destination_path} already exists, not overwriting")
                        return False
                    logger.info(f"Destination file {destination_path} already exists, overwriting")
                except ResourceNotFoundError:
                    pass
                
                # Get the source URL (for this case we use the same account, so no SAS needed)
                source_url = source_file_client.url
                
                # Start the copy operation
                logger.info(f"Copying {source_path} to {destination_path}")
                destination_file_client.start_copy_from_url(source_url)
                
                # Poll the copy operation status
                props = destination_file_client.get_file_properties()
                copy_status = props.copy.status
                
                # Wait for the copy to complete
                while copy_status == 'pending':
                    time.sleep(1)
                    props = destination_file_client.get_file_properties()
                    copy_status = props.copy.status
                
                if copy_status == 'success':
                    logger.info(f"Successfully copied {source_path} to {destination_path}")
                    return True
                else:
                    error_message = f"Copy operation failed with status: {copy_status}"
                    if hasattr(props.copy, 'error') and props.copy.error:
                        error_message += f", Error: {props.copy.error.message}"
                    logger.error(error_message)
                    raise StorageError(error_message)
                    
        except Exception as ex:
            if not isinstance(ex, StorageError):
                logger.error(f"Error copying {source_path} to {destination_path}: {str(ex)}")
                raise StorageError(f"Error copying {source_path} to {destination_path}: {str(ex)}")
            raise
    
    async def list_files(self, directory_path: str) -> List[str]:
        """
        List all files in a directory.
        Works with both ADLS Gen2 and Blob Storage.
        
        Args:
            directory_path: The directory path to list files from
            
        Returns:
            List[str]: List of file paths
            
        Raises:
            StorageError: If the files cannot be listed
        """
        try:
            if self.use_local_emulator:
                # Blob Storage approach
                if directory_path and not directory_path.endswith('/'):
                    directory_path = directory_path + '/'
                
                # List blobs with the specified prefix
                blobs = self.file_system_client.list_blobs(name_starts_with=directory_path)
                
                # Filter out placeholder blobs and directory markers
                paths = []
                for blob in blobs:
                    # Skip placeholder blobs and directory markers
                    if blob.name.endswith('.placeholder') or blob.name.endswith('/'):
                        continue
                    paths.append(blob.name)
                
                logger.info(f"Found {len(paths)} files in {directory_path} using blob storage")
                return paths
            else:
                # ADLS Gen2 approach
                # Create a directory client
                directory_client = self.file_system_client.get_directory_client(directory_path)
                
                try:
                    # Check if the directory exists
                    directory_properties = directory_client.get_directory_properties()
                except ResourceNotFoundError:
                    logger.warning(f"Directory {directory_path} does not exist")
                    return []
                
                # List all paths in the directory
                paths = []
                paths_iter = directory_client.get_paths(recursive=False)
                
                for path in paths_iter:
                    if not path.is_directory:
                        paths.append(f"{directory_path}/{path.name}")
                
                logger.info(f"Found {len(paths)} files in {directory_path}")
                return paths
                
        except Exception as ex:
            logger.error(f"Error listing files in {directory_path}: {str(ex)}")
            raise StorageError(f"Error listing files in {directory_path}: {str(ex)}")
    
    async def move_all_files(
        self, 
        source_directory: str, 
        destination_directory: str,
        overwrite: bool = True,
        delete_source_files: bool = False
    ) -> List[str]:
        """
        Move all files from source directory to destination directory.
        Uses rename (metadata-only) operations in ADLS Gen2 and copy+delete in Blob Storage.
        
        Args:
            source_directory: Source directory path
            destination_directory: Destination directory path
            overwrite: Whether to overwrite existing files
            delete_source_files: Whether to delete source files if move fails (normally not needed as rename removes source)
            
        Returns:
            List[str]: List of destination file paths
            
        Raises:
            StorageError: If files cannot be moved
        """
        # List all files in the source directory
        source_files = await self.list_files(source_directory)
        
        if not source_files:
            logger.info(f"No files found in {source_directory} to move")
            return []
        
        # Ensure destination directory exists
        self.ensure_directory_exists(destination_directory)
        
        # Move each file
        destination_files = []
        
        for source_file in source_files:
            file_name = os.path.basename(source_file)
            destination_file = f"{destination_directory}/{file_name}"
            
            try:
                if self.use_local_emulator:
                    # In blob storage, we use copy + delete
                    logger.info(f"Moving {source_file} to {destination_file} in blob storage")
                    success = await self.copy_file(source_file, destination_file, overwrite)
                    
                    if success:
                        # Delete the source blob
                        source_blob_client = self.file_system_client.get_blob_client(source_file)
                        source_blob_client.delete_blob()
                        
                        destination_files.append(destination_file)
                        logger.info(f"Successfully moved {source_file} to {destination_file} in blob storage")
                else:
                    # In ADLS Gen2, try to rename first (fastest)
                    success = await self.rename_file(source_file, destination_file, overwrite)
                    if success:
                        destination_files.append(destination_file)
                        # No need to delete source as rename removes it
                        logger.info(f"Successfully moved {source_file} to {destination_file} via rename")
                        continue
                    
                    # If rename fails for any reason, fall back to copy
                    logger.warning(f"Rename operation failed, falling back to copy for {source_file}")
                    success = await self.copy_file(source_file, destination_file, overwrite)
                    if success:
                        destination_files.append(destination_file)
                        
                        # Delete source file if requested
                        if delete_source_files:
                            source_file_client = self.file_system_client.get_file_client(source_file)
                            source_file_client.delete_file()
                            logger.info(f"Deleted source file {source_file}")
            except StorageError as ex:
                logger.error(f"Failed to move {source_file} to {destination_file}: {str(ex)}")
                # Continue with the next file
        
        logger.info(f"Successfully moved {len(destination_files)} files to {destination_directory}")
        return destination_files 
```
