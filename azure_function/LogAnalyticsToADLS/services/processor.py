"""Main processor service that coordinates the data extraction and transfer workflow."""
import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set, Tuple, Generator

import pandas as pd

from ..config import get_settings, QueryConfig
from ..utils.exceptions import (
    LogAnalyticsToADLSError
)
from .auth_service import AuthenticationService
from .log_analytics_service import LogAnalyticsService
from .storage_service import StorageService

# Logger for this module
logger = logging.getLogger(__name__)


class LogAnalyticsToADLSProcessor:
    """Main processor that coordinates the data extraction and transfer workflow."""
    
    def __init__(self):
        """Initialize the processor."""
        self.settings = get_settings()
        self.auth_service = AuthenticationService()
        self.log_analytics_service = LogAnalyticsService(self.auth_service)
        self.storage_service = StorageService(self.auth_service)
        
        # Initialize tracking
        self.uploaded_files: List[str] = []
        self.processing_id = str(uuid.uuid4())
        self.start_time = None
        self.end_time = None
    
    async def extract_and_upload_chunks(
        self,
        query_config: QueryConfig,
        chunk_size: int = 100000,
        row_process_size: int = 10000
    ) -> List[str]:
        """
        Extract data from Log Analytics in chunks and upload to staging directory.
        
        Args:
            query_config: Query configuration including query string, output pattern, etc.
            chunk_size: Chunk size for pagination from Log Analytics
            row_process_size: Number of rows to process at once when writing files
            
        Returns:
            List[str]: List of uploaded file paths
        """
        logger.info(f"Starting data extraction for query pattern '{query_config.output_pattern}' with processing ID: {self.processing_id}")
        
        # Get query details
        query = query_config.query
        days = query_config.days
        file_format = query_config.file_format
        output_pattern = query_config.output_pattern
        
        # Determine which workspaces to query
        workspace_ids = query_config.workspace_ids
        if workspace_ids is None:
            # Use all configured workspace IDs if none specified for this query
            workspace_ids = self.settings.get_all_workspace_ids()
            logger.info(f"Using all configured workspace IDs for query '{output_pattern}': {workspace_ids}")
        else:
            logger.info(f"Using specified workspace IDs for query '{output_pattern}': {workspace_ids}")
        
        # Make sure uploaded_files is initialized for this query
        query_files = []
        
        # Generate timestamp for file names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Get staging directory from settings
        staging_directory = self.settings.STAGING_DIRECTORY
        
        # Create a run-specific subdirectory to keep related files together
        run_directory = f"{staging_directory}/{self.processing_id}/{output_pattern}"
        
        # Query Log Analytics in chunks
        chunk_index = 0
        total_rows = 0
        
        async for chunk_df in self.log_analytics_service.query_azure_activity(
            query_override=query,
            days=days,
            chunk_size=chunk_size,
            workspace_ids=workspace_ids
        ):
            if chunk_df.empty:
                logger.info(f"Received empty chunk for '{output_pattern}', skipping")
                continue
                
            # Generate unique file name for this chunk
            if file_format.lower() == "csv":
                file_name = f"{output_pattern}_{timestamp}_chunk{chunk_index:03d}.csv.gz"
            else:
                file_name = f"{output_pattern}_{timestamp}_chunk{chunk_index:03d}.parquet"
            
            # Upload the chunk to staging with compression
            file_path = None
            if file_format.lower() == "csv":
                file_path = await self.storage_service.upload_dataframe_to_csv_gz(
                    chunk_df, 
                    run_directory, 
                    file_name,
                    chunksize=row_process_size
                )
            else:
                file_path = await self.storage_service.upload_dataframe_to_parquet_gz(
                    chunk_df, 
                    run_directory, 
                    file_name,
                    compression='gzip',
                    row_group_size=row_process_size
                )
                
            if file_path:
                query_files.append(file_path)
                self.uploaded_files.append(file_path)
                
            total_rows += len(chunk_df)
            chunk_index += 1
            
            logger.info(f"Uploaded chunk {chunk_index} for '{output_pattern}', total rows so far: {total_rows}")
            
            # Force garbage collection to free memory
            chunk_df = None
            import gc
            gc.collect()
        
        logger.info(f"Completed data extraction for '{output_pattern}', uploaded {len(query_files)} files with {total_rows} total rows")
        return query_files
    
    async def move_to_final_directory(self, delete_source: bool = False) -> List[str]:
        """
        Move all files from staging directory to final directory using fast metadata-only operations.
        
        Args:
            delete_source: Whether to delete source files if move fails (default: False)
            
        Returns:
            List[str]: List of destination file paths
        """
        if not self.uploaded_files:
            logger.warning("No files to move, extraction may have failed or returned no data")
            return []
        
        # Group files by their subdirectories
        file_groups = {}
        for file_path in self.uploaded_files:
            # Extract the part after the processing_id including the output_pattern
            parts = file_path.split(f"{self.processing_id}/")
            if len(parts) > 1:
                output_pattern_path = parts[1].split("/")[0]  # Get the output pattern directory
                directory = os.path.dirname(file_path)
                if directory not in file_groups:
                    file_groups[directory] = []
                file_groups[directory].append(file_path)
        
        all_destination_files = []
        
        # Move files from each source directory to the corresponding final directory
        for source_directory, files in file_groups.items():
            # Extract the relative path under the processing_id directory
            relative_path = source_directory.split(f"{self.processing_id}/")[1] if f"{self.processing_id}/" in source_directory else ""
            final_directory = f"{self.settings.FINAL_DIRECTORY}/{self.processing_id}/{relative_path}"
            
            logger.info(f"Moving files from {source_directory} to {final_directory} (using metadata-only operations)")
            
            # Move all files using efficient rename operations
            start_time = time.time()
            destination_files = await self.storage_service.move_all_files(
                source_directory,
                final_directory,
                overwrite=True,
                delete_source_files=delete_source
            )
            duration = time.time() - start_time
            
            all_destination_files.extend(destination_files)
            logger.info(f"Moved {len(destination_files)} files to final directory in {duration:.2f} seconds")
        
        return all_destination_files
    
    async def process_multiple_queries(
        self,
        chunk_size: int = 100000,
        row_process_size: int = 10000,
        delete_staging_files: bool = False
    ) -> Dict[str, Any]:
        """
        Process multiple queries as defined in settings.
        
        Args:
            chunk_size: Chunk size for pagination from Log Analytics
            row_process_size: Number of rows to process at once when writing files
            delete_staging_files: Whether to delete staging files after moving
            
        Returns:
            Dict[str, Any]: Results of the processing
        """
        logger.info(f"Starting multiple query processing with ID {self.processing_id}")
        self.start_time = datetime.now()
        self.uploaded_files = []
        
        # Get query configurations from settings
        query_configs = self.settings.get_queries()
        if not query_configs:
            logger.warning("No queries configured, nothing to process")
            return {
                "status": "completed_with_warnings",
                "message": "No queries configured, nothing to process",
                "processing_id": self.processing_id,
                "start_time": self.start_time,
                "end_time": datetime.now(),
                "duration_seconds": (datetime.now() - self.start_time).total_seconds()
            }
        
        # Process each query
        query_results = {}
        for query_config in query_configs:
            try:
                logger.info(f"Processing query for pattern '{query_config.output_pattern}'")
                workspace_str = ", ".join(query_config.workspace_ids) if query_config.workspace_ids else "all configured workspaces"
                logger.info(f"Query will run on workspaces: {workspace_str}")
                
                files = await self.extract_and_upload_chunks(
                    query_config,
                    chunk_size=chunk_size,
                    row_process_size=row_process_size
                )
                query_results[query_config.output_pattern] = {
                    "status": "success" if files else "no_data",
                    "files_count": len(files),
                    "query": query_config.query,
                    "days": query_config.days,
                    "file_format": query_config.file_format,
                    "workspace_ids": query_config.workspace_ids or self.settings.get_all_workspace_ids()
                }
            except Exception as e:
                logger.error(f"Error processing query '{query_config.output_pattern}': {str(e)}")
                query_results[query_config.output_pattern] = {
                    "status": "error",
                    "error": str(e),
                    "query": query_config.query,
                    "workspace_ids": query_config.workspace_ids or self.settings.get_all_workspace_ids()
                }
        
        # Move all files to final directory
        final_files = []
        if self.uploaded_files:
            final_files = await self.move_to_final_directory(delete_source=delete_staging_files)
        
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        
        # Determine overall status
        statuses = [result.get("status") for result in query_results.values()]
        overall_status = "completed"
        if "error" in statuses:
            overall_status = "completed_with_errors"
        elif all(status == "no_data" for status in statuses):
            overall_status = "completed_no_data"
        
        return {
            "status": overall_status,
            "message": f"Processed {len(query_configs)} queries",
            "query_results": query_results,
            "staging_files_count": len(self.uploaded_files),
            "final_files_count": len(final_files),
            "processing_id": self.processing_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": duration
        }
    
    async def run(
        self,
        query: Optional[str] = None,
        days: int = 1,
        chunk_size: int = 100000,
        file_format: str = "parquet",
        delete_staging_files: bool = False,
        row_process_size: int = 10000,
        workspace_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Run the full extraction and transfer process for a single query.
        (Legacy method for backwards compatibility)
        
        Args:
            query: Query override (default: use settings)
            days: Days to look back (default: 1)
            chunk_size: Chunk size for pagination from Log Analytics (default: 100000)
            file_format: File format ("csv" or "parquet", default: "parquet")
            delete_staging_files: Whether to delete staging files after copying (default: False)
            row_process_size: Number of rows to process at once when writing files (default: 10000)
            workspace_ids: Optional list of workspace IDs to query (default: use all configured workspaces)
            
        Returns:
            Dict[str, Any]: Process results and statistics
        """
        logger.info("Starting Log Analytics to ADLS processing for single query")
        
        try:
            self.start_time = datetime.now()
            self.uploaded_files = []
            
            # Create a query config object
            query_config = QueryConfig(
                query=query or "AzureActivity | where TimeGenerated >= ago(1d)",
                output_pattern="azure_activity",
                days=days,
                file_format=file_format,
                workspace_ids=workspace_ids
            )
            
            # Step 1: Extract and upload to staging
            staging_files = await self.extract_and_upload_chunks(
                query_config=query_config,
                chunk_size=chunk_size,
                row_process_size=row_process_size
            )
            
            if not staging_files:
                logger.warning("No data was extracted from Log Analytics")
                return {
                    "status": "completed_with_warnings",
                    "message": "No data was extracted from Log Analytics",
                    "staging_files": [],
                    "final_files": [],
                    "processing_id": self.processing_id,
                    "start_time": self.start_time,
                    "end_time": datetime.now(),
                    "duration_seconds": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
                }
            
            # Step 2: Move to final directory using metadata-only operations
            final_files = await self.move_to_final_directory(delete_source=delete_staging_files)
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds() if self.start_time else 0
            
            return {
                "status": "completed",
                "message": "Successfully extracted and transferred data",
                "staging_files": staging_files,
                "final_files": final_files,
                "processing_id": self.processing_id,
                "start_time": self.start_time,
                "end_time": self.end_time,
                "duration_seconds": duration,
                "workspace_ids": self.settings.get_all_workspace_ids()
            }
        except LogAnalyticsToADLSError as ex:
            logger.error(f"Error during processing: {str(ex)}")
            return {
                "status": "failed",
                "error": str(ex),
                "processing_id": self.processing_id,
                "start_time": self.start_time,
                "end_time": datetime.now(),
                "duration_seconds": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            }
        except Exception as ex:
            logger.error(f"Unexpected error during processing: {str(ex)}")
            return {
                "status": "failed",
                "error": f"Unexpected error: {str(ex)}",
                "processing_id": self.processing_id,
                "start_time": self.start_time,
                "end_time": datetime.now(),
                "duration_seconds": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
            } 