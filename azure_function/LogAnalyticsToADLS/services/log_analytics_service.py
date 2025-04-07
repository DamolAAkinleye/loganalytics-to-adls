"""Service for querying data from Log Analytics."""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Generator, AsyncGenerator

import pandas as pd
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.monitor.query.models import LogsQueryResult, LogsBatchQuery, LogsQueryRequest

from ..config import get_settings
from ..utils.retry_utils import async_retry
from ..utils.exceptions import (
    LogAnalyticsQueryError,
    AuthenticationError,
    handle_exceptions
)
from .auth_service import AuthenticationService

# Logger for this module
logger = logging.getLogger(__name__)


class LogAnalyticsService:
    """Service for querying data from Log Analytics."""
    
    def __init__(self, auth_service: AuthenticationService):
        """
        Initialize the Log Analytics service.
        
        Args:
            auth_service: The authentication service to use for retrieving credentials
        """
        self.settings = get_settings()
        self.auth_service = auth_service
        self.default_workspace_id = self.settings.LOG_ANALYTICS_WORKSPACE_ID
        self._logs_client = None
    
    async def _get_logs_client(self) -> LogsQueryClient:
        """
        Get or create the Log Analytics client using SPN credentials.
        
        Returns:
            LogsQueryClient: The Log Analytics client
        """
        if self._logs_client is None:
            try:
                spn_credential = await self.auth_service.get_spn_credential()
                self._logs_client = LogsQueryClient(credential=spn_credential)
                logger.info("Created Log Analytics client")
            except AuthenticationError as ex:
                logger.error(f"Failed to create Log Analytics client: {str(ex)}")
                raise
            except Exception as ex:
                logger.error(f"Unexpected error creating Log Analytics client: {str(ex)}")
                raise LogAnalyticsQueryError(f"Failed to create Log Analytics client: {str(ex)}")
                
        return self._logs_client
    
    @async_retry((LogAnalyticsQueryError, ConnectionError, TimeoutError), max_attempts=3)
    async def query_logs(
        self, 
        query: str, 
        timespan: Optional[timedelta] = None,
        workspace_id: Optional[str] = None
    ) -> LogsQueryResult:
        """
        Query Log Analytics.
        
        Args:
            query: The KQL query to run
            timespan: Optional timespan for the query (default: 1 day)
            workspace_id: Optional workspace ID to query (default: use default workspace ID)
            
        Returns:
            LogsQueryResult: The query result
            
        Raises:
            LogAnalyticsQueryError: If the query fails
        """
        logs_client = await self._get_logs_client()
        timespan = timespan or timedelta(days=1)
        workspace_id = workspace_id or self.default_workspace_id
        
        try:
            logger.info(f"Querying Log Analytics workspace {workspace_id}: {query}")
            start_time = time.time()
            
            response = logs_client.query_workspace(
                workspace_id=workspace_id,
                query=query,
                timespan=timespan
            )
            
            duration = time.time() - start_time
            logger.info(f"Query to workspace {workspace_id} completed in {duration:.2f} seconds")
            
            if response.status == LogsQueryStatus.PARTIAL:
                logger.warning(f"Query to workspace {workspace_id} returned partial results")
            elif response.status == LogsQueryStatus.FAILURE:
                error_msg = f"Query to workspace {workspace_id} failed"
                if hasattr(response, 'error') and response.error:
                    error_msg = f"Query to workspace {workspace_id} failed: {response.error.message}"
                raise LogAnalyticsQueryError(error_msg)
            
            return response
        except Exception as ex:
            logger.error(f"Error querying Log Analytics workspace {workspace_id}: {str(ex)}")
            raise LogAnalyticsQueryError(f"Error querying Log Analytics workspace {workspace_id}: {str(ex)}")
    
    def _result_to_dataframe(self, response: LogsQueryResult, workspace_id: str) -> pd.DataFrame:
        """
        Convert a LogsQueryResult to a Pandas DataFrame.
        
        Args:
            response: The query result from Log Analytics
            workspace_id: The workspace ID that was queried (added to DataFrame)
            
        Returns:
            pd.DataFrame: The result as a DataFrame with workspace_id column
        """
        if not response.tables:
            logger.warning(f"Query to workspace {workspace_id} returned no tables")
            return pd.DataFrame()
            
        table = response.tables[0]
        
        # Create a DataFrame from the table
        df = pd.DataFrame(table.rows, columns=[col.name for col in table.columns])
        
        # Add workspace_id column to identify the source
        df['WorkspaceId'] = workspace_id
        
        logger.info(f"Converted {len(df)} rows from workspace {workspace_id} to DataFrame")
        
        return df
    
    async def query_logs_to_dataframe(
        self, 
        query: str, 
        timespan: Optional[timedelta] = None,
        workspace_id: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Query Log Analytics and convert the result to a Pandas DataFrame.
        
        Args:
            query: The KQL query to run
            timespan: Optional timespan for the query (default: 1 day)
            workspace_id: Optional workspace ID to query (default: use default workspace ID)
            
        Returns:
            pd.DataFrame: The query result as a DataFrame
            
        Raises:
            LogAnalyticsQueryError: If the query fails
        """
        workspace_id = workspace_id or self.default_workspace_id
        response = await self.query_logs(query, timespan, workspace_id)
        return self._result_to_dataframe(response, workspace_id)
    
    async def query_with_pagination(
        self,
        query: str,
        days: int = 1,
        chunk_size: Optional[int] = None,
        workspace_id: Optional[str] = None
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """
        Query Log Analytics with pagination and yield results in chunks.
        
        Args:
            query: The KQL query to run
            days: Number of days to look back (default: 1)
            chunk_size: Optional chunk size for pagination (default: None, all data at once)
            workspace_id: Optional workspace ID to query (default: use default workspace ID)
            
        Yields:
            pd.DataFrame: The query result as a DataFrame, possibly in chunks
            
        Raises:
            LogAnalyticsQueryError: If the query fails
        """
        workspace_id = workspace_id or self.default_workspace_id
        timespan = timedelta(days=days)
        
        # If no chunk size, return all at once
        if not chunk_size:
            df = await self.query_logs_to_dataframe(query, timespan, workspace_id)
            yield df
            return
            
        # With chunk size, paginate the results
        base_query = query
        if "order by" not in base_query.lower():
            base_query += " | order by TimeGenerated asc"
            
        if "limit" in base_query.lower() or "top" in base_query.lower():
            logger.warning(f"Removing existing LIMIT/TOP from query for pagination in workspace {workspace_id}")
            # Simple way to remove TOP/LIMIT - in a real implementation, use a more robust approach
            base_query = base_query.split(" | top ")[0].split(" | limit ")[0]
        
        offset = 0
        total_rows = 0
        
        while True:
            paginated_query = f"{base_query} | offset {offset} | limit {chunk_size}"
            
            try:
                # Get the response directly
                response = await self.query_logs(paginated_query, timespan, workspace_id)
                
                # Convert to DataFrame
                chunk_df = self._result_to_dataframe(response, workspace_id)
                
                if chunk_df.empty:
                    break
                    
                total_rows += len(chunk_df)
                offset += chunk_size
                
                logger.info(f"Retrieved chunk of {len(chunk_df)} rows from workspace {workspace_id}, total: {total_rows}")
                
                # Yield the chunk and clear references to free memory
                yield chunk_df
                
                # Clear references to help with garbage collection
                del response
                del chunk_df
                import gc
                gc.collect()
                
                if len(chunk_df) < chunk_size:
                    break
            except LogAnalyticsQueryError as ex:
                logger.error(f"Error querying chunk at offset {offset} from workspace {workspace_id}: {str(ex)}")
                # If it's a timeout, reduce the chunk size and try again
                if "timeout" in str(ex).lower() and chunk_size > 1000:
                    new_chunk_size = max(1000, chunk_size // 2)
                    logger.info(f"Reducing chunk size from {chunk_size} to {new_chunk_size} due to timeout in workspace {workspace_id}")
                    chunk_size = new_chunk_size
                    continue
                raise
    
    async def query_azure_activity(
        self, 
        query_override: Optional[str] = None, 
        days: int = 1,
        chunk_size: Optional[int] = None,
        workspace_ids: Optional[List[str]] = None
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """
        Query the AzureActivity table from one or more workspaces and yield results in chunks.
        
        Args:
            query_override: Optional query override
            days: Number of days to look back (default: 1)
            chunk_size: Optional chunk size for pagination (default: None, all data at once)
            workspace_ids: Optional list of workspace IDs to query (default: use default workspace ID)
            
        Yields:
            pd.DataFrame: The query result as a DataFrame, possibly in chunks
            
        Raises:
            LogAnalyticsQueryError: If the query fails
        """
        query = query_override or self.settings.get_queries()[0].query
        
        # If workspace_ids is None or empty, use the default workspace ID
        if not workspace_ids:
            workspace_ids = [self.default_workspace_id]
            
        # Query each workspace and yield results
        for workspace_id in workspace_ids:
            logger.info(f"Querying workspace: {workspace_id}")
            async for df_chunk in self.query_with_pagination(
                query=query,
                days=days,
                chunk_size=chunk_size,
                workspace_id=workspace_id
            ):
                yield df_chunk
    
    async def query_multiple_workspaces(
        self, 
        query: str,
        days: int = 1,
        chunk_size: Optional[int] = None,
        workspace_ids: List[str] = None
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """
        Query multiple Log Analytics workspaces with the same query.
        
        Args:
            query: The KQL query to run
            days: Number of days to look back (default: 1)
            chunk_size: Optional chunk size for pagination (default: None, all data at once)
            workspace_ids: List of workspace IDs to query (default: all configured workspaces)
            
        Yields:
            pd.DataFrame: The query result as a DataFrame, with rows from all workspaces
            
        Raises:
            LogAnalyticsQueryError: If the query fails for all workspaces
        """
        # If workspace_ids is None, use all configured workspace IDs
        if workspace_ids is None:
            workspace_ids = self.settings.get_all_workspace_ids()
            
        if not workspace_ids:
            logger.warning("No workspace IDs provided, using default workspace ID")
            workspace_ids = [self.default_workspace_id]
            
        # Track errors to report if all workspaces fail
        errors = []
        total_results = 0
        
        # Query each workspace and yield results
        for workspace_id in workspace_ids:
            try:
                logger.info(f"Querying workspace: {workspace_id}")
                async for df_chunk in self.query_with_pagination(
                    query=query,
                    days=days,
                    chunk_size=chunk_size,
                    workspace_id=workspace_id
                ):
                    total_results += len(df_chunk)
                    yield df_chunk
            except LogAnalyticsQueryError as ex:
                logger.error(f"Error querying workspace {workspace_id}: {str(ex)}")
                errors.append(f"Workspace {workspace_id}: {str(ex)}")
        
        # If we didn't get any results and all workspaces failed, raise an error
        if total_results == 0 and len(errors) == len(workspace_ids):
            raise LogAnalyticsQueryError(f"All workspaces failed: {'; '.join(errors)}") 