import asyncio
import json
import logging
import os
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, Union, Tuple, List

import azure.functions as func

from LogAnalyticsToADLS import LogAnalyticsToADLSProcessor
from LogAnalyticsToADLS.config import get_settings, QueryConfig
from LogAnalyticsToADLS.utils.exceptions import LogAnalyticsToADLSError

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("azure.functions")


async def process_queries(
    query_pattern: Optional[str] = None,
    workspace_ids: Optional[List[str]] = None
) -> Tuple[Dict[str, Any], bool]:
    """
    Shared processing function that can be called by both timer and HTTP triggers.
    
    Args:
        query_pattern: Optional pattern to run a specific query (None means run all)
        workspace_ids: Optional list of workspace IDs to query (None means use all configured workspaces)
    
    Returns:
        Tuple[Dict[str, Any], bool]: (Result dictionary, success flag)
    """
    try:
        # Get settings
        settings = get_settings()
        
        # Initialize processor
        processor = LogAnalyticsToADLSProcessor()
        
        # If a specific query pattern is requested, find it and run just that query
        if query_pattern:
            found = False
            for query_config in settings.get_queries():
                if query_config.output_pattern == query_pattern:
                    # Run single query
                    logger.info(f"Running single query for pattern: {query_pattern}")
                    # If workspace_ids is provided, use it; otherwise, use the ones in the query_config
                    query_workspace_ids = workspace_ids or query_config.workspace_ids
                    logger.info(f"Using workspace IDs: {query_workspace_ids or 'all configured workspaces'}")
                    
                    result = await processor.run(
                        query=query_config.query,
                        days=query_config.days,
                        chunk_size=settings.CHUNK_SIZE,
                        file_format=query_config.file_format,
                        delete_staging_files=settings.DELETE_STAGING_FILES,
                        row_process_size=settings.ROW_PROCESS_SIZE,
                        workspace_ids=query_workspace_ids
                    )
                    found = True
                    break
            
            if not found:
                return {
                    "status": "failed",
                    "error": f"Query pattern '{query_pattern}' not found in configuration",
                    "timestamp": datetime.now().isoformat()
                }, False
        else:
            # Run all queries with the workspace_ids applied to each query that doesn't specify its own
            logger.info("Running all configured queries")
            if workspace_ids:
                logger.info(f"Using workspace IDs for queries without specific workspaces: {workspace_ids}")
                # Create a new list of query configs with workspace_ids overridden when not specified
                query_configs = []
                for query_config in settings.get_queries():
                    if query_config.workspace_ids is None:
                        # Only override if the query doesn't specify its own workspace_ids
                        query_config = QueryConfig(
                            query=query_config.query,
                            output_pattern=query_config.output_pattern,
                            days=query_config.days,
                            file_format=query_config.file_format,
                            workspace_ids=workspace_ids
                        )
                    query_configs.append(query_config)
                
                # Override settings to use our modified query configs
                # Note: This is a bit of a hack, but it avoids modifying the processor class
                original_get_queries = settings.get_queries
                settings.get_queries = lambda: query_configs
                
                try:
                    result = await processor.process_multiple_queries(
                        chunk_size=settings.CHUNK_SIZE,
                        row_process_size=settings.ROW_PROCESS_SIZE,
                        delete_staging_files=settings.DELETE_STAGING_FILES
                    )
                finally:
                    # Restore original method
                    settings.get_queries = original_get_queries
            else:
                # Run normally with configured workspace IDs
                result = await processor.process_multiple_queries(
                    chunk_size=settings.CHUNK_SIZE,
                    row_process_size=settings.ROW_PROCESS_SIZE,
                    delete_staging_files=settings.DELETE_STAGING_FILES
                )
        
        # Format timestamps for logging/JSON
        if result.get("start_time"):
            result["start_time"] = result["start_time"].isoformat()
        if result.get("end_time"):
            result["end_time"] = result["end_time"].isoformat()
        
        # Clear memory
        processor = None
        import gc
        gc.collect()
        
        # Determine if successful
        success = result.get("status") != "failed"
        
        return result, success
        
    except LogAnalyticsToADLSError as ex:
        error_response = {
            "status": "failed",
            "error": str(ex),
            "timestamp": datetime.now().isoformat()
        }
        logger.error(f"LogAnalyticsToADLSError: {str(ex)}")
        return error_response, False
        
    except Exception as ex:
        error_response = {
            "status": "failed",
            "error": f"Unexpected error: {str(ex)}",
            "timestamp": datetime.now().isoformat()
        }
        logger.error(f"Unexpected error: {str(ex)}\n{traceback.format_exc()}")
        return error_response, False


async def main(mytimer: func.TimerRequest) -> None:
    """
    Timer trigger function to extract data from Log Analytics and move to ADLS Gen2.
    This function processes multiple queries as configured in settings.
    
    Parameters:
        - mytimer: Timer request object
    """
    logger.info("Log Analytics to ADLS Timer trigger function executed")
    
    # Skip if triggered by past due timer
    if mytimer.past_due:
        logger.info("Timer is past due, skipping execution")
        return
    
    # Use the shared processing function to run all queries
    result, success = await process_queries()
    
    # Log the result
    logger.info(f"Function completed with status: {result['status']}")
    logger.info(f"Processed {len(result.get('query_results', {}))} queries")
    logger.info(f"Total duration: {result.get('duration_seconds', 0):.2f} seconds")
    
    # Log detailed results if there were errors
    if not success:
        logger.error(f"Function completed with errors: {json.dumps(result, indent=2)}")


# Create the Azure Function with timer trigger
timer_trigger = func.TimerTrigger(
    name="TimerTrigger",
    schedule=get_settings().TIMER_SCHEDULE,
    run_on_startup=False,
    use_monitor=True
)

# Bind the function
log_analytics_to_adls = func.Blueprint()
log_analytics_to_adls.add_function(
    "log_analytics_to_adls_timer",
    main,
    trigger=timer_trigger
)


# Optional: Keep the HTTP trigger for manual execution and testing
async def manual_trigger(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger function to manually run the Log Analytics to ADLS extraction.
    This can be used for testing or manual execution.
    
    Parameters:
        - query_pattern: (optional) Only run the query with this output pattern
        - workspace_ids: (optional) List of workspace IDs to query, overriding defaults
    """
    logger.info("Manual HTTP trigger for Log Analytics to ADLS processing")
    
    # Get request parameters
    req_body = req.get_json() if req.get_body() else {}
    
    # If a specific query pattern is provided, run just that query
    query_pattern = req_body.get("query_pattern") or req.params.get("query_pattern")
    
    # Get workspace IDs if provided (as JSON array string or comma-separated values)
    workspace_ids_param = req_body.get("workspace_ids") or req.params.get("workspace_ids")
    workspace_ids = None
    
    if workspace_ids_param:
        try:
            # Try to parse as JSON
            workspace_ids = json.loads(workspace_ids_param) if isinstance(workspace_ids_param, str) else workspace_ids_param
            if not isinstance(workspace_ids, list):
                workspace_ids = [str(workspace_ids)]
        except json.JSONDecodeError:
            # Treat as comma-separated string
            workspace_ids = [ws_id.strip() for ws_id in workspace_ids_param.split(",") if ws_id.strip()]
    
    # Use the shared processing function
    result, success = await process_queries(query_pattern, workspace_ids)
    
    # Return results in HTTP response
    return func.HttpResponse(
        json.dumps(result, indent=2),
        mimetype="application/json",
        status_code=200 if success else 500
    )


# Add HTTP trigger (optional, for manual execution)
http_trigger = func.HttpTrigger(
    name="HTTPTrigger",
    methods=["GET", "POST"],
    authLevel=func.AuthLevel.FUNCTION,
    route="log-analytics-to-adls"
)

# Bind the HTTP function
log_analytics_to_adls.add_function(
    "log_analytics_to_adls_manual",
    manual_trigger,
    trigger=http_trigger
) 