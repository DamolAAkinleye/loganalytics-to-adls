# multi_workspace_example.py

```python
"""
Example script to demonstrate querying multiple Log Analytics workspaces.

This script shows how to:
1. Query multiple Log Analytics workspaces with the same query
2. Process the results with workspace information preserved
3. Upload the combined data to Azure Data Lake Storage

Usage:
    python multi_workspace_example.py

Requirements:
    - Azure credentials configured (managed identity or environment variables)
    - Log Analytics workspace IDs to query
"""
import asyncio
import os
import pandas as pd
from datetime import timedelta

from LogAnalyticsToADLS.services.auth_service import AuthenticationService
from LogAnalyticsToADLS.services.log_analytics_service import LogAnalyticsService
from LogAnalyticsToADLS.services.storage_service import StorageService


async def query_multiple_workspaces_example():
    """Query multiple Log Analytics workspaces and display results."""
    # Initialize services
    auth_service = AuthenticationService()
    log_analytics_service = LogAnalyticsService(auth_service)
    storage_service = StorageService(auth_service)
    
    # List of workspace IDs to query - replace with your actual workspace IDs
    workspace_ids = [
        "your-workspace-id-1",
        "your-workspace-id-2",
        "your-workspace-id-3"
    ]
    
    # Query to run against all workspaces
    query = """
    AzureActivity
    | where TimeGenerated >= ago(1d)
    | project TimeGenerated, ResourceProvider, OperationName, Caller, Level, ActivityStatus
    | limit 100
    """
    
    print(f"Querying {len(workspace_ids)} workspaces...")
    
    # Create a list to hold all dataframes from all workspaces
    all_results = []
    total_rows = 0
    
    # Query each workspace using the async generator
    async for df_chunk in log_analytics_service.query_multiple_workspaces(
        query=query,
        days=1,
        chunk_size=10000,
        workspace_ids=workspace_ids
    ):
        # Print a preview of each chunk with workspace ID
        print(f"\nReceived chunk with {len(df_chunk)} rows")
        print("Workspace distribution:")
        print(df_chunk['WorkspaceId'].value_counts())
        
        if not df_chunk.empty:
            # Display a sample of the data
            print("\nSample data:")
            print(df_chunk.head(3))
            
            # Add to results
            all_results.append(df_chunk)
            total_rows += len(df_chunk)
    
    # Combine all results if we have any
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        print(f"\nTotal rows from all workspaces: {total_rows}")
        
        # Show distribution by workspace
        print("\nData distribution by workspace:")
        workspace_counts = combined_df['WorkspaceId'].value_counts()
        for workspace_id, count in workspace_counts.items():
            print(f"  {workspace_id}: {count} rows ({count/total_rows*100:.1f}%)")
        
        # Save the combined data to a file
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"multi_workspace_example_{timestamp}.csv"
        combined_df.to_csv(file_name, index=False)
        print(f"\nSaved combined data to {file_name}")
        
        # Optionally upload to Azure Data Lake
        upload = input("\nUpload to Azure Data Lake? (y/n): ").strip().lower()
        if upload == 'y':
            # Ensure the staging directory exists
            staging_dir = "multi_workspace_examples"
            storage_service.ensure_directory_exists(staging_dir)
            
            # Upload the CSV with gzip compression
            file_path = await storage_service.upload_dataframe_to_csv_gz(
                combined_df,
                staging_dir,
                file_name + ".gz"
            )
            
            if file_path:
                print(f"Uploaded data to {file_path}")
            else:
                print("Upload failed")
    else:
        print("No data returned from any workspace")


if __name__ == "__main__":
    asyncio.run(query_multiple_workspaces_example()) 
```
