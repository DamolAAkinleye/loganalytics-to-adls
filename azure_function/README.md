# Log Analytics to ADLS Azure Function

This Azure Function extracts data from the AzureActivity table in multiple Log Analytics workspaces and moves the data to Azure Data Lake Storage Gen2 (ADLS Gen2) using efficient metadata-only operations for file transfers.

## Features

- **Authentication**: Uses Service Principal (SPN) credentials stored in Azure Key Vault for secure access to Log Analytics
- **Managed Identity**: Uses the Function App's managed identity to access Azure Key Vault and ADLS Gen2
- **Robust Error Handling**: Comprehensive error handling and retry logic using async retry package
- **Memory Optimization**: Chunk-based processing with streaming operations to minimize memory usage
- **Async Implementation**: Fully asynchronous to maximize throughput and performance
- **Multiple Workspace Support**: Query one or more Log Analytics workspaces with the same query
- **Configurable Queries**: Run multiple queries with different configurations
- **Metadata Storage**: Stores workspace source information in the data
- **Data Compression**: Automatically compresses all data files with gzip for both CSV and Parquet formats
- **Ultrafast file transfers**: Uses metadata-only rename operations for 10-100x faster file movement
- **Local Development Support**: Automatically detects local environment and uses Azurite blob emulator instead of ADLS Gen2

## Project Structure

```
azure_function/
  ├── host.json               # Azure Functions host configuration
  ├── local.settings.json     # Local settings (not checked in to source control)
  ├── requirements.txt        # Python dependencies
  ├── function_app.py         # Azure Function entry point
  ├── LogAnalyticsToADLS/     # Main package
  │   ├── __init__.py
  │   ├── config/             # Configuration management
  │   │   ├── __init__.py
  │   │   └── settings.py     # Application settings
  │   ├── services/           # Core services
  │   │   ├── __init__.py
  │   │   ├── auth_service.py        # Authentication service
  │   │   ├── log_analytics_service.py # Log Analytics service
  │   │   ├── processor.py            # Main processor
  │   │   └── storage_service.py      # Storage service for moving files
  │   └── utils/              # Utilities
  │       ├── __init__.py
  │       ├── exceptions.py   # Custom exceptions
  │       └── retry_utils.py  # Retry utilities
```

## Services

- **Authentication Service**: Manages authentication with Azure services
- **Log Analytics Service**: Queries data from multiple Log Analytics workspaces
- **Storage Service**: Manages file operations in ADLS Gen2
- **Processor**: Coordinates the data extraction and transfer workflow

## Requirements

- Python 3.8 or higher
- Azure Subscription with the following resources:
  - Azure Log Analytics workspace(s)
  - Azure Key Vault
  - Azure Storage Account (with Data Lake Storage Gen2 enabled)
  - Azure Function App with Managed Identity enabled
- Service Principal with permissions to read from the Log Analytics workspace(s)

## Setup

### 1. Clone the repository

```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Key Vault

Store the Service Principal credentials in Azure Key Vault:

- Create a secret for the SPN Client ID (e.g., `loganalytics-spn-client-id`)
- Create a secret for the SPN Client Secret (e.g., `loganalytics-spn-client-secret`)
- Create a secret for the SPN Tenant ID (e.g., `loganalytics-spn-tenant-id`)

### 4. Configure Azure Function App

1. Enable Managed Identity on your Azure Function App
2. Grant the Managed Identity the following permissions:
   - Key Vault: Secret Reader
   - Storage Account: Storage Blob Data Contributor

### 5. Configure local settings

Update the `local.settings.json` file with your environment-specific settings:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "KEYVAULT_NAME": "your-keyvault-name",
    "LOG_ANALYTICS_WORKSPACE_ID": "your-default-workspace-id",
    "LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS": "[\"additional-workspace-id-1\", \"additional-workspace-id-2\"]",
    "SPN_CLIENT_ID_SECRET_NAME": "loganalytics-spn-client-id",
    "SPN_CLIENT_SECRET_SECRET_NAME": "loganalytics-spn-client-secret",
    "SPN_TENANT_ID_SECRET_NAME": "loganalytics-spn-tenant-id",
    "STORAGE_ACCOUNT_NAME": "yourstorageaccount",
    "STORAGE_CONTAINER_NAME": "loganalytics-data",
    "STAGING_DIRECTORY": "staging",
    "FINAL_DIRECTORY": "processed",
    "USE_SPN_FOR_STORAGE": "false",
    "USE_ENV_VARS_FOR_SECRETS": "true",
    "QUERIES_CONFIG": "[{\"query\": \"AzureActivity | where TimeGenerated >= ago(1d)\", \"output_pattern\": \"azure_activity\", \"days\": 1, \"file_format\": \"parquet\", \"workspace_ids\": [\"specific-workspace-id-1\", \"specific-workspace-id-2\"]}, {\"query\": \"SecurityEvent | where TimeGenerated >= ago(1d)\", \"output_pattern\": \"security_events\", \"days\": 1, \"file_format\": \"parquet\"}]",
    "MAX_ROWS_PER_FILE": "100000",
    "TIMER_SCHEDULE": "0 0 * * * *", 
    "CHUNK_SIZE": "100000",
    "ROW_PROCESS_SIZE": "10000",
    "DELETE_STAGING_FILES": "false",
    
    "SPN_CLIENT_ID": "your-client-id-for-local-development",
    "SPN_CLIENT_SECRET": "your-client-secret-for-local-development",
    "SPN_TENANT_ID": "your-tenant-id-for-local-development"
  }
}
```

## Configuration Options

### Multiple Workspace Configuration

You can query multiple Log Analytics workspaces in various ways:

1. **Define a default workspace**: Set `LOG_ANALYTICS_WORKSPACE_ID` to your primary workspace ID.

2. **Add additional workspaces**: Configure `LOG_ANALYTICS_ADDITIONAL_WORKSPACE_IDS` with a JSON array of additional workspace IDs.

3. **Specify workspaces per query**: In the `QUERIES_CONFIG`, you can include a `workspace_ids` array for each query, which will override the global workspace settings.

4. **Runtime workspace selection**: When triggering the function manually via HTTP, you can specify `workspace_ids` as a parameter.

### Storage Authentication Configuration

By default, the function uses Managed Identity for accessing Azure Data Lake Storage Gen2. However, you can configure it to use Service Principal (SPN) credentials instead:

1. **Using Managed Identity (default)**: Set `USE_SPN_FOR_STORAGE` to `false` or omit it.

2. **Using Service Principal**: Set `USE_SPN_FOR_STORAGE` to `true`. When enabled, the function will use the same SPN credentials that are used for Log Analytics to access ADLS Gen2.

This is particularly useful in scenarios where:
- The Managed Identity doesn't have sufficient permissions on the storage account
- You need to access storage in a different tenant or subscription
- You need to use the same authentication method for both Log Analytics and Storage

### Query Configuration

Configure queries in the `QUERIES_CONFIG` setting as a JSON array:

```json
[
  {
    "query": "AzureActivity | where TimeGenerated >= ago(1d)",
    "output_pattern": "azure_activity",
    "days": 1,
    "file_format": "parquet",
    "workspace_ids": ["workspace-id-1", "workspace-id-2"]
  },
  {
    "query": "SecurityEvent | where TimeGenerated >= ago(1d)",
    "output_pattern": "security_events",
    "days": 1,
    "file_format": "parquet"
  }
]
```

Each query configuration supports:

- `query`: The KQL query to run
- `output_pattern`: Pattern for the output file name
- `days`: Number of days to look back (default: 1)
- `file_format`: Output file format ("csv" or "parquet", default: "parquet")
- `workspace_ids`: Optional list of specific workspace IDs to query for this query

## Key Features in Detail

### Multiple Workspace Support

The function can query one or more Log Analytics workspaces with the same query:

- Each query can specify which workspaces to query
- Workspace ID information is preserved in the output data (added as a column)
- Results from multiple workspaces are combined into a single dataset

### Efficient File Transfers

The function uses efficient metadata-only rename operations for file transfers:

- 10-100x faster than full copy operations
- Reduced resource usage (bandwidth, CPU, memory)
- Atomic operation guarantees
- Automatic deletion of source files

### File Compression

All data is automatically compressed:

- CSV files are compressed with gzip (.csv.gz)
- Parquet files use gzip compression internally

### Memory Optimization

The function uses several techniques to optimize memory usage:

- Chunked processing of query results
- Automatic compression of data
- Streaming uploads to avoid storing full datasets in memory
- Garbage collection to free memory after processing
- Dynamic chunk sizing to adjust for memory constraints

## Running the Function

### Automatically (Timer Trigger)

The function runs automatically based on the schedule defined in `TIMER_SCHEDULE`.

### Manually (HTTP Trigger)

You can trigger the function manually via HTTP:

```bash
# Run all queries
curl -X POST https://your-function-app.azurewebsites.net/api/log-analytics-to-adls?code=your-function-key

# Run a specific query
curl -X POST https://your-function-app.azurewebsites.net/api/log-analytics-to-adls?code=your-function-key \
  -H "Content-Type: application/json" \
  -d '{"query_pattern": "azure_activity"}'

# Run a query on specific workspaces
curl -X POST https://your-function-app.azurewebsites.net/api/log-analytics-to-adls?code=your-function-key \
  -H "Content-Type: application/json" \
  -d '{"query_pattern": "azure_activity", "workspace_ids": ["workspace-id-1", "workspace-id-2"]}'
```

## Local Development with Blob Emulator

This project is now configured to automatically detect whether it's running in a local development environment or in Azure. When running locally, it uses the Azurite blob emulator instead of ADLS Gen2, which makes local development and testing easier.

### Setting Up Azurite

1. Install the Azurite emulator:

```bash
npm install -g azurite
```

2. Start Azurite:

```bash
azurite --silent --location azurite-data --debug azurite-debug.log
```

By default, Azurite will listen on:
- Blob service: http://127.0.0.1:10000
- Queue service: http://127.0.0.1:10001
- Table service: http://127.0.0.1:10002

### How It Works

The `StorageService` class automatically detects whether it's running in a local development environment by:

1. Checking if the `AZURE_FUNCTIONS_ENVIRONMENT` environment variable is set to "Development"
2. Checking if a socket connection can be established to the local Azurite endpoint (127.0.0.1:10000)

When running locally:
- The service uses Blob Storage APIs instead of ADLS Gen2 
- The storage URL is set to `http://127.0.0.1:10000/devstoreaccount1`
- The connection string uses the Azurite default credentials

When running in Azure:
- The service uses ADLS Gen2 APIs
- The storage URL is set to `https://{storage_account_name}.dfs.core.windows.net`
- Authentication uses the Function App's managed identity

### Differences Between Local and Azure Environments

While the code automatically adapts to the environment, there are some differences to be aware of:

1. **Directory Operations**: ADLS Gen2 supports true hierarchical namespaces with directories, while Blob Storage simulates directories with prefixes. The code handles this difference transparently.

2. **Rename Operations**: In ADLS Gen2, file renames are metadata-only operations. In Blob Storage, renames are implemented as copy+delete operations, which are slower.

3. **Performance**: Local blob emulator will be slower than ADLS Gen2 in Azure, especially for large file operations.

These differences are handled automatically by the `StorageService` class, allowing a seamless development experience without code changes when moving between environments.

## Local Development with Environment Variables

When developing locally, you can use environment variables for secrets instead of connecting to Azure Key Vault. This makes local development easier, especially when working offline or without access to Azure resources.

### How to Configure

1. Set `USE_ENV_VARS_FOR_SECRETS` to `true` in your `local.settings.json` file (it's `true` by default for local development).

2. Add your secrets as environment variables in your `local.settings.json` file. The service will try two formats for each secret:
   - The direct secret name (e.g., `loganalytics-spn-client-id`)
   - A simplified format (e.g., `SPN_CLIENT_ID`)

Example configuration in `local.settings.json`:

```json
{
  "Values": {
    "USE_ENV_VARS_FOR_SECRETS": "true",
    "SPN_CLIENT_ID": "your-client-id",
    "SPN_CLIENT_SECRET": "your-client-secret",
    "SPN_TENANT_ID": "your-tenant-id"
  }
}
```

### How It Works

When `USE_ENV_VARS_FOR_SECRETS` is enabled and the application is running in local development mode:

1. The `AuthenticationService` will first check for the secret in environment variables.
2. If found, it will use the value from the environment variable.
3. If not found, it will fall back to Azure Key Vault (which requires internet access and appropriate credentials).

This approach allows you to:
- Develop offline without Key Vault access
- Use different credentials for local development vs. production
- Simplify the development setup process
- Avoid storing sensitive credentials in code or configuration files that might be shared

### Security Considerations

Remember that `local.settings.json` is excluded from source control by default (via `.gitignore`). This is important since it contains sensitive information. If you need to share configuration with other developers, create a `local.settings.example.json` file with placeholder values.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 