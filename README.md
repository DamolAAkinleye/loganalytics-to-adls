# Log Analytics to ADLS Gen2 Extractor

This repository contains an Azure Function that extracts data from Azure Log Analytics and stores it in Azure Data Lake Storage Gen2.

## Features

- Extracts data from Log Analytics using multiple configurable queries
- Scheduled extraction using timer trigger with configurable schedule
- Securely authenticates using Service Principal (SPN) for Log Analytics and Managed Identity for other resources
- Implements robust retry logic and error handling
- Supports data pagination for large datasets
- Uses metadata-only operations for ultrafast file transfers (10-100x faster than copying)
- Memory-efficient design to handle large datasets with minimal memory usage
- Automatic gzip compression for all output files (both CSV and Parquet)
- Streaming data processing to minimize memory footprint
- Each query can have its own output pattern, lookback period, and file format

## Detailed Documentation

For detailed setup and usage instructions, please see the [Azure Function README](azure_function/README.md).

## Solution Structure

```
.
└── azure_function/               # Azure Function implementation
    ├── LogAnalyticsToADLS/       # Main package
    │   ├── config/               # Configuration
    │   ├── services/             # Service classes
    │   └── utils/                # Utilities
    ├── function_app.py           # Function entry point
    ├── host.json                 # Function configuration
    ├── local.settings.json       # Local settings template
    ├── rest-client-examples.http # HTTP request examples for testing
    ├── README.md                 # Detailed documentation
    └── requirements.txt          # Dependencies
```

## Getting Started

To get started quickly:

```bash
# Clone the repository
git clone <repository-url>
cd <repository-directory>

# Navigate to the Azure Function directory
cd azure_function

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure your settings
# Edit local.settings.json with your environment details

# Run locally
func start
```

## Testing the Function

The repository includes VS Code REST Client examples for easily testing the function:

1. Install the [REST Client extension](https://marketplace.visualstudio.com/items?itemName=humao.rest-client) for VS Code
2. Open the `azure_function/rest-client-examples.http` file
3. Update the variables with your function URL and key
4. Click "Send Request" above any example to test

## Multiple Query Configuration

The function supports multiple queries, each with its own:
- Output file pattern (naming convention)
- Lookback period (days)
- File format (CSV or Parquet)

Configure these queries in the QUERIES_CONFIG application setting as a JSON array:

```json
[
  {
    "query": "AzureActivity | where TimeGenerated >= ago(1d)",
    "output_pattern": "azure_activity",
    "days": 1,
    "file_format": "parquet"
  },
  {
    "query": "SecurityEvent | where TimeGenerated >= ago(1d)",
    "output_pattern": "security_events",
    "days": 1,
    "file_format": "parquet"
  }
]
```

## Timer Trigger

The function automatically runs on a schedule defined by the TIMER_SCHEDULE setting using the NCRONTAB format:
- `0 0 * * * *` - Run every hour at the start of the hour (default)
- `0 0 0 * * *` - Run every day at midnight
- `0 0 0 * * 1-5` - Run every weekday at midnight

## Memory Optimization and Compression

This solution is optimized for memory efficiency and storage optimization:

- All data is automatically compressed using gzip
- Processing is done in configurable chunks to control memory usage
- Streaming uploads avoid loading entire datasets into memory
- Automatic garbage collection helps manage memory usage
- Files are stored in CSV.gz or gzipped Parquet format for optimal storage
- Metadata-only rename operations instead of file copying for ultrafast transfers

## License

This project is licensed under the MIT License. 