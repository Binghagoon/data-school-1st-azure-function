# data-school-1st-azure-function

Azure Function project for API crawling.

## Overview

This project provides an Azure Function (Python v2 programming model) that crawls
a target API endpoint and returns the response. The function is exposed as an HTTP
trigger at the route `GET /api/crawl`.

## Prerequisites

- Python 3.10+
- [Azure Functions Core Tools v4](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local)
- [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite) (local storage emulator) or an Azure Storage account

## Getting Started

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure local settings

Copy `local.settings.json` and set the `TARGET_API_URL` value to the API you want
to crawl by default:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "TARGET_API_URL": "https://your-target-api.com/endpoint",
    "POSTGRES_HOST": "your-server.postgres.database.azure.com",
    "POSTGRES_DB": "your_database",
    "POSTGRES_USER": "your_user",
    "POSTGRES_PASSWORD": "your_password",
    "POSTGRES_PORT": "5432",
    "POSTGRES_SSLMODE": "require"
  }
}
```

### 3. Run locally

```bash
func start
```

### 4. Invoke the function

```bash
# Use the TARGET_API_URL environment variable
curl http://localhost:7071/api/crawl

# Override the target URL via query parameter
curl "http://localhost:7071/api/crawl?url=https://api.example.com/data"

# Check PostgreSQL connectivity
curl http://localhost:7071/api/db-health
```

## Deployment

Deploy to Azure using the Azure Functions Core Tools:

```bash
func azure functionapp publish <YOUR_FUNCTION_APP_NAME>
```

## Project Structure

```
.
├── function_app.py       # Main Azure Function app (HTTP triggers – Main/CrawlApi/DbHealth)
├── db/
│   └── postgres_connector.py  # PostgreSQL connection helper
├── host.json             # Azure Functions host configuration
├── local.settings.json   # Local development settings (not committed to production)
├── requirements.txt      # Python dependencies
└── README.md
```
