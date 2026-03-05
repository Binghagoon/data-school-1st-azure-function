# data-school-1st-azure-function

Azure Function project for disaster and data sync workflows.

## Overview

This project provides an Azure Function (Python v2 programming model) with:
- HTTP endpoints for disaster data and DB health checks
- Timer-triggered data sync jobs

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

Copy `local.settings.json` and set required values:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "POSTGRES_HOST": "your-server.postgres.database.azure.com",
    "POSTGRES_DB": "your_database",
    "POSTGRES_USER": "your_user",
    "POSTGRES_PASSWORD": "your_password",
    "POSTGRES_PORT": "5432",
    "POSTGRES_SSLMODE": "require",
    "WEBHOOK_URL": "https://your-webhook-url.com/endpoint"
  }
}
```

### 3. Run locally

```bash
func start
```

### 4. Invoke functions

```bash
# Get disasters by date
curl "http://localhost:7071/api/disasters?date=2026-03-05"

# Check PostgreSQL connectivity
curl http://localhost:7071/api/db-health
```

## Deployment

Deploy to Azure using Azure Functions Core Tools:

```bash
func azure functionapp publish <YOUR_FUNCTION_APP_NAME>
```

## Project Structure

```text
.
|-- function_app.py
|-- blueprint/
|   |-- main.py
|   |-- disasters.py
|   `-- db_health.py
|-- db/
|   `-- postgres_connector.py
|-- host.json
|-- local.settings.json
|-- requirements.txt
`-- README.md
```
