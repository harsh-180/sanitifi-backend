# Databricks Configuration

This document explains how to configure Databricks access for the application.

## Environment Variables

The following environment variables are required for Databricks integration:

### Required Variables

- `DATABRICKS_ACCESS_TOKEN`: Your Databricks access token
  - Generate this from your Databricks workspace: User Settings > Developer > Access Tokens
  - This is a sensitive credential and should never be committed to version control

### Optional Variables

- `DATABRICKS_WORKSPACE_URL`: Your Databricks workspace URL
  - Default: `https://dbc-e8343889-d484.cloud.databricks.com`
  - Only set this if you're using a different workspace

## Setting Environment Variables

### Windows (PowerShell)
```powershell
$env:DATABRICKS_ACCESS_TOKEN="your_token_here"
$env:DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
```

### Windows (Command Prompt)
```cmd
set DATABRICKS_ACCESS_TOKEN=your_token_here
set DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com
```

### Linux/macOS
```bash
export DATABRICKS_ACCESS_TOKEN="your_token_here"
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
```

## Security Notes

- Never commit access tokens to version control
- Use environment variables or secure configuration management
- Rotate access tokens regularly
- Use least-privilege access tokens when possible

## Troubleshooting

If you get an error about missing `DATABRICKS_ACCESS_TOKEN`, make sure you have set the environment variable correctly in your system.
