"""Deploy dbt project to Microsoft Fabric: lakehouse, files, and notebook.

Prerequisites:
  - az login (interactive Azure CLI authentication)
  - pip install azure-identity azure-storage-file-datalake requests

Usage:
  python deploy_to_fabric.py
"""

import base64
import json
import os
import sys
import time
from pathlib import Path

import jwt
import requests
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

# --- Config ---
TENANT_ID                 = "4a86d5bb-4173-45ee-bfd5-a3b56ee2d3d5"
WORKSPACE_ID              = "be079b0f-3416-4de2-9184-9c546bad223c"
LAKEHOUSE_NAME            = "raw"
NOTEBOOK_NAME             = "run"
PIPELINE_NAME             = "run_pipeline"
PIPELINE_TIMEOUT          = "0.01:00:00"  # 1 hour
SCHEDULE_INTERVAL_MINUTES = 60
METADATA_LOCAL_PATH       = '/lakehouse/default/Files/metadata.db'

EXCLUDE_DIRS = {".git", "target", "logs", "dbt_packages", "__pycache__", ".github", ".claude"}
EXCLUDE_FILES = {"metadata.db", "deploy_to_fabric.py", ".user.yml", "nul", "README.md"}
EXCLUDE_PATTERNS = {"%SystemDrive%"}

DEPLOY_BRANCH = "production"

BASE_URL = "https://api.fabric.microsoft.com/v1"

# --- Checkout production branch ---
import subprocess
project_root = Path(__file__).resolve().parent
print(f"Checking out '{DEPLOY_BRANCH}' branch...")
subprocess.run(["git", "checkout", DEPLOY_BRANCH], check=True, cwd=project_root)
subprocess.run(["git", "pull", "origin", DEPLOY_BRANCH], check=True, cwd=project_root)
print(f"Branch: {DEPLOY_BRANCH} (up to date)")

# --- Auth: az login with tenant enforcement ---
print("Authenticating with Azure CLI credential...")
credential = AzureCliCredential()

try:
    fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    claims = jwt.decode(fabric_token, options={"verify_signature": False})
    actual_tenant = claims.get("tid")
    if actual_tenant != TENANT_ID:
        print(f"  wrong tenant '{actual_tenant}', logging into '{TENANT_ID}'...")
        raise Exception("wrong tenant")
except Exception:
    print(f"Logging in to tenant {TENANT_ID}...")
    subprocess.run(["az", "login", "--tenant", TENANT_ID], check=True)
    credential = AzureCliCredential()
    fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    claims = jwt.decode(fabric_token, options={"verify_signature": False})

headers = {"Authorization": f"Bearer {fabric_token}", "Content-Type": "application/json"}
print(f"  tenant: {claims.get('tid')}")
print(f"  user:   {claims.get('upn', claims.get('preferred_username', 'N/A'))}")


def wait_for_operation(operation_id):
    """Poll a long-running Fabric API operation until complete."""
    for _ in range(30):
        time.sleep(2)
        resp = requests.get(f"{BASE_URL}/operations/{operation_id}", headers=headers)
        resp.raise_for_status()
        status = resp.json().get("status")
        if status == "Succeeded":
            return True
        if status == "Failed":
            print(f"  operation failed: {resp.json().get('error', {}).get('message', 'unknown')}")
            return False
    print("  operation timed out")
    return False


# --- Step 1: Workspace ---
print(f"Using workspace {WORKSPACE_ID}")

# --- Step 2: Create lakehouse if not exists ---
print(f"Checking if lakehouse '{LAKEHOUSE_NAME}' exists...")
resp = requests.get(
    f"{BASE_URL}/workspaces/{WORKSPACE_ID}/lakehouses",
    headers=headers,
)
resp.raise_for_status()
lakehouse = next(
    (lh for lh in resp.json().get("value", []) if lh["displayName"] == LAKEHOUSE_NAME),
    None,
)

if lakehouse is None:
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items",
        headers=headers,
        json={"displayName": LAKEHOUSE_NAME, "type": "Lakehouse"},
    )
    resp.raise_for_status()
    lakehouse = resp.json()
    time.sleep(2)  # wait for provisioning
    print(f"  created lakehouse '{LAKEHOUSE_NAME}'")
else:
    print(f"  lakehouse '{LAKEHOUSE_NAME}' already exists")

LAKEHOUSE_ID = lakehouse["id"]

# --- Step 3: Upload dbt files to OneLake ---
print("Uploading dbt project files to OneLake...")
datalake_client = DataLakeServiceClient(
    account_url="https://onelake.dfs.fabric.microsoft.com",
    credential=credential,
)
fs = datalake_client.get_file_system_client(WORKSPACE_ID)

uploaded = 0

for local_path in sorted(project_root.rglob("*")):
    if local_path.is_dir():
        continue
    if any(part in EXCLUDE_DIRS for part in local_path.relative_to(project_root).parts):
        continue
    if local_path.name in EXCLUDE_FILES:
        continue

    relative = str(local_path.relative_to(project_root)).replace("\\", "/")
    if any(p in relative for p in EXCLUDE_PATTERNS):
        continue
    onelake_path = f"{LAKEHOUSE_NAME}.Lakehouse/Files/dbt/{relative}"

    file_client = fs.get_file_client(onelake_path)
    with open(local_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)
    print(f"  {relative}")
    uploaded += 1

print(f"  {uploaded} files uploaded to Files/dbt/")

# --- Step 4: Deploy notebook as Fabric item with lakehouse attached ---
print(f"Deploying notebook '{NOTEBOOK_NAME}' to workspace...")

# Derive ROOT_PATH from workspace and lakehouse names
ROOT_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}.Lakehouse"

# Build notebook content with values from this script
notebook_json = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "cells": [
        {
            "cell_type": "code",
            "metadata": {},
            "outputs": [],
            "execution_count": None,
            "source": [
                "!pip install -q duckdb==1.4.4\n",
                "!pip install -q dbt-duckdb\n",
                "import sys\n",
                "sys.exit(0)",
            ],
        },
        {
            "cell_type": "code",
            "metadata": {},
            "outputs": [],
            "execution_count": None,
            "source": [
                "import os\n",
                f"os.environ['ROOT_PATH']           = '{ROOT_PATH}'\n",
                f"os.environ['METADATA_LOCAL_PATH'] = '{METADATA_LOCAL_PATH}'\n",
                "os.environ['download_limit']      = '100'\n",
                "os.environ['process_limit']       = '100'\n",
                "\n",
                "!cd /lakehouse/default/Files/dbt && dbt run --target prod && dbt test --target prod",
            ],
        },
    ],
    "metadata": {},
}

# Set notebook metadata for Python runtime with lakehouse attached
notebook_json["metadata"] = {
    "kernelspec": {"name": "python3", "display_name": "python3", "language": "python"},
    "kernel_info": {"name": "jupyter"},
    "microsoft": {"language": "python", "language_group": "jupyter_python"},
    "language_info": {"name": "python"},
    "trident": {
        "lakehouse": {
            "default_lakehouse": LAKEHOUSE_ID,
            "default_lakehouse_name": LAKEHOUSE_NAME,
            "default_lakehouse_workspace_id": WORKSPACE_ID,
        }
    },
}

notebook_base64 = base64.b64encode(json.dumps(notebook_json, indent=2).encode("utf-8")).decode("utf-8")

# Check if notebook already exists
resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/notebooks", headers=headers)
resp.raise_for_status()
existing_notebook = next(
    (nb for nb in resp.json().get("value", []) if nb["displayName"] == NOTEBOOK_NAME),
    None,
)

definition_payload = {
    "definition": {
        "format": "ipynb",
        "parts": [
            {
                "path": "notebook-content.ipynb",
                "payload": notebook_base64,
                "payloadType": "InlineBase64",
            }
        ],
    }
}

if existing_notebook:
    notebook_id = existing_notebook["id"]
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/notebooks/{notebook_id}/updateDefinition",
        headers=headers,
        json=definition_payload,
    )
    resp.raise_for_status()
    if resp.status_code == 202:
        op_id = resp.headers.get("x-ms-operation-id")
        if op_id:
            wait_for_operation(op_id)
    print(f"  updated notebook '{NOTEBOOK_NAME}' (id: {notebook_id})")
else:
    definition_payload["displayName"] = NOTEBOOK_NAME
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/notebooks",
        headers=headers,
        json=definition_payload,
    )
    resp.raise_for_status()
    if resp.status_code == 202:
        op_id = resp.headers.get("x-ms-operation-id")
        if op_id:
            wait_for_operation(op_id)
    # Re-fetch to get the notebook ID
    resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/notebooks", headers=headers)
    resp.raise_for_status()
    nb = next(nb for nb in resp.json().get("value", []) if nb["displayName"] == NOTEBOOK_NAME)
    notebook_id = nb["id"]
    print(f"  created notebook '{NOTEBOOK_NAME}' (id: {notebook_id})")

# --- Step 5: Deploy pipeline with notebook activity + timeout ---
print(f"Deploying pipeline '{PIPELINE_NAME}' to workspace...")

pipeline_json = {
    "properties": {
        "activities": [
            {
                "name": "Run Notebook",
                "type": "TridentNotebook",
                "dependsOn": [],
                "policy": {
                    "timeout": PIPELINE_TIMEOUT,
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": False,
                    "secureInput": False,
                },
                "typeProperties": {
                    "notebookId": notebook_id,
                    "workspaceId": WORKSPACE_ID,
                },
            }
        ]
    }
}

pipeline_base64 = base64.b64encode(json.dumps(pipeline_json).encode("utf-8")).decode("utf-8")

# Check if pipeline already exists
resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items?type=DataPipeline", headers=headers)
resp.raise_for_status()
existing_pipeline = next(
    (p for p in resp.json().get("value", []) if p["displayName"] == PIPELINE_NAME),
    None,
)

pipeline_def = {
    "definition": {
        "parts": [
            {
                "path": "pipeline-content.json",
                "payload": pipeline_base64,
                "payloadType": "InlineBase64",
            }
        ]
    }
}

if existing_pipeline:
    pipeline_id = existing_pipeline["id"]
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/updateDefinition",
        headers=headers,
        json=pipeline_def,
    )
    resp.raise_for_status()
    if resp.status_code == 202:
        op_id = resp.headers.get("x-ms-operation-id")
        if op_id:
            wait_for_operation(op_id)
    print(f"  updated pipeline '{PIPELINE_NAME}'")
else:
    pipeline_def["displayName"] = PIPELINE_NAME
    pipeline_def["type"] = "DataPipeline"
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items",
        headers=headers,
        json=pipeline_def,
    )
    resp.raise_for_status()
    if resp.status_code == 202:
        op_id = resp.headers.get("x-ms-operation-id")
        if op_id:
            wait_for_operation(op_id)
    pipeline_id = resp.json().get("id")
    print(f"  created pipeline '{PIPELINE_NAME}'")

# --- Step 6: Schedule the pipeline ---
print(f"Setting schedule (every {SCHEDULE_INTERVAL_MINUTES} min)...")

resp = requests.get(
    f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules",
    headers=headers,
)
resp.raise_for_status()
existing_schedules = resp.json().get("value", [])

schedule_config = {
    "enabled": True,
    "configuration": {
        "startDateTime": "2025-01-01T00:00:00",
        "endDateTime": "2030-12-31T23:59:00",
        "localTimeZoneId": "AUS Eastern Standard Time",
        "type": "Cron",
        "interval": SCHEDULE_INTERVAL_MINUTES,
    },
}

if existing_schedules:
    schedule_id = existing_schedules[0]["id"]
    resp = requests.patch(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules/{schedule_id}",
        headers=headers,
        json=schedule_config,
    )
    resp.raise_for_status()
    print(f"  updated schedule (id: {schedule_id})")
else:
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules",
        headers=headers,
        json=schedule_config,
    )
    resp.raise_for_status()
    schedule_id = resp.json().get("id", "unknown")
    print(f"  created schedule (id: {schedule_id})")

print("\nDeploy complete.")
