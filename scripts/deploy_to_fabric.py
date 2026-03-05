"""Deploy dbt project to Microsoft Fabric: lakehouse, files, and notebook.

Prerequisites:
  - az login (interactive Azure CLI authentication)
  - pip install azure-identity azure-storage-file-datalake requests

Usage:
  export FABRIC_WORKSPACE_NAME="dbt"        # optional, defaults to "dbt"
  export FABRIC_LAKEHOUSE_NAME="data"       # optional, defaults to "data"
  python scripts/deploy_to_fabric.py
"""

import base64
import json
import os
import sys
import time
from pathlib import Path

import requests
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

# --- Config ---
WORKSPACE_NAME            =  "prod"
LAKEHOUSE_NAME            =  "raw"
NOTEBOOK_NAME             =  "dbt"
METADATA_LOCAL_PATH       = '/lakehouse/default/Files/metadata.db'

EXCLUDE_DIRS = {".git", "target", "logs", "dbt_packages", "__pycache__", ".github", "scripts"}
EXCLUDE_FILES = {"metadata.db"}  # runtime state, do not overwrite

DEPLOY_BRANCH = "production"

BASE_URL = "https://api.fabric.microsoft.com/v1"

# --- Checkout production branch ---
import subprocess
project_root_path = Path(__file__).resolve().parent.parent
print(f"Checking out '{DEPLOY_BRANCH}' branch...")
subprocess.run(["git", "checkout", DEPLOY_BRANCH], check=True, cwd=project_root_path)
subprocess.run(["git", "pull", "origin", DEPLOY_BRANCH], check=True, cwd=project_root_path)
print(f"Branch: {DEPLOY_BRANCH} (up to date)")

# --- Auth: reuses existing az login session ---
print("Authenticating with Azure CLI credential...")
credential = AzureCliCredential()
fabric_token = credential.get_token("https://api.fabric.microsoft.com/.default").token
headers = {"Authorization": f"Bearer {fabric_token}", "Content-Type": "application/json"}


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


# --- Step 1: Resolve workspace ID from name ---
print(f"Resolving workspace '{WORKSPACE_NAME}'...")
resp = requests.get(
    f"{BASE_URL}/workspaces",
    headers=headers,
    params={"$filter": f"displayName eq '{WORKSPACE_NAME}'"},
)
resp.raise_for_status()
workspaces = resp.json().get("value", [])
if not workspaces:
    print(f"Error: workspace '{WORKSPACE_NAME}' not found.")
    sys.exit(1)
WORKSPACE_ID = workspaces[0]["id"]
print(f"  workspace id: {WORKSPACE_ID}")

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
fs = datalake_client.get_file_system_client(WORKSPACE_NAME)

project_root = Path(__file__).resolve().parent.parent  # repo root (parent of scripts/)
uploaded = 0

for local_path in sorted(project_root.rglob("*")):
    if local_path.is_dir():
        continue
    if any(part in EXCLUDE_DIRS for part in local_path.relative_to(project_root).parts):
        continue
    if local_path.name in EXCLUDE_FILES:
        continue

    relative = str(local_path.relative_to(project_root)).replace("\\", "/")
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
ROOT_PATH = f"abfss://{WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_NAME}.Lakehouse"

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
    # Update existing notebook
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
    print(f"  updated notebook '{NOTEBOOK_NAME}'")
else:
    # Create new notebook
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
    print(f"  created notebook '{NOTEBOOK_NAME}'")

print("\nDeploy complete.")
