"""Deploy dbt project to Microsoft Fabric: lakehouse, files, and notebook.

Prerequisites:
  - az login (interactive Azure CLI authentication)
  - pip install azure-identity azure-storage-file-datalake requests

Usage:
  python deploy_to_fabric.py                           # deploy everything (production)
  python deploy_to_fabric.py --env test                # deploy everything (test)
  python deploy_to_fabric.py --env test notebook       # deploy notebook to test
  python deploy_to_fabric.py semantic_model            # deploy semantic model (production)

Steps: lakehouse, files, initial_load, notebook, semantic_model, pipeline, schedule
Environments: test, production (default: production)
"""

import argparse
import base64
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import jwt
import requests
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

ALL_STEPS = ["lakehouse", "files", "initial_load", "notebook", "semantic_model", "pipeline", "schedule"]
ENV_CHOICES = ["test", "production"]
parser = argparse.ArgumentParser(description="Deploy dbt project to Microsoft Fabric")
parser.add_argument("--env", choices=ENV_CHOICES, default="production",
                    help="Target environment (default: production)")
parser.add_argument("steps", nargs="*", default=None,
                    help=f"Steps to deploy (default: all). Choices: {', '.join(ALL_STEPS)}")
args = parser.parse_args()
if not args.steps:
    STEPS = set(ALL_STEPS)
else:
    invalid = set(args.steps) - set(ALL_STEPS)
    if invalid:
        parser.error(f"invalid step(s): {', '.join(invalid)}. Choose from: {', '.join(ALL_STEPS)}")
    STEPS = set(args.steps)

# --- Config (from deploy_config.json) ---
_config_path = Path(__file__).resolve().parent / "deploy_config.json"
with open(_config_path) as _f:
    _cfg = json.load(_f)

_env_cfg = _cfg["environments"][args.env]
print(f"Environment: {args.env}")

TENANT_ID                 = _cfg["tenant_id"]
WORKSPACE_ID              = _env_cfg["workspace_id"]
LAKEHOUSE_NAME            = _env_cfg.get("lakehouse_name", _cfg["lakehouse_name"])
NOTEBOOK_NAME             = _env_cfg.get("notebook_name", _cfg["notebook_name"])
PIPELINE_NAME             = _env_cfg.get("pipeline_name", _cfg["pipeline_name"])
PIPELINE_TIMEOUT          = _env_cfg.get("pipeline_timeout", _cfg.get("pipeline_timeout", "0.01:00:00"))
SCHEDULE_INTERVAL_MINUTES = int(_env_cfg.get("schedule_interval_minutes", _cfg.get("schedule_interval_minutes", 30)))
METADATA_LOCAL_PATH       = _env_cfg.get("metadata_local_path", _cfg.get("metadata_local_path", "/lakehouse/default/Files/metadata.db"))
DEPLOY_BRANCH             = _env_cfg.get("deploy_branch", _cfg.get("deploy_branch", "production"))
SEMANTIC_MODEL_NAME       = _env_cfg.get("semantic_model_name", _cfg.get("semantic_model_name", "aemo_electricity"))
DOWNLOAD_LIMIT            = _env_cfg.get("download_limit", _cfg.get("download_limit", 100))
PROCESS_LIMIT             = _env_cfg.get("process_limit", _cfg.get("process_limit", 100))

EXCLUDE_DIRS = {".git", "target", "logs", "dbt_packages", "__pycache__", ".github", ".claude"}
EXCLUDE_FILES = {"metadata.db", ".user.yml", "nul", "README.md", "deploy_config.json"}
EXCLUDE_PATTERNS = {"%SystemDrive%"}

BASE_URL = "https://api.fabric.microsoft.com/v1"

IS_CI = os.environ.get("CI") == "true"

# --- Resolve project root ---
if IS_CI:
    project_root = Path(__file__).resolve().parent
    print(f"CI mode: using {project_root}")
else:
    REPO_URL = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        capture_output=True, text=True, check=True,
        cwd=Path(__file__).resolve().parent,
    ).stdout.strip()
    tmp_dir = tempfile.mkdtemp(prefix="dbt_deploy_")
    project_root = Path(tmp_dir)
    print(f"Cloning '{DEPLOY_BRANCH}' branch into {tmp_dir}...")
    subprocess.run(
        ["git", "clone", "--branch", DEPLOY_BRANCH, "--depth", "1", REPO_URL, tmp_dir],
        check=True,
    )
    print(f"Branch: {DEPLOY_BRANCH} (cloned)")

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
    if IS_CI:
        print("ERROR: Azure CLI authentication failed in CI. Check azure/login step.")
        sys.exit(1)
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


# --- Resolve lakehouse (always needed for LAKEHOUSE_ID) ---
print(f"Using workspace {WORKSPACE_ID}")
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

FIRST_DEPLOY = False
if lakehouse is None and "lakehouse" in STEPS:
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/lakehouses",
        headers=headers,
        json={
            "displayName": LAKEHOUSE_NAME,
            "creationPayload": {"enableSchemas": True},
        },
    )
    resp.raise_for_status()
    lakehouse = resp.json()
    time.sleep(2)
    FIRST_DEPLOY = True
    print(f"  created lakehouse '{LAKEHOUSE_NAME}'")
elif lakehouse is None:
    print(f"  ERROR: lakehouse '{LAKEHOUSE_NAME}' does not exist. Run with 'lakehouse' step first.")
    sys.exit(1)
else:
    print(f"  lakehouse '{LAKEHOUSE_NAME}' already exists")

# Output for GitHub Actions
if IS_CI:
    gh_output = os.environ.get("GITHUB_OUTPUT")
    if gh_output:
        with open(gh_output, "a") as f:
            f.write(f"first_deploy={'true' if FIRST_DEPLOY else 'false'}\n")

LAKEHOUSE_ID = lakehouse["id"]
ONELAKE_URL = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_ID}"
ROOT_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}"


# --- Step: files ---
def deploy_files():
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
        onelake_path = f"{LAKEHOUSE_ID}/Files/dbt/{relative}"

        file_client = fs.get_file_client(onelake_path)
        with open(local_path, "rb") as f:
            file_client.upload_data(f, overwrite=True)
        print(f"  {relative}")
        uploaded += 1

    print(f"  {uploaded} files uploaded to Files/dbt/")


# --- Step: notebook ---
def deploy_notebook(download_limit=100, process_limit=100):
    print(f"Deploying notebook '{NOTEBOOK_NAME}' (download_limit={download_limit})...")

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
                    f"os.environ['download_limit']      = '{download_limit}'\n",
                    f"os.environ['process_limit']       = '{process_limit}'\n",
                    "\n",
                    "!cd /lakehouse/default/Files/dbt && dbt run --target prod --profiles-dir . && dbt test --target prod --profiles-dir .",
                ],
            },
        ],
        "metadata": {
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
        },
    }

    notebook_base64 = base64.b64encode(json.dumps(notebook_json, indent=2).encode("utf-8")).decode("utf-8")

    resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/notebooks", headers=headers)
    resp.raise_for_status()
    existing = next(
        (nb for nb in resp.json().get("value", []) if nb["displayName"] == NOTEBOOK_NAME),
        None,
    )

    definition_payload = {
        "definition": {
            "format": "ipynb",
            "parts": [{"path": "notebook-content.ipynb", "payload": notebook_base64, "payloadType": "InlineBase64"}],
        }
    }

    if existing:
        notebook_id = existing["id"]
        resp = requests.post(
            f"{BASE_URL}/workspaces/{WORKSPACE_ID}/notebooks/{notebook_id}/updateDefinition",
            headers=headers, json=definition_payload,
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
            headers=headers, json=definition_payload,
        )
        resp.raise_for_status()
        if resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id")
            if op_id:
                wait_for_operation(op_id)
        resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/notebooks", headers=headers)
        resp.raise_for_status()
        nb = next(nb for nb in resp.json().get("value", []) if nb["displayName"] == NOTEBOOK_NAME)
        notebook_id = nb["id"]
        print(f"  created notebook '{NOTEBOOK_NAME}' (id: {notebook_id})")

    return notebook_id


# --- Step: pipeline ---
def deploy_pipeline(notebook_id):
    print(f"Deploying pipeline '{PIPELINE_NAME}' to workspace...")

    pipeline_json = {
        "properties": {
            "concurrency": 1,
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

    resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items?type=DataPipeline", headers=headers)
    resp.raise_for_status()
    existing = next(
        (p for p in resp.json().get("value", []) if p["displayName"] == PIPELINE_NAME),
        None,
    )

    pipeline_def = {
        "definition": {
            "parts": [{"path": "pipeline-content.json", "payload": pipeline_base64, "payloadType": "InlineBase64"}]
        }
    }

    if existing:
        pipeline_id = existing["id"]
        resp = requests.post(
            f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/updateDefinition",
            headers=headers, json=pipeline_def,
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
            headers=headers, json=pipeline_def,
        )
        resp.raise_for_status()
        if resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id")
            if op_id:
                wait_for_operation(op_id)
        pipeline_id = resp.json().get("id")
        print(f"  created pipeline '{PIPELINE_NAME}'")

    return pipeline_id


# --- Step: schedule ---
def deploy_schedule(pipeline_id):
    enabled = SCHEDULE_INTERVAL_MINUTES > 0
    print(f"Setting schedule ({'every ' + str(SCHEDULE_INTERVAL_MINUTES) + ' min' if enabled else 'disabled'})...")

    resp = requests.get(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules",
        headers=headers,
    )
    resp.raise_for_status()
    existing_schedules = resp.json().get("value", [])

    interval = SCHEDULE_INTERVAL_MINUTES if enabled else 720  # 12h placeholder when disabled
    schedule_config = {
        "enabled": enabled,
        "configuration": {
            "startDateTime": "2025-01-01T00:00:00",
            "endDateTime": "2030-12-31T23:59:00",
            "localTimeZoneId": "AUS Eastern Standard Time",
            "type": "Cron",
            "interval": interval,
        },
    }

    if existing_schedules:
        schedule_id = existing_schedules[0]["id"]
        existing_config = existing_schedules[0].get("configuration", {})
        if existing_config.get("interval") == interval and existing_schedules[0].get("enabled") == enabled:
            print(f"  schedule unchanged (id: {schedule_id})")
            return
        resp = requests.patch(
            f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules/{schedule_id}",
            headers=headers, json=schedule_config,
        )
        resp.raise_for_status()
        print(f"  updated schedule (id: {schedule_id})")
    else:
        resp = requests.post(
            f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules",
            headers=headers, json=schedule_config,
        )
        resp.raise_for_status()
        schedule_id = resp.json().get("id", "unknown")
        print(f"  created schedule (id: {schedule_id})")


# --- Step: run notebook one-off and wait ---
def run_notebook_and_wait(notebook_id):
    print("Triggering notebook run...")
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{notebook_id}/jobs/instances?jobType=RunNotebook",
        headers=headers,
    )
    resp.raise_for_status()
    location = resp.headers.get("Location")
    if not location:
        print("  WARNING: no job location returned, cannot wait for completion")
        return False

    print("  waiting for notebook to complete (this may take several minutes)...")
    for i in range(120):  # up to 20 minutes
        time.sleep(10)
        resp = requests.get(location, headers=headers)
        resp.raise_for_status()
        status = resp.json().get("status")
        if status == "Completed":
            print("  notebook run completed successfully")
            return True
        if status in ("Failed", "Cancelled", "Deduped"):
            error = resp.json().get("failureReason", resp.json())
            print(f"  notebook run {status.lower()}: {error}")
            return False
        if i % 6 == 0:
            print(f"  still running... ({i * 10}s)")
    print("  notebook run timed out")
    return False


# --- Step: refresh semantic model ---
def refresh_semantic_model(semantic_model_id):
    """Refresh semantic model using Fabric Job Scheduler API (works with service principals)."""
    print("  refreshing semantic model...")
    resp = requests.post(
        f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items/{semantic_model_id}/jobs/DefaultJob/instances",
        headers=headers,
    )
    if resp.status_code != 202:
        print(f"  refresh failed ({resp.status_code}): {resp.text}")
        return

    location = resp.headers.get("Location")
    if not location:
        print("  refresh triggered (no location to poll)")
        return

    print("  refresh triggered, waiting for completion...")
    for attempt in range(60):
        time.sleep(5)
        sr = requests.get(location, headers=headers)
        sr.raise_for_status()
        status = sr.json().get("status")
        if status == "Completed":
            print("  refresh completed")
            return
        if status in ("Failed", "Cancelled"):
            error = sr.json().get("failureReason", sr.json())
            print(f"  refresh {status.lower()}: {error}")
            return
        if attempt % 6 == 0 and attempt > 0:
            print(f"  refresh still running...")
    print("  refresh timed out")


# --- Step: semantic_model ---
def deploy_semantic_model():
    print(f"Deploying semantic model '{SEMANTIC_MODEL_NAME}'...")

    # Read model.bim and substitute OneLake URL placeholder
    bim_path = project_root / "model.bim"
    bim_content = bim_path.read_text(encoding="utf-8")
    bim_content = bim_content.replace("{{ONELAKE_URL}}", ONELAKE_URL)
    print(f"  loaded {bim_path}")

    bim_base64 = base64.b64encode(bim_content.encode()).decode()
    pbism = json.dumps({"version": "1.0"})
    parts = [
        {"path": "model.bim", "payload": bim_base64, "payloadType": "InlineBase64"},
        {"path": "definition.pbism", "payload": base64.b64encode(pbism.encode()).decode(), "payloadType": "InlineBase64"},
    ]

    resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/semanticModels", headers=headers)
    resp.raise_for_status()
    existing = next(
        (m for m in resp.json().get("value", []) if m["displayName"] == SEMANTIC_MODEL_NAME),
        None,
    )

    sm_definition = {"definition": {"parts": parts}}

    if existing:
        semantic_model_id = existing["id"]
        resp = requests.post(
            f"{BASE_URL}/workspaces/{WORKSPACE_ID}/semanticModels/{semantic_model_id}/updateDefinition",
            headers=headers, json=sm_definition,
        )
        resp.raise_for_status()
        if resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id")
            if op_id:
                wait_for_operation(op_id)
        print(f"  updated semantic model '{SEMANTIC_MODEL_NAME}' (id: {semantic_model_id})")
    else:
        sm_definition["displayName"] = SEMANTIC_MODEL_NAME
        resp = requests.post(
            f"{BASE_URL}/workspaces/{WORKSPACE_ID}/semanticModels",
            headers=headers, json=sm_definition,
        )
        resp.raise_for_status()
        if resp.status_code == 202:
            op_id = resp.headers.get("x-ms-operation-id")
            if op_id:
                wait_for_operation(op_id)
        resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/semanticModels", headers=headers)
        resp.raise_for_status()
        sm = next((m for m in resp.json().get("value", []) if m["displayName"] == SEMANTIC_MODEL_NAME), None)
        if not sm:
            print("  ERROR: semantic model was not created. Check the Fabric workspace for details.")
            return
        semantic_model_id = sm["id"]
        print(f"  created semantic model '{SEMANTIC_MODEL_NAME}' (id: {semantic_model_id})")

    refresh_semantic_model(semantic_model_id)


# --- Run selected steps ---
print(f"\nSteps: {', '.join(s for s in ALL_STEPS if s in STEPS)}\n")

if "files" in STEPS:
    deploy_files()

if "initial_load" in STEPS:
    print("\nInitial load: deploying notebook with download_limit=2 and running...")
    notebook_id = deploy_notebook(download_limit=2, process_limit=2)
    success = run_notebook_and_wait(notebook_id)
    if not success:
        print("  FAILED: initial load notebook run did not complete successfully")
        sys.exit(1)
    # Update notebook back to normal limits
    deploy_notebook(download_limit=DOWNLOAD_LIMIT, process_limit=PROCESS_LIMIT)
    print(f"  notebook updated back to download_limit={DOWNLOAD_LIMIT}")

notebook_id = None
if "notebook" in STEPS:
    notebook_id = deploy_notebook(download_limit=DOWNLOAD_LIMIT, process_limit=PROCESS_LIMIT)

if "semantic_model" in STEPS:
    deploy_semantic_model()

if "pipeline" in STEPS:
    if notebook_id is None:
        resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/notebooks", headers=headers)
        resp.raise_for_status()
        nb = next((nb for nb in resp.json().get("value", []) if nb["displayName"] == NOTEBOOK_NAME), None)
        if nb:
            notebook_id = nb["id"]
        else:
            print(f"  ERROR: notebook '{NOTEBOOK_NAME}' not found. Deploy notebook first.")
            sys.exit(1)
    pipeline_id = deploy_pipeline(notebook_id)
else:
    pipeline_id = None

if "schedule" in STEPS:
    if pipeline_id is None:
        resp = requests.get(f"{BASE_URL}/workspaces/{WORKSPACE_ID}/items?type=DataPipeline", headers=headers)
        resp.raise_for_status()
        pl = next((p for p in resp.json().get("value", []) if p["displayName"] == PIPELINE_NAME), None)
        if pl:
            pipeline_id = pl["id"]
        else:
            print(f"  ERROR: pipeline '{PIPELINE_NAME}' not found. Deploy pipeline first.")
            sys.exit(1)
    deploy_schedule(pipeline_id)

print("\nDeploy complete.")
