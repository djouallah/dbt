"""Resolve a Fabric item's OneLake Tables path for the dbt `duckrun` target's root_path — which
addresses items by GUID, not display name. Prints a line for $GITHUB_ENV.

Usage: python item_path.py <item_name> <lakehouses|warehouses>
  -> ONELAKE_TABLES_PATH=abfss://<ws_id>@onelake.dfs.fabric.microsoft.com/<item_id>/Tables
"""
import os
import sys
import subprocess

import requests

ws = os.environ["WS_ID"]
name, kind = sys.argv[1], sys.argv[2]
FAB = "https://api.fabric.microsoft.com/v1"


def fabric_token():
    try:
        from duckrun.auth import get_fabric_token
        return get_fabric_token()
    except Exception:
        return subprocess.check_output(
            ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
             "--query", "accessToken", "-o", "tsv"], text=True).strip()


r = requests.get(f"{FAB}/workspaces/{ws}/{kind}",
                 headers={"Authorization": "Bearer " + fabric_token()})
r.raise_for_status()
guid = next(i["id"] for i in r.json().get("value", []) if i["displayName"] == name)
print(f"ONELAKE_TABLES_PATH=abfss://{ws}@onelake.dfs.fabric.microsoft.com/{guid}/Tables")
