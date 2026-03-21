import argparse
import json
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--env", default="prod")
args = parser.parse_args()

root       = Path(__file__).parent
all_cfg    = yaml.safe_load((root / "deploy_config.yml").read_text())
if args.env not in all_cfg:
    raise SystemExit(f"No '{args.env}' section in deploy_config.yml. Add it for this branch.")
cfg        = {**all_cfg.get("defaults", {}), **all_cfg[args.env]}
WS_ID     = cfg["ws_id"]
dbt        = root / "dbt"
SM_NAME   = cfg["sm_name"]

# Resolve workspace name from ID
result = subprocess.run(
    ["fab", "api", "-X", "get", f"workspaces/{WS_ID}"],
    capture_output=True, text=True, check=True, cwd=str(root),
)
ws = json.loads(result.stdout)["text"]["displayName"]
print(f"Resolved workspace: {ws} ({WS_ID})")

LAKEHOUSE = f"{ws}.Workspace/{cfg['lakehouse']}.Lakehouse"
NOTEBOOK  = f"{ws}.Workspace/{cfg['notebook']}.Notebook"
PIPELINE  = f"{ws}.Workspace/{cfg['pipeline']}.DataPipeline"


def fab(args, cwd=root):
    subprocess.run(["fab"] + args, check=True, cwd=str(cwd))


import re
PARAM_FILE = root / "parameter.yml"

# Extract source workspace_id and lakehouse_id from the bim file OneLake URL
bim_text = (root / "fabric_items" / f"{SM_NAME}.SemanticModel" / "model.bim").read_text()
url_match = re.search(r'onelake\.dfs\.fabric\.microsoft\.com/([0-9a-f-]{36})/([0-9a-f-]{36})', bim_text)
if not url_match:
    raise SystemExit("Could not find OneLake URL with workspace/lakehouse GUIDs in model.bim")
source_ws_id = url_match.group(1)
source_lh_id = url_match.group(2)
print(f"Source workspace ID: {source_ws_id}")
print(f"Source lakehouse ID: {source_lh_id}")

# Extract source notebook_id from .platform logicalId (used in pipeline-content.json)
source_nb_id = json.loads(
    (root / "fabric_items" / f"{cfg['notebook']}.Notebook" / ".platform").read_text()
)["config"]["logicalId"]
print(f"Source notebook ID:  {source_nb_id}")


def get_target_item_id(item_type, display_name):
    """Query target workspace for an item's ID by type and name."""
    r = subprocess.run(
        ["fab", "api", "-X", "get", f"workspaces/{WS_ID}/items?type={item_type}"],
        capture_output=True, text=True, check=True, cwd=str(root),
    )
    return next(i["id"] for i in json.loads(r.stdout)["text"]["value"]
                if i["displayName"] == display_name)


def write_parameter_yml(entries):
    """Write parameter.yml with the given find_replace entries."""
    PARAM_FILE.write_text(yaml.dump({"find_replace": entries}, default_flow_style=False))
    print(f"Wrote parameter.yml with {len(entries)} entries")


def fab_deploy(item_types, use_parameters=True):
    """Write a temporary fab deploy config and run deploy, then clean up."""
    content = (
        'core:\n'
        f'  workspace: "{ws}"\n'
        '  repository_directory: "./fabric_items"\n'
    )
    if use_parameters:
        content += f'  parameter: "./{PARAM_FILE.name}"\n'
    content += '  item_types_in_scope:\n'
    for t in item_types:
        content += f'    - {t}\n'
    tmp = root / "_fab_deploy_tmp.yml"
    tmp.write_text(content)
    try:
        cmd = ["deploy", "--config", tmp.name, "-f"]
        if use_parameters:
            cmd += ["--target_env", ws]
        fab(cmd)
    finally:
        tmp.unlink(missing_ok=True)


# 1. Ensure lakehouse exists with schemas enabled
print("=== 1. Create lakehouse ===")
subprocess.run(["fab", "create", LAKEHOUSE, "-P", "enableSchemas=true"], cwd=str(root))

# Get target lakehouse ID (create above ensures it exists)
target_lh_id = get_target_item_id("Lakehouse", cfg["lakehouse"])
print(f"Target lakehouse ID: {target_lh_id}")

# Build parameter.yml v1: workspace + lakehouse + download_limit
param_entries = [
    {"find_value": source_ws_id, "replace_value": {"_ALL_": "$workspace.id"}},
    {"find_value": source_lh_id, "replace_value": {"_ALL_": target_lh_id}},
]
default_dl = all_cfg.get("defaults", {}).get("download_limit", "2")
target_dl = cfg.get("download_limit", default_dl)
if default_dl != target_dl:
    param_entries.append({
        "find_value": f"download_limit']      = '{default_dl}'",
        "replace_value": {"_ALL_": f"download_limit']      = '{target_dl}'"},
    })
write_parameter_yml(param_entries)

# 2a. Deploy lakehouse (no parameters needed)
print("=== 2a. Deploy lakehouse ===")
fab_deploy(["Lakehouse"], use_parameters=False)

# 2b. Deploy notebook (uses parameter.yml v1: ws_id + lakehouse_id + download_limit)
print("=== 2b. Deploy notebook ===")
fab_deploy(["Notebook"])

# 2c. Attach lakehouse to notebook
print("=== 2c. Attach lakehouse to notebook ===")
lakehouse_payload = json.dumps({
    "known_lakehouses": [{"id": target_lh_id}],
    "default_lakehouse": target_lh_id,
    "default_lakehouse_name": cfg["lakehouse"],
    "default_lakehouse_workspace_id": WS_ID,
})
fab(["set", NOTEBOOK, "-q",
     "definition.parts[0].payload.metadata.dependencies.lakehouse",
     "-i", lakehouse_payload])

# Get target notebook ID (now exists after deploy) and rebuild parameter.yml with it
target_nb_id = get_target_item_id("Notebook", cfg["notebook"])
print(f"Target notebook ID:  {target_nb_id}")
param_entries.append({"find_value": source_nb_id, "replace_value": {"_ALL_": target_nb_id}})
write_parameter_yml(param_entries)

# 3. Copy dbt files to OneLake
print("=== 3. Copy dbt files to OneLake ===")
dirs = set()
for f in dbt.rglob("*"):
    if f.is_file():
        p = f.relative_to(root).parent
        while p.parts:
            dirs.add(p.as_posix())
            p = p.parent

for d in sorted(dirs):
    subprocess.run(["fab", "mkdir", f"{LAKEHOUSE}/Files/{d}"], cwd=str(root))

files = [f for f in dbt.rglob("*") if f.is_file()]

def copy_file(f):
    rel = f.relative_to(root)
    fab(["cp", rel.as_posix(), f"{LAKEHOUSE}/Files/{rel.parent.as_posix()}/", "-f"])

with ThreadPoolExecutor(max_workers=8) as executor:
    executor.map(copy_file, files)

# 4. Run notebook (blocks until dbt finishes and Delta tables are created)
print("=== 4. Run notebook ===")
fab(["job", "run", NOTEBOOK, "-i", "{}"])

# 5. Deploy semantic model
print("=== 5. Deploy semantic model ===")
fab_deploy(["SemanticModel"])

# 6. Refresh semantic model
print("=== 6. Refresh semantic model ===")
sm_id = get_target_item_id("SemanticModel", SM_NAME)
fab(["api", "-A", "powerbi", "-X", "post", f"groups/{WS_ID}/datasets/{sm_id}/refreshes"])

# 7. Deploy DataPipeline + schedule
print("=== 7. Deploy pipeline + schedule ===")
fab_deploy(["DataPipeline"])

result = subprocess.run(["fab", "job", "run-list", PIPELINE, "--schedule"],
                        capture_output=True, text=True, cwd=str(root))
print(f"run-list stdout: {result.stdout!r}")
print(f"run-list stderr: {result.stderr!r}")
print(f"run-list returncode: {result.returncode}")
has_active_schedule = "True" in result.stdout

if has_active_schedule:
    print("Pipeline already scheduled and enabled, skipping.")
else:
    fab(["job", "run-sch", PIPELINE,
         "--type", "cron", "--interval", cfg["schedule_interval"],
         "--start", cfg["schedule_start"], "--end", cfg["schedule_end"], "--enable"])

PARAM_FILE.unlink(missing_ok=True)
print("=== Deploy complete ===")
