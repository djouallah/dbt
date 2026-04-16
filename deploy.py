import argparse
import base64
import json
import re
import subprocess
import time
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

# Derive item names from fabric_items/ folder names
fabric_items = root / "fabric_items"
def find_item(item_type):
    matches = list(fabric_items.glob(f"*.{item_type}"))
    if len(matches) != 1:
        raise SystemExit(f"Expected exactly one {item_type} in fabric_items/, found {len(matches)}")
    return matches[0].name.removesuffix(f".{item_type}")

LH_NAME  = cfg["lakehouse_name"]
NB_NAME  = find_item("Notebook")
PL_NAME  = find_item("DataPipeline")
SM_NAME  = find_item("SemanticModel")

# Resolve workspace name from ID
result = subprocess.run(
    ["fab", "api", "-X", "get", f"workspaces/{WS_ID}"],
    capture_output=True, text=True, check=True, cwd=str(root),
)
ws = json.loads(result.stdout)["text"]["displayName"]
print(f"Resolved workspace: {ws} ({WS_ID})")

LAKEHOUSE = f"{ws}.Workspace/{LH_NAME}.Lakehouse"
NOTEBOOK  = f"{ws}.Workspace/{NB_NAME}.Notebook"
PIPELINE  = f"{ws}.Workspace/{PL_NAME}.DataPipeline"


def fab(args, cwd=root):
    subprocess.run(["fab"] + args, check=True, cwd=str(cwd))


# Extract source workspace_id and lakehouse_id from the bim file OneLake URL
bim_path = root / "fabric_items" / f"{SM_NAME}.SemanticModel" / "model.bim"
bim_text = bim_path.read_text()
url_match = re.search(r'onelake\.dfs\.fabric\.microsoft\.com/([0-9a-f-]{36})/([0-9a-f-]{36})', bim_text)
if not url_match:
    raise SystemExit("Could not find OneLake URL with workspace/lakehouse GUIDs in model.bim")
source_ws_id = url_match.group(1)
source_lh_id = url_match.group(2)
print(f"Source workspace ID: {source_ws_id}")
print(f"Source lakehouse ID: {source_lh_id}")



def get_item_id(path):
    """Get an item's ID using fab get -q id."""
    r = subprocess.run(["fab", "get", path, "-q", "id"],
                       capture_output=True, text=True, check=True, cwd=str(root))
    return r.stdout.strip()


def fab_deploy(item_types):
    """Write a temporary fab deploy config and run deploy, then clean up."""
    content = (
        'core:\n'
        f'  workspace: "{ws}"\n'
        '  repository_directory: "./fabric_items"\n'
        '  item_types_in_scope:\n'
    )
    for t in item_types:
        content += f'    - {t}\n'
    tmp = root / "_fab_deploy_tmp.yml"
    tmp.write_text(content)
    try:
        fab(["deploy", "--config", tmp.name, "-f"])
    finally:
        tmp.unlink(missing_ok=True)


fab(["config", "set", "folder_listing_enabled", "true"])

# 1. Ensure lakehouse exists with schemas enabled
print("=== 1. Create lakehouse ===")
exists_result = subprocess.run(["fab", "exists", LAKEHOUSE],
                               capture_output=True, text=True, cwd=str(root))
if "true" not in exists_result.stdout.lower():
    fab(["create", LAKEHOUSE, "-P", "enableSchemas=true"])
    print("New lakehouse — waiting 60s for provisioning...")
    time.sleep(60)
else:
    print(f"Lakehouse '{LH_NAME}' already exists, skipping create.")

# Get target lakehouse ID (create above ensures it exists)
target_lh_id = get_item_id(LAKEHOUSE)
print(f"Target lakehouse ID: {target_lh_id}")


# Create/update variable library with deploy_config (download_limit etc.)
print("=== 1b. Create variable library ===")
VL_NAME = "deploy_config"

def b64(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()

vl_variables = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/variables/1.0.0/schema.json",
    "variables": [
        {"name": "download_limit", "type": "String", "value": cfg["download_limit"]},
        {"name": "process_limit",   "type": "String", "value": cfg["process_limit"]},
        {"name": "lakehouse_name",  "type": "String", "value": cfg["lakehouse_name"]},
        {"name": "workspace_id",    "type": "String", "value": cfg["ws_id"]},
        {"name": "dbt_path",        "type": "String", "value": cfg["dbt_path"]},
        {"name": "metadata_path",   "type": "String", "value": cfg["metadata_path"]},
    ],
}
vl_settings = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/settings/1.0.0/schema.json",
    "valueSetsOrder": [],
}
vl_definition = {
    "format": "VariableLibraryV1",
    "parts": [
        {"path": "variables.json", "payload": b64(vl_variables), "payloadType": "InlineBase64"},
        {"path": "settings.json", "payload": b64(vl_settings), "payloadType": "InlineBase64"},
    ],
}

# Check if variable library already exists
VL_PATH = f"{ws}.Workspace/{VL_NAME}.VariableLibrary"
vl_exists = subprocess.run(["fab", "exists", VL_PATH],
                           capture_output=True, text=True, cwd=str(root))

if "true" in vl_exists.stdout.lower():
    # Update definition
    vl_id = get_item_id(VL_PATH)
    fab(["api", "-X", "post",
         f"workspaces/{WS_ID}/variableLibraries/{vl_id}/updateDefinition",
         "-i", json.dumps({"definition": vl_definition})])
    print(f"Updated variable library '{VL_NAME}'")
else:
    fab(["api", "-X", "post", f"workspaces/{WS_ID}/variableLibraries",
         "-i", json.dumps({"displayName": VL_NAME, "definition": vl_definition})])
    print(f"Created variable library '{VL_NAME}'")

# 2. Deploy notebook (no parameters — code uses notebookutils at runtime)
print("=== 2b. Deploy notebook ===")
fab_deploy(["Notebook"])

# 2c. Attach lakehouse to notebook via fab set
print("=== 2c. Attach lakehouse to notebook ===")
lakehouse_payload = json.dumps({
    "known_lakehouses": [{"id": target_lh_id}],
    "default_lakehouse": target_lh_id,
    "default_lakehouse_name": LH_NAME,
    "default_lakehouse_workspace_id": WS_ID,
})
fab(["set", NOTEBOOK, "-q", "lakehouse", "-i", lakehouse_payload, "-f"])

# Get target notebook ID (needed for pipeline fab set later)
target_nb_id = get_item_id(NOTEBOOK)
print(f"Target notebook ID:  {target_nb_id}")

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

# 5. Deploy semantic model (replace GUIDs in bim, deploy, restore)
print("=== 5. Deploy semantic model ===")
bim_path.write_text(bim_text.replace(source_ws_id, WS_ID).replace(source_lh_id, target_lh_id))
try:
    fab_deploy(["SemanticModel"])
finally:
    subprocess.run(["git", "checkout", str(bim_path)], cwd=str(root))

# 6. Refresh semantic model
print("=== 6. Refresh semantic model ===")
SEMANTIC_MODEL = f"{ws}.Workspace/{SM_NAME}.SemanticModel"
sm_id = get_item_id(SEMANTIC_MODEL)
fab(["api", "-A", "powerbi", "-X", "post", f"groups/{WS_ID}/datasets/{sm_id}/refreshes"])

# 7. Deploy DataPipeline + set notebook reference + schedule
print("=== 7. Deploy pipeline ===")
fab_deploy(["DataPipeline"])

# 7b. Set notebook reference on pipeline via fab set
print("=== 7b. Set notebook on pipeline ===")
fab(["set", PIPELINE, "-q",
     "definition.parts[0].payload.properties.activities[0].typeProperties.notebookId",
     "-i", target_nb_id, "-f"])
fab(["set", PIPELINE, "-q",
     "definition.parts[0].payload.properties.activities[0].typeProperties.workspaceId",
     "-i", WS_ID, "-f"])

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


print("=== Deploy complete ===")
