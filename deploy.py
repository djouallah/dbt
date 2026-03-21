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


# Generate parameter.yml by scanning content files for GUIDs
import re
PARAM_TMP = root / "_parameter_tmp.yml"
GUID_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)

# Regex all GUIDs from fabric_items/
content_guids = set()
for f in (root / "fabric_items").rglob("*"):
    if f.is_file():
        try:
            content_guids.update(g.lower() for g in GUID_RE.findall(f.read_text(encoding="utf-8")))
        except (UnicodeDecodeError, PermissionError):
            pass

# Source workspace GUID is in the config
source_ws_id = all_cfg["main"]["ws_id"].lower()
param_entries = []

if source_ws_id in content_guids:
    param_entries.append({"find_value": source_ws_id,
                          "replace_value": {"_ALL_": "$workspace.id"}})
    content_guids.discard(source_ws_id)

# Remaining GUIDs: one API call to list items in source workspace
r = subprocess.run(["fab", "api", "-X", "get", f"workspaces/{source_ws_id}/items"],
                   capture_output=True, text=True, check=True, cwd=str(root))
source_items = {i["id"].lower(): i for i in json.loads(r.stdout)["text"]["value"]}
for guid in content_guids:
    if guid in source_items:
        item = source_items[guid]
        token = f"$items.{item['type']}.{item['displayName']}.$id"
        param_entries.append({"find_value": guid, "replace_value": {"_ALL_": token}})

# Env-specific: download_limit
default_dl = all_cfg.get("defaults", {}).get("download_limit", "2")
param_entries.append({
    "find_value": f"download_limit']      = '{default_dl}'",
    "replace_value": {"_ALL_": f"download_limit']      = '{cfg['download_limit']}'"}
})

PARAM_TMP.write_text(yaml.dump({"find_replace": param_entries}, default_flow_style=False))
print(f"Generated parameter.yml with {len(param_entries)} entries")


def fab_deploy(item_types, use_parameters=True):
    """Write a temporary fab deploy config and run deploy, then clean up."""
    content = (
        'core:\n'
        f'  workspace: "{ws}"\n'
        '  repository_directory: "./fabric_items"\n'
    )
    if use_parameters:
        content += f'  parameter: "./{PARAM_TMP.name}"\n'
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

# 2. Deploy lakehouse first so $items.Lakehouse.data.$id resolves for notebook
print("=== 2a. Deploy lakehouse ===")
fab_deploy(["Lakehouse"], use_parameters=False)

print("=== 2b. Deploy notebook ===")
fab_deploy(["Notebook"])

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
result = subprocess.run(
    ["fab", "api", "-X", "get", f"workspaces/{WS_ID}/items?type=SemanticModel"],
    capture_output=True, text=True, cwd=str(root), check=True
)
items = json.loads(result.stdout)
sm_id = next(i["id"] for i in items["text"]["value"] if i["displayName"] == SM_NAME)
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

PARAM_TMP.unlink(missing_ok=True)
print("=== Deploy complete ===")
