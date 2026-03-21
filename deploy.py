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
config_file = "deploy_config_test.yml" if args.env == "test" else "deploy_config.yml"
cfg        = yaml.safe_load((root / config_file).read_text())
ws         = cfg["workspace"]
dbt        = root / cfg["dbt_dir"]

LAKEHOUSE = f"{ws}.Workspace/{cfg['lakehouse']}.Lakehouse"
NOTEBOOK  = f"{ws}.Workspace/{cfg['notebook']}.Notebook"
PIPELINE  = f"{ws}.Workspace/{cfg['pipeline']}.DataPipeline"
WS_ID     = cfg["ws_id"]
SM_NAME   = cfg["sm_name"]


def fab(args, cwd=root):
    subprocess.run(["fab"] + args, check=True, cwd=str(cwd))


def fab_deploy(item_types):
    """Write a temporary fab deploy config and run deploy, then clean up."""
    content = (
        'core:\n'
        f'  workspace: "{ws}"\n'
        '  repository_directory: "./fabric_items"\n'
        '  parameter: "./parameter.yml"\n'
        '  item_types_in_scope:\n'
    )
    for t in item_types:
        content += f'    - {t}\n'
    tmp = root / "_fab_deploy_tmp.yml"
    tmp.write_text(content)
    try:
        fab(["deploy", "--config", tmp.name, "--target_env", ws, "-f"])
    finally:
        tmp.unlink(missing_ok=True)


# 1. Ensure lakehouse exists with schemas enabled
print("=== 1. Create lakehouse ===")
subprocess.run(["fab", "create", LAKEHOUSE, "-P", "enableSchemas=true"], cwd=str(root))

# 2. Deploy lakehouse first so $items.Lakehouse.data.$id resolves for notebook
print("=== 2a. Deploy lakehouse ===")
fab_deploy(["Lakehouse"])

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

print("=== Deploy complete ===")
