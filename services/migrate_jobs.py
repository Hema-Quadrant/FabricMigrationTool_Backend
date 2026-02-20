import base64
import json
import re
import time
import uuid
import logging
import requests
from collections import defaultdict
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

from .migrate_notebooks import get_fabric_token
from .connect_databricks import get_job, export_notebook


def sanitize_name(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', '_', name).strip()[:200]


# ─── Fabric notebook helpers ───────────────────────────────

def get_existing_notebook_id(workspace_id: str, token: str, name: str) -> str | None:
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=Notebook", headers=headers, timeout=45)
    if r.status_code != 200:
        return None
    clean = name.lower().strip()
    items = r.json().get("value", [])
    for item in items:
        if item["displayName"].lower().strip() == clean:
            return item["id"]
    for item in items:
        if clean in item["displayName"].lower():
            return item["id"]
    return None


def notebook_ready(workspace_id: str, token: str, item_id: str) -> bool:
    from datetime import datetime
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{item_id}"
    try:
        if requests.get(url, headers=headers, timeout=20).status_code != 200:
            return False
        r = requests.patch(url, headers=headers, json={"description": f"touch {datetime.utcnow().isoformat()}"}, timeout=60)
        return r.status_code in (200, 204)
    except Exception:
        return False


def wait_until_ready(workspace_id: str, token: str, item_id: str, timeout: int = 600) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if notebook_ready(workspace_id, token, item_id):
            return True
        time.sleep(8)
    return False


def poll_lro(location: str, token: str, refresh_fn, max_sec: int = 1200) -> dict:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    start = time.time()
    while time.time() - start < max_sec:
        time.sleep(10)
        if (time.time() - start) % 300 < 12:
            headers["Authorization"] = f"Bearer {refresh_fn()}"
        try:
            r = requests.get(location, headers=headers, timeout=30)
            if r.status_code == 200:
                data = r.json()
                status = data.get("status")
                if status in ("Succeeded", "Completed", None):
                    final = requests.get(r.headers.get("Location") or location, headers=headers, timeout=30)
                    final.raise_for_status()
                    return final.json()
                if status in ("Failed", "Cancelled", "Error"):
                    raise RuntimeError(f"LRO failed: {json.dumps(data)}")
        except requests.HTTPError as e:
            if e.response and e.response.status_code == 429:
                time.sleep(int(e.response.headers.get("Retry-After", 30)) + 5)
    raise TimeoutError(f"LRO timed out after {max_sec}s")


def create_fabric_notebook(workspace_id: str, token: str, name: str, content_b64: str, refresh_fn, run_id: str) -> str:
    name = sanitize_name(name)
    logging.info(f"[RUN:{run_id}] Creating notebook in Fabric: {name}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "displayName": name, "type": "Notebook",
        "definition": {"format": "ipynb", "parts": [{"path": "notebook-content.ipynb", "payloadType": "InlineBase64", "payload": content_b64}]}
    }
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    for attempt in range(1, 4):
        headers["Authorization"] = f"Bearer {refresh_fn()}"
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=180)
            if r.status_code in (200, 201):
                item_id = r.json()["id"]
            elif r.status_code == 202:
                item_id = poll_lro(r.headers["Location"], token, refresh_fn).get("id")
            else:
                r.raise_for_status()
            logging.info(f"[RUN:{run_id}] Notebook created: {name} (ID: {item_id}), waiting until ready...")
            if wait_until_ready(workspace_id, token, item_id):
                logging.info(f"[RUN:{run_id}] Notebook ready: {name}")
                return item_id
            raise RuntimeError(f"Notebook not ready: {item_id}")
        except Exception as e:
            logging.warning(f"[RUN:{run_id}] Notebook create attempt {attempt} failed: {str(e)}")
            if attempt == 3:
                raise
            time.sleep(10 * attempt)
    raise RuntimeError("Failed after 3 attempts")


def create_fabric_pipeline(workspace_id: str, token: str, payload: dict, refresh_fn, run_id: str) -> str:
    pipeline_name = payload.get("displayName", "unknown")
    logging.info(f"[RUN:{run_id}] Creating pipeline: {pipeline_name}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    for attempt in range(1, 4):
        headers["Authorization"] = f"Bearer {refresh_fn()}"
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=180)
            if r.status_code in (200, 201):
                pid = r.json()["id"]
                logging.info(f"[RUN:{run_id}] Pipeline created: {pipeline_name} (ID: {pid})")
                return pid
            if r.status_code == 202:
                item_id = poll_lro(r.headers["Location"], token, refresh_fn).get("id")
                if item_id:
                    logging.info(f"[RUN:{run_id}] Pipeline created via LRO: {pipeline_name} (ID: {item_id})")
                    return item_id
                raise ValueError("Pipeline LRO missing id")
            r.raise_for_status()
        except Exception as e:
            logging.warning(f"[RUN:{run_id}] Pipeline create attempt {attempt} failed: {str(e)}")
            if attempt == 3:
                raise
            time.sleep(12 * attempt)
    raise RuntimeError("Failed after 3 attempts")


def get_existing_pipeline_id(workspace_id: str, token: str, name: str) -> str | None:
    r = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=DataPipeline",
        headers={"Authorization": f"Bearer {token}"}, timeout=60
    )
    if r.status_code == 200:
        for item in r.json().get("value", []):
            if item["displayName"] == name:
                return item["id"]
    return None


# ─── Condition / expression helpers ────────────────────────

def get_adf_operand(operand) -> str:
    if operand is None:
        return "''"
    if isinstance(operand, bool):
        return str(operand).lower()
    if isinstance(operand, (int, float)):
        return str(operand)
    if isinstance(operand, (list, dict)):
        return f"'{json.dumps(operand)}'"
    operand = str(operand).strip()
    if operand.startswith("@job.parameters."):
        return f"pipeline().parameters.{operand[len('@job.parameters.'):]}"
    if operand.startswith("job.parameters."):
        return f"pipeline().parameters.{operand[len('job.parameters.'):]}"
    if operand.startswith("{{job.parameters.") and operand.endswith("}}"):
        inner = operand[2:-2].strip()
        return f"pipeline().parameters.{inner[len('job.parameters.'):]}"
    if operand.startswith("{{") and operand.endswith("}}"):
        inner = operand[2:-2].strip()
        m = re.match(r"tasks\.([^.]+)\.(values|output)\.(.+)", inner)
        if m:
            task, section, field = m.groups()
            return f"activity('{task}').output.{section}.{field}"
        m2 = re.match(r"tasks\.([^.]+)\.(.+)", inner)
        if m2:
            task, field = m2.groups()
            return f"activity('{task}').output.result.exitValue.{field}"
        return f"@{inner}"
    if operand.lower() in ("true", "false"):
        return operand.lower()
    try:
        float(operand)
        return operand
    except ValueError:
        pass
    return f"'{operand}'"


def get_condition_expression(condition: dict) -> str:
    op_map = {
        "EQUAL": "equals", "EQUAL_TO": "equals", "NOT_EQUAL": "notEquals",
        "GREATER_THAN": "greater", "GREATER_THAN_OR_EQUAL": "greaterOrEquals",
        "LESS_THAN": "less", "LESS_THAN_OR_EQUAL": "lessOrEquals",
    }
    fn = op_map.get(condition.get("op", "").upper(), "equals")
    return f"@{fn}({get_adf_operand(condition.get('left', ''))}, {get_adf_operand(condition.get('right', ''))})"


def translate_condition(cond_str: str) -> str:
    if not cond_str:
        return "@true"
    cond = cond_str.strip()
    cond = re.sub(r'\{\{job\.parameters\.([^}]+)\}\}', r'pipeline().parameters.\1', cond)
    cond = re.sub(r'@job\.parameters\.([A-Za-z_][A-Za-z0-9_]*)', r'pipeline().parameters.\1', cond)
    cond = re.sub(r'\bjob\.parameters\.([A-Za-z_][A-Za-z0-9_]*)\b', r'pipeline().parameters.\1', cond)
    for op, fn in sorted({'>=': "@greaterOrEquals", '<=': "@lessOrEquals", '==': "@equals", '=': "@equals", '>': "@greater", '<': "@less"}.items(), key=lambda x: -len(x[0])):
        if op in cond:
            left, right = map(str.strip, cond.split(op, 1))
            return f"{fn}({left}, {right})"
    return f"@{cond}"


def infer_param_type(value) -> str:
    if value is None: return "String"
    if isinstance(value, bool): return "Bool"
    if isinstance(value, list): return "Array"
    if isinstance(value, int): return "Int"
    if isinstance(value, float): return "Float"
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("true", "false"): return "Bool"
        if re.fullmatch(r"-?\d+", v): return "Int"
        if re.fullmatch(r"-?\d+\.\d+", v): return "Float"
    return "String"


def extract_parameters(job_settings: dict) -> dict:
    params = {}
    for p in job_settings.get("parameters", []):
        name, default = p.get("name"), p.get("default")
        if not name:
            continue
        if isinstance(default, str):
            v = default.strip()
            if v.startswith("[") and v.endswith("]"):
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, list):
                        params[name] = {"type": "Array", "defaultValue": parsed}
                        continue
                except Exception:
                    pass
        params[name] = {"type": infer_param_type(default), "defaultValue": default}
    return params


def get_depends_on(depends_on: list, ignore: set = None) -> list:
    ignore = ignore or set()
    deps_map = defaultdict(set)
    for d in depends_on:
        tk = d.get("task_key")
        if not tk or tk in ignore:
            continue
        o = (d.get("outcome") or d.get("run_if") or "ALL_SUCCESS").upper()
        if any(x in o for x in ["FAIL", "FALSE"]):
            deps_map[tk].add("Failed")
        elif any(x in o for x in ["ALL_DONE", "COMPLETED"]):
            deps_map[tk].add("Completed")
        else:
            deps_map[tk].add("Succeeded")
    return [{"activity": tk, "dependencyConditions": list(c)} for tk, c in deps_map.items()]


def collect_notebooks(tasks: list, nbs: set):
    for task in tasks:
        if "notebook_task" in task:
            nbs.add(task["notebook_task"]["notebook_path"])
        elif "for_each_task" in task and task["for_each_task"].get("task"):
            collect_notebooks([task["for_each_task"]["task"]], nbs)
        elif "switch_task" in task:
            for c in task["switch_task"].get("cases", []):
                if c.get("task"):
                    collect_notebooks([c["task"]], nbs)
            if task["switch_task"].get("default"):
                collect_notebooks([task["switch_task"]["default"]], nbs)


def build_activities(keys, visited, task_dict, successors, nb_map, workspace_id, ignore=None, run_id="") -> list:
    ignore = ignore or set()
    acts = []
    for key in keys:
        if key in visited:
            continue
        visited.add(key)
        task = task_dict.get(key)
        if not task:
            continue
        deps = get_depends_on(task.get("depends_on", []), ignore)
        logging.info(f"[RUN:{run_id}] Building activity: {key} (type: {'condition' if 'condition_task' in task else 'notebook' if 'notebook_task' in task else 'foreach' if 'for_each_task' in task else 'switch' if 'switch_task' in task else 'unknown'})")

        if "condition_task" in task or "condition" in task:
            expr = get_condition_expression(task["condition_task"]) if "condition_task" in task else translate_condition(task.get("condition", "true"))
            succ = successors.get(key, [])
            branch_map = defaultdict(set)
            for s, outcome in succ:
                o = (outcome or "").upper()
                if o in ("TRUE", "CONDITION_TRUE"): branch_map[s].add("true")
                elif o in ("FALSE", "CONDITION_FALSE"): branch_map[s].add("false")
            true_keys, false_keys, shared_keys = [], [], []
            for tk in set(s for s, _ in succ):
                b = branch_map[tk]
                if ("true" in b and "false" in b) or not b: shared_keys.append(tk)
                elif "true" in b: true_keys.append(tk)
                else: false_keys.append(tk)
            logging.info(f"[RUN:{run_id}] IfCondition '{key}': true={true_keys}, false={false_keys}, shared={shared_keys}")
            acts.append({
                "name": key, "type": "IfCondition", "dependsOn": deps,
                "typeProperties": {
                    "expression": {"value": expr, "type": "Expression"},
                    "ifTrueActivities": build_activities(true_keys, visited.copy(), task_dict, successors, nb_map, workspace_id, ignore | {key}, run_id),
                    "ifFalseActivities": build_activities(false_keys, visited.copy(), task_dict, successors, nb_map, workspace_id, ignore | {key}, run_id)
                }
            })
            acts.extend(build_activities(shared_keys, visited.copy(), task_dict, successors, nb_map, workspace_id, ignore, run_id))

        elif "notebook_task" in task:
            nb_id = nb_map.get(task["notebook_task"]["notebook_path"])
            if not nb_id:
                logging.warning(f"[RUN:{run_id}] No Fabric notebook ID found for path: {task['notebook_task']['notebook_path']} — skipping activity '{key}'")
                continue
            params = {}
            for k, v in task["notebook_task"].get("base_parameters", {}).items():
                val = get_adf_operand(v)
                params[k] = {"value": val, "type": "Expression" if val.startswith("@") else "String"}
            act = {"name": key, "type": "TridentNotebook", "dependsOn": deps, "policy": {"timeout": "1.00:00:00", "retry": 1, "retryIntervalInSeconds": 30}, "typeProperties": {"notebookId": nb_id, "workspaceId": workspace_id}}
            if params:
                act["typeProperties"]["parameters"] = params
            logging.info(f"[RUN:{run_id}] Notebook activity '{key}' built with {len(params)} parameter(s)")
            acts.append(act)
            acts += build_activities([s for s, _ in successors.get(key, [])], visited, task_dict, successors, nb_map, workspace_id, ignore, run_id)

        elif "for_each_task" in task:
            inputs_expr = translate_condition(task["for_each_task"].get("inputs", "[]")).lstrip("@")
            nested = task["for_each_task"].get("task", {})
            nk = nested.get("task_key", f"{key}_inner")
            inner_acts = build_activities([nk], visited.copy(), {nk: nested}, successors, nb_map, workspace_id, ignore | {key}, run_id)
            for ia in inner_acts:
                for p in ia.get("typeProperties", {}).get("parameters", {}).values():
                    if isinstance(p.get("value"), str) and "{{input}}" in p["value"]:
                        p["value"] = p["value"].replace("{{input}}", "@item()")
                        p["type"] = "Expression"
            concurrency = task["for_each_task"].get("concurrency", 0)
            act = {"name": key, "type": "ForEach", "dependsOn": deps, "typeProperties": {"items": {"value": f"@{inputs_expr}", "type": "Expression"}, "isSequential": concurrency <= 1, "activities": inner_acts}}
            if concurrency > 1:
                act["typeProperties"]["batchCount"] = min(concurrency, 50)
            logging.info(f"[RUN:{run_id}] ForEach activity '{key}' built with {len(inner_acts)} inner activity(s)")
            acts.append(act)
            acts += build_activities([s for s, _ in successors.get(key, [])], visited, task_dict, successors, nb_map, workspace_id, ignore, run_id)

        elif "switch_task" in task:
            switch_val = translate_condition(task["switch_task"].get("expression", "@true")).lstrip("@")
            cases = []
            for ci in task["switch_task"].get("cases", []):
                cv, ct = ci.get("value", ""), ci.get("task", {})
                ck = ct.get("task_key", f"{key}_case_{cv}")
                cases.append({"value": cv, "activities": build_activities([ck], visited.copy(), {ck: ct}, successors, nb_map, workspace_id, ignore | {key}, run_id)})
            default_acts = []
            if task["switch_task"].get("default"):
                dc = task["switch_task"]["default"]
                dk = dc.get("task_key", f"{key}_default")
                default_acts = build_activities([dk], visited.copy(), {dk: dc}, successors, nb_map, workspace_id, ignore | {key}, run_id)
            logging.info(f"[RUN:{run_id}] Switch activity '{key}' built with {len(cases)} case(s)")
            acts.append({"name": key, "type": "Switch", "dependsOn": deps, "typeProperties": {"expression": {"value": f"@{switch_val}", "type": "Expression"}, "cases": cases, "defaultActivities": default_acts}})
            acts += build_activities([s for s, _ in successors.get(key, [])], visited, task_dict, successors, nb_map, workspace_id, ignore, run_id)

        else:
            logging.warning(f"[RUN:{run_id}] Unsupported task type for '{key}' — skipping")
            acts += build_activities([s for s, _ in successors.get(key, [])], visited, task_dict, successors, nb_map, workspace_id, ignore, run_id)

    return acts


# ─── Main function ──────────────────────────────────────────

def migrate_jobs(
    tenant_id: str, client_id: str, client_secret: str,
    workspace_id: str, db_url: str, pat: str,
    job_ids: list
) -> dict:
    def refresh_token():
        return get_fabric_token(tenant_id, client_id, client_secret)

    token = refresh_token()
    results = {"created": [], "failed": [], "already_exist": []}

    logging.info(f"Starting job migration for {len(job_ids)} job(s): {job_ids}")

    for job_id in job_ids:
        run_id = str(uuid.uuid4())
        logging.info(f"[RUN:{run_id}] Starting migration for job ID: {job_id}")
        try:
            job_settings = get_job(db_url, pat, job_id)
            job_name = job_settings.get("name", f"Job_{job_id}")
            logging.info(f"[RUN:{run_id}] Job name: '{job_name}', tasks: {len(job_settings.get('tasks', []))}")

            # Migrate notebooks
            all_nbs = set()
            collect_notebooks(job_settings.get("tasks", []), all_nbs)
            logging.info(f"[RUN:{run_id}] Found {len(all_nbs)} notebook(s) to migrate: {list(all_nbs)}")

            nb_map = {}
            for path in all_nbs:
                name = sanitize_name(path.split("/")[-1])
                logging.info(f"[RUN:{run_id}] Checking notebook: {name} ({path})")
                nb_id = get_existing_notebook_id(workspace_id, token, name)
                if not nb_id:
                    try:
                        logging.info(f"[RUN:{run_id}] Exporting notebook from Databricks: {path}")
                        content_b64 = base64.b64encode(export_notebook(db_url, pat, path)).decode()
                        nb_id = create_fabric_notebook(workspace_id, token, name, content_b64, refresh_token, run_id)
                    except Exception as e:
                        logging.error(f"[RUN:{run_id}] Failed to migrate notebook '{path}': {str(e)}", exc_info=True)
                        results["failed"].append({"job_id": job_id, "path": path, "error": str(e), "run_id": run_id})
                        continue
                else:
                    logging.info(f"[RUN:{run_id}] Notebook already exists in Fabric: {name} (ID: {nb_id})")

                if not wait_until_ready(workspace_id, token, nb_id):
                    logging.error(f"[RUN:{run_id}] Notebook not ready after timeout: {name}")
                    results["failed"].append({"job_id": job_id, "path": path, "error": "Notebook not ready", "run_id": run_id})
                    continue
                nb_map[path] = nb_id
                logging.info(f"[RUN:{run_id}] Notebook mapped: {path} → {nb_id}")

            # Build activities
            logging.info(f"[RUN:{run_id}] Building pipeline activities...")
            task_dict = {t["task_key"]: t for t in job_settings.get("tasks", []) if "task_key" in t}
            successors = defaultdict(list)
            for t in task_dict.values():
                for d in t.get("depends_on", []):
                    outcome = d.get("outcome") or d.get("run_if") or "ALL_SUCCESS"
                    successors[d["task_key"]].append((t["task_key"], outcome))

            roots = list(set(task_dict.keys()) - {tgt for deps in successors.values() for tgt, _ in deps})
            logging.info(f"[RUN:{run_id}] Root tasks: {roots}")
            activities = build_activities(roots, set(), task_dict, successors, nb_map, workspace_id, run_id=run_id)
            logging.info(f"[RUN:{run_id}] Built {len(activities)} top-level activity(s)")

            if not activities:
                logging.error(f"[RUN:{run_id}] No activities built for job '{job_name}'")
                results["failed"].append({"job_id": job_id, "error": "No activities built", "run_id": run_id})
                continue

            # Check for existing pipeline
            pipeline_name = sanitize_name(job_name)
            existing_pid = get_existing_pipeline_id(workspace_id, token, pipeline_name)
            if existing_pid:
                logging.info(f"[RUN:{run_id}] Pipeline already exists: '{pipeline_name}' (ID: {existing_pid})")
                results["already_exist"].append({"job_id": job_id, "name": job_name, "fabric_pipeline_id": existing_pid, "run_id": run_id})
                continue

            # Create pipeline
            logging.info(f"[RUN:{run_id}] Creating pipeline: '{pipeline_name}'")
            inner = {"properties": {"activities": activities, "parameters": extract_parameters(job_settings), "variables": {}, "annotations": [], "description": f"Migrated from Databricks job '{job_name}' (ID: {job_id})"}}
            payload = {
                "displayName": pipeline_name, "type": "DataPipeline",
                "description": f"Migrated Databricks job {job_id}",
                "definition": {"parts": [{"path": "pipeline-content.json", "payloadType": "InlineBase64", "payload": base64.b64encode(json.dumps(inner).encode()).decode()}]}
            }
            pid = create_fabric_pipeline(workspace_id, token, payload, refresh_token, run_id)
            logging.info(f"[RUN:{run_id}] Job '{job_name}' migrated successfully → pipeline ID: {pid}")
            results["created"].append({"job_id": job_id, "name": job_name, "fabric_pipeline_id": pid, "run_id": run_id})
            time.sleep(3)

        except Exception as e:
            logging.error(f"[RUN:{run_id}] Failed to migrate job '{job_id}': {str(e)}", exc_info=True)
            results["failed"].append({"job_id": job_id, "error": str(e), "run_id": run_id})

    created_cnt, exists_cnt, failed_cnt = len(results["created"]), len(results["already_exist"]), len(results["failed"])
    overall = "success" if failed_cnt == 0 else "partial" if created_cnt + exists_cnt > 0 else "failed"
    logging.info(f"Job migration complete — status: {overall}, created: {created_cnt}, already_exist: {exists_cnt}, failed: {failed_cnt}, total: {len(job_ids)}")

    return {
        "status": overall,
        "created": created_cnt,
        "already_exist": exists_cnt,
        "failed": failed_cnt,
        "total": len(job_ids),
        "created_jobs": results["created"],
        "already_exist_jobs": results["already_exist"],
        "failed_jobs": results["failed"]
    }