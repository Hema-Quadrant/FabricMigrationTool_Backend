import uuid
import logging
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

from .migrate_notebooks import get_fabric_token


def map_cluster_to_pool(cluster: dict, run_id: str) -> dict:
    node_type = cluster.get("node_type_id", "")
    node_size = "Medium" if "D8" in node_type else "Small"
    logging.info(f"[RUN:{run_id}] Node type: {node_type} → size: {node_size}")

    autoscale = cluster.get("autoscale")
    if autoscale:
        min_nodes = autoscale.get("min_workers", 1)
        max_nodes = autoscale.get("max_workers", min_nodes)
        logging.info(f"[RUN:{run_id}] Autoscale: min={min_nodes}, max={max_nodes}")
    else:
        num_workers = cluster.get("num_workers", 1)
        if isinstance(num_workers, list):
            num_workers = len(num_workers)
        min_nodes = max_nodes = max(int(num_workers), 1)
        logging.info(f"[RUN:{run_id}] Fixed nodes: {min_nodes}")

    if cluster.get("is_single_node"):
        min_nodes = max_nodes = 1
        logging.info(f"[RUN:{run_id}] Single-node cluster, forcing nodes to 1")

    name = cluster.get("cluster_name", "cluster")
    pool_name = name if name.startswith("Mig_") else f"Mig_{name}"
    logging.info(f"[RUN:{run_id}] Pool name: {pool_name}")

    return {
        "name": pool_name,
        "nodeFamily": "MemoryOptimized",
        "nodeSize": node_size,
        "autoScale": {"enabled": True, "minNodeCount": min_nodes, "maxNodeCount": max_nodes},
        "dynamicExecutorAllocation": {"enabled": False}
    }


def ensure_workspace(fabric_cfg: dict, token: str, run_id: str) -> str:
    if fabric_cfg.get("workspaceId"):
        logging.info(f"[RUN:{run_id}] Using existing workspace: {fabric_cfg['workspaceId']}")
        return fabric_cfg["workspaceId"]
    logging.info(f"[RUN:{run_id}] Creating new workspace: {fabric_cfg.get('workspaceName')}")
    r = requests.post(
        "https://api.fabric.microsoft.com/v1/workspaces",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "displayName": fabric_cfg.get("workspaceName", "MigratedWorkspace"),
            "capacityId": fabric_cfg["capacityId"],
            "region": fabric_cfg.get("region", "westeurope")
        },
        timeout=60
    )
    r.raise_for_status()
    ws_id = r.json()["id"]
    logging.info(f"[RUN:{run_id}] Workspace created: {ws_id}")
    return ws_id


def ensure_environment(ws_id: str, env_name: str, token: str, run_id: str) -> str:
    list_url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/environments"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    r = requests.get(list_url, headers=headers, timeout=45)
    if r.status_code == 200:
        for env in r.json().get("value", []):
            if env.get("displayName") == env_name:
                logging.info(f"[RUN:{run_id}] Found existing environment: {env_name} (ID: {env['id']})")
                return env["id"]

    logging.info(f"[RUN:{run_id}] Creating environment: {env_name}")
    r = requests.post(list_url, headers=headers, json={"displayName": env_name, "description": "Migrated from Databricks"}, timeout=60)
    r.raise_for_status()
    env_id = r.json()["id"]
    logging.info(f"[RUN:{run_id}] Environment created: {env_name} (ID: {env_id})")
    return env_id


def attach_pool_to_environment(ws_id: str, env_id: str, pool_name: str, token: str, run_id: str):
    logging.info(f"[RUN:{run_id}] Attaching pool '{pool_name}' to environment {env_id}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    requests.patch(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/environments/{env_id}/staging/sparkcompute",
        headers=headers,
        json={"instancePool": {"name": pool_name, "type": "Workspace"}, "runtimeVersion": "1.3"},
        timeout=60
    ).raise_for_status()
    logging.info(f"[RUN:{run_id}] Spark compute staged, publishing...")
    requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/environments/{env_id}/staging/publish",
        headers=headers, timeout=90
    ).raise_for_status()
    logging.info(f"[RUN:{run_id}] Pool attached and published successfully")


def create_pool(ws_id: str, token: str, payload: dict, run_id: str):
    pool_name = payload["name"]
    logging.info(f"[RUN:{run_id}] Creating Spark pool: {pool_name}")
    r = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/spark/pools",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload, timeout=120
    )
    if r.status_code == 409:
        logging.warning(f"[RUN:{run_id}] Pool already exists: {pool_name}")
        raise Exception("already exists")
    r.raise_for_status()
    logging.info(f"[RUN:{run_id}] Pool created: {pool_name}")


def migrate_clusters(fabric_cfg: dict, db_url: str, pat: str, cluster_ids: list) -> dict:
    from connect_databricks import get_cluster

    run_id = str(uuid.uuid4())
    logging.info(f"[RUN:{run_id}] Starting cluster migration for {len(cluster_ids)} cluster(s)")

    token = get_fabric_token(fabric_cfg["tenantId"], fabric_cfg["clientId"], fabric_cfg["clientSecret"])
    ws_id = ensure_workspace(fabric_cfg, token, run_id)

    success_list, failed_list = [], []

    for cluster_id in cluster_ids:
        c_run_id = str(uuid.uuid4())
        cluster_name = cluster_id
        logging.info(f"[RUN:{c_run_id}] Processing cluster: {cluster_id}")
        try:
            cluster = get_cluster(db_url, pat, cluster_id)
            cluster_name = cluster.get("cluster_name", cluster_id)
            logging.info(f"[RUN:{c_run_id}] Cluster name resolved: {cluster_name}")

            pool_payload = map_cluster_to_pool(cluster, c_run_id)
            pool_name = pool_payload["name"]
            create_pool(ws_id, token, pool_payload, c_run_id)

            env_id = ensure_environment(ws_id, f"Env_{pool_name}", token, c_run_id)
            attach_pool_to_environment(ws_id, env_id, pool_name, token, c_run_id)

            logging.info(f"[RUN:{c_run_id}] Cluster '{cluster_name}' migrated successfully")
            success_list.append({"name": cluster_name, "run_id": c_run_id})
        except Exception as e:
            logging.error(f"[RUN:{c_run_id}] Failed to migrate cluster '{cluster_name}': {str(e)}", exc_info=True)
            failed_list.append({"name": cluster_name, "message": str(e), "run_id": c_run_id})

    total = len(cluster_ids)
    overall = "success" if not failed_list else "partial" if success_list else "failed"
    logging.info(f"[RUN:{run_id}] Cluster migration complete — status: {overall}, success: {len(success_list)}, failed: {len(failed_list)}")

    return {
        "status": overall,
        "Success": success_list,
        "Failed": failed_list,
        "summary": {"total": total, "success": len(success_list), "failed": len(failed_list)}
    }