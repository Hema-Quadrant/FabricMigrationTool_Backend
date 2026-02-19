import uuid
import requests
from .migrate_notebooks import get_fabric_token


def map_cluster_to_pool(cluster: dict) -> dict:
    node_type = cluster.get("node_type_id", "")
    node_size = "Medium" if "D8" in node_type else "Small"

    autoscale = cluster.get("autoscale")
    if autoscale:
        min_nodes = autoscale.get("min_workers", 1)
        max_nodes = autoscale.get("max_workers", min_nodes)
    else:
        min_nodes = max_nodes = max(cluster.get("num_workers", 1), 1)

    if cluster.get("is_single_node"):
        min_nodes = max_nodes = 1

    name = cluster.get("cluster_name", "cluster")
    pool_name = name if name.startswith("Mig_") else f"Mig_{name}"

    return {
        "name": pool_name,
        "nodeFamily": "MemoryOptimized",
        "nodeSize": node_size,
        "autoScale": {"enabled": True, "minNodeCount": min_nodes, "maxNodeCount": max_nodes},
        "dynamicExecutorAllocation": {"enabled": False}
    }


def ensure_workspace(fabric_cfg: dict, token: str) -> str:
    if fabric_cfg.get("workspaceId"):
        return fabric_cfg["workspaceId"]
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
    return r.json()["id"]


def ensure_environment(ws_id: str, env_name: str, token: str) -> str:
    list_url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/environments"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    r = requests.get(list_url, headers=headers, timeout=45)
    if r.status_code == 200:
        for env in r.json().get("value", []):
            if env.get("displayName") == env_name:
                return env["id"]

    r = requests.post(list_url, headers=headers, json={"displayName": env_name, "description": "Migrated from Databricks"}, timeout=60)
    r.raise_for_status()
    return r.json()["id"]


def attach_pool_to_environment(ws_id: str, env_id: str, pool_name: str, token: str):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    requests.patch(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/environments/{env_id}/staging/sparkcompute",
        headers=headers,
        json={"instancePool": {"name": pool_name, "type": "Workspace"}, "runtimeVersion": "1.3"},
        timeout=60
    ).raise_for_status()
    requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/environments/{env_id}/staging/publish",
        headers=headers, timeout=90
    ).raise_for_status()


def create_pool(ws_id: str, token: str, payload: dict):
    r = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/spark/pools",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload, timeout=120
    )
    if r.status_code == 409:
        raise Exception("already exists")
    r.raise_for_status()


def migrate_clusters(fabric_cfg: dict, db_url: str, pat: str, cluster_ids: list) -> dict:
    from services.connect_databricks import get_cluster

    token = get_fabric_token(fabric_cfg["tenantId"], fabric_cfg["clientId"], fabric_cfg["clientSecret"])
    ws_id = ensure_workspace(fabric_cfg, token)

    success_list, failed_list = [], []

    for cluster_id in cluster_ids:
        run_id = str(uuid.uuid4())
        cluster_name = cluster_id
        try:
            cluster = get_cluster(db_url, pat, cluster_id)
            cluster_name = cluster.get("cluster_name", cluster_id)

            pool_payload = map_cluster_to_pool(cluster)
            pool_name = pool_payload["name"]
            create_pool(ws_id, token, pool_payload)

            env_id = ensure_environment(ws_id, f"Env_{pool_name}", token)
            attach_pool_to_environment(ws_id, env_id, pool_name, token)

            success_list.append({"name": cluster_name, "run_id": run_id})
        except Exception as e:
            failed_list.append({"name": cluster_name, "message": str(e), "run_id": run_id})

    total = len(cluster_ids)
    return {
        "status": "success" if not failed_list else "partial" if success_list else "failed",
        "Success": success_list,
        "Failed": failed_list,
        "summary": {"total": total, "success": len(success_list), "failed": len(failed_list)}
    }