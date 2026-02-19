import requests


def get_headers(pat: str) -> dict:
    return {"Authorization": f"Bearer {pat}", "Content-Type": "application/json"}


def get_workspace_name(db_url: str, pat: str) -> str:
    r = requests.get(
        f"{db_url}/api/2.0/workspace-conf",
        headers=get_headers(pat),
        params={"keys": "workspaceName"}
    )
    return r.json().get("workspaceName", "Unknown") if r.status_code == 200 else "Unknown"


def list_workspace(db_url: str, pat: str, path: str = "/") -> list:
    r = requests.get(
        f"{db_url}/api/2.0/workspace/list",
        headers=get_headers(pat),
        params={"path": path}
    )
    r.raise_for_status()
    result = []
    for obj in r.json().get("objects", []):
        result.append(obj)
        if obj.get("object_type") == "DIRECTORY":
            result.extend(list_workspace(db_url, pat, obj["path"]))
    return result


def list_jobs(db_url: str, pat: str) -> list:
    r = requests.get(f"{db_url}/api/2.1/jobs/list", headers=get_headers(pat))
    r.raise_for_status()
    return r.json().get("jobs", [])


def list_clusters(db_url: str, pat: str) -> list:
    r = requests.get(f"{db_url}/api/2.0/clusters/list", headers=get_headers(pat))
    r.raise_for_status()
    return r.json().get("clusters", [])


def get_cluster(db_url: str, pat: str, cluster_id: str) -> dict:
    r = requests.get(
        f"{db_url}/api/2.0/clusters/get",
        headers=get_headers(pat),
        params={"cluster_id": cluster_id}
    )
    r.raise_for_status()
    return r.json()


def get_job(db_url: str, pat: str, job_id: str) -> dict:
    r = requests.get(
        f"{db_url}/api/2.1/jobs/get",
        headers=get_headers(pat),
        params={"job_id": job_id}
    )
    r.raise_for_status()
    return r.json().get("settings", {})


def export_notebook(db_url: str, pat: str, path: str) -> bytes:
    """Returns raw notebook bytes (JUPYTER format)."""
    r = requests.get(
        f"{db_url}/api/2.0/workspace/export",
        headers=get_headers(pat),
        params={"path": path, "format": "JUPYTER", "direct_download": "true"},
        timeout=90
    )
    r.raise_for_status()
    return r.content


def export_notebook_with_fallback(db_url: str, pat: str, path: str, run_id: str):
    """Returns parsed nbformat notebook object, trying JUPYTER then SOURCE."""
    import base64
    import logging
    import nbformat

    headers = get_headers(pat)
    url = f"{db_url}/api/2.0/workspace/export"

    for fmt in ["JUPYTER", "SOURCE"]:
        try:
            r = requests.get(url, headers=headers, params={"path": path, "format": fmt})
            if r.status_code != 200:
                continue
            content = (r.json() or {}).get("content")
            if not content:
                continue
            decoded = base64.b64decode(content).decode("utf-8", errors="replace")
            if fmt == "JUPYTER":
                nb = nbformat.reads(decoded, as_version=4)
            else:
                nb = nbformat.v4.new_notebook()
                nb.cells = [nbformat.v4.new_code_cell(decoded)]
            return nbformat.writes(nb).encode("utf-8")
        except Exception as e:
            logging.warning(f"[RUN:{run_id}] Export failed for format {fmt}: {e}")
    return None