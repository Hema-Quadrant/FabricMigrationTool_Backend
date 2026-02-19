import base64
import time
import uuid
import logging
import requests

from services.connect_databricks import export_notebook_with_fallback


def get_fabric_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    r = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://api.fabric.microsoft.com/.default"
        },
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]


def list_existing_notebooks(workspace_id: str, token: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items", headers=headers)
    r.raise_for_status()
    return {
        item["displayName"].lower(): item["id"]
        for item in (r.json() or {}).get("value", [])
        if item.get("type") == "Notebook"
    }


def create_notebook(workspace_id: str, token: str, name: str, ipynb_bytes: bytes) -> str:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "displayName": name,
        "type": "Notebook",
        "definition": {
            "format": "ipynb",
            "parts": [{"path": f"{name}.ipynb", "payloadType": "InlineBase64", "payload": base64.b64encode(ipynb_bytes).decode()}]
        }
    }
    r = requests.post(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items", headers=headers, json=payload)
    r.raise_for_status()
    return (r.json() or {}).get("id")


def update_notebook(workspace_id: str, token: str, notebook_id: str, ipynb_bytes: bytes):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "definition": {
            "format": "ipynb",
            "parts": [{"path": "notebook.ipynb", "payloadType": "InlineBase64", "payload": base64.b64encode(ipynb_bytes).decode()}]
        }
    }
    r = requests.post(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/updateDefinition", headers=headers, json=payload)
    r.raise_for_status()


def migrate_notebooks(
    tenant_id: str, client_id: str, client_secret: str,
    workspace_id: str, db_url: str, pat: str,
    notebooks: list, replace_if_exists: bool = False
) -> dict:
    from datetime import datetime

    token = get_fabric_token(tenant_id, client_id, client_secret)
    existing = list_existing_notebooks(workspace_id, token)
    time.sleep(2)

    migrated = replaced = already_exists = skipped = failed = 0
    details = []
    MAX_RETRIES, BASE_DELAY = 3, 4

    for nb in notebooks:
        name = (nb.get("name") or "").strip()
        path = (nb.get("path") or "").strip()
        run_id = str(uuid.uuid4())

        if not name or not path:
            failed += 1
            details.append({"name": name, "status": "invalid-input", "run_id": run_id})
            continue

        try:
            ipynb_bytes = export_notebook_with_fallback(db_url, pat, path, run_id)
            if not ipynb_bytes:
                failed += 1
                details.append({"name": name, "status": "export-failed", "run_id": run_id})
                continue

            if name.lower() in existing:
                if replace_if_exists:
                    update_notebook(workspace_id, token, existing[name.lower()], ipynb_bytes)
                    replaced += 1
                    status = "replaced"
                else:
                    skipped += 1
                    already_exists += 1
                    status = "already-exists"
            else:
                created = False
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        create_notebook(workspace_id, token, name, ipynb_bytes)
                        migrated += 1
                        status = "created"
                        created = True
                        break
                    except requests.HTTPError as e:
                        if e.response and e.response.status_code == 400 and attempt < MAX_RETRIES:
                            time.sleep(BASE_DELAY * attempt)
                            continue
                        raise
                if not created:
                    failed += 1
                    details.append({"name": name, "status": "create-failed-after-retries", "run_id": run_id})
                    continue

            details.append({"name": name, "status": status, "run_id": run_id})

        except Exception as e:
            failed += 1
            details.append({"name": name, "status": "failed", "error": str(e), "run_id": run_id})

    total = len(notebooks)
    success_count = migrated + replaced + already_exists
    return {
        "status": "success" if failed == 0 else "partial" if success_count > 0 else "failed",
        "migrated": migrated, "replaced": replaced, "already_exists": already_exists,
        "skipped": skipped, "failed": failed, "total": total,
        "details": details,
        "timestamp": datetime.utcnow().isoformat()
    }