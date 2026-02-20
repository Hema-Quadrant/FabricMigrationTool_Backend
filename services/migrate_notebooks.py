import base64
import time
import uuid
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

from .connect_databricks import export_notebook_with_fallback


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


def create_notebook(workspace_id: str, token: str, name: str, ipynb_bytes: bytes, run_id: str) -> str:
    logging.info(f"[RUN:{run_id}] Creating notebook: {name}")
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
    nb_id = (r.json() or {}).get("id")
    logging.info(f"[RUN:{run_id}] Notebook created: {name} (ID: {nb_id})")
    return nb_id


def update_notebook(workspace_id: str, token: str, notebook_id: str, ipynb_bytes: bytes, run_id: str):
    logging.info(f"[RUN:{run_id}] Updating notebook ID: {notebook_id}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "definition": {
            "format": "ipynb",
            "parts": [{"path": "notebook.ipynb", "payloadType": "InlineBase64", "payload": base64.b64encode(ipynb_bytes).decode()}]
        }
    }
    r = requests.post(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{notebook_id}/updateDefinition", headers=headers, json=payload)
    r.raise_for_status()
    logging.info(f"[RUN:{run_id}] Notebook updated successfully")


def migrate_notebooks(
    tenant_id: str, client_id: str, client_secret: str,
    workspace_id: str, db_url: str, pat: str,
    notebooks: list, replace_if_exists: bool = False
) -> dict:
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

        logging.info(f"[RUN:{run_id}] Processing notebook: {name} ({path})")

        if not name or not path:
            logging.error(f"[RUN:{run_id}] Invalid input — name='{name}' path='{path}'")
            failed += 1
            details.append({"name": name, "status": "invalid-input", "run_id": run_id})
            continue

        try:
            logging.info(f"[RUN:{run_id}] Exporting from Databricks: {path}")
            ipynb_bytes = export_notebook_with_fallback(db_url, pat, path, run_id)
            if not ipynb_bytes:
                logging.error(f"[RUN:{run_id}] Export failed for: {path}")
                failed += 1
                details.append({"name": name, "status": "export-failed", "run_id": run_id})
                continue

            if name.lower() in existing:
                if replace_if_exists:
                    update_notebook(workspace_id, token, existing[name.lower()], ipynb_bytes, run_id)
                    replaced += 1
                    status = "replaced"
                else:
                    logging.info(f"[RUN:{run_id}] Notebook already exists, skipping: {name}")
                    skipped += 1
                    already_exists += 1
                    status = "already-exists"
            else:
                created = False
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        create_notebook(workspace_id, token, name, ipynb_bytes, run_id)
                        migrated += 1
                        status = "created"
                        created = True
                        break
                    except requests.HTTPError as e:
                        logging.warning(f"[RUN:{run_id}] Create attempt {attempt} failed: {str(e)}")
                        if e.response and e.response.status_code == 400 and attempt < MAX_RETRIES:
                            time.sleep(BASE_DELAY * attempt)
                            continue
                        raise
                if not created:
                    logging.error(f"[RUN:{run_id}] Failed to create after {MAX_RETRIES} attempts: {name}")
                    failed += 1
                    details.append({"name": name, "status": "create-failed-after-retries", "run_id": run_id})
                    continue

            logging.info(f"[RUN:{run_id}] Notebook '{name}' status: {status}")
            details.append({"name": name, "status": status, "run_id": run_id})

        except Exception as e:
            logging.error(f"[RUN:{run_id}] Error processing '{name}': {str(e)}", exc_info=True)
            failed += 1
            details.append({"name": name, "status": "failed", "error": str(e), "run_id": run_id})

    total = len(notebooks)
    success_count = migrated + replaced + already_exists
    overall = "success" if failed == 0 else "partial" if success_count > 0 else "failed"
    logging.info(f"Notebook migration complete — status: {overall}, migrated: {migrated}, replaced: {replaced}, failed: {failed}, total: {total}")

    return {
        "status": overall,
        "migrated": migrated, "replaced": replaced, "already_exists": already_exists,
        "skipped": skipped, "failed": failed, "total": total,
        "details": details,
        "timestamp": datetime.utcnow().isoformat()
    }