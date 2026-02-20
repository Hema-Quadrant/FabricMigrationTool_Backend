import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Any, Optional
import requests

from logging_config import setup_logging
setup_logging()

from services.connect_databricks import list_workspace, list_jobs, list_clusters, get_workspace_name
from services.get_logs import get_logs
from services.migrate_notebooks import migrate_notebooks
from services.migrate_clusters import migrate_clusters
from services.migrate_jobs import migrate_jobs

app = FastAPI(title="Databricks → Fabric Migration API")


# ─── Request models ─────────────────────────────────────────

class ScanRequest(BaseModel):
    databricksUrl: str
    personalAccessToken: str


class NotebookEntry(BaseModel):
    name: str
    path: str

class MigrateNotebooksRequest(BaseModel):
    tenantId: str
    clientId: str
    clientSecret: str
    workspaceId: str
    databricksUrl: str
    personalAccessToken: str
    replaceIfExists: bool = False
    notebooks: list[NotebookEntry]


class DatabricksConfig(BaseModel):
    host: str
    pat: str

class MigrateClustersRequest(BaseModel):
    fabric: dict
    databricks: DatabricksConfig
    selectedClusters: list[str]


class MigrateJobsRequest(BaseModel):
    tenantId: str
    clientId: str
    clientSecret: str
    workspaceId: str
    databricksUrl: str
    personalAccessToken: str
    jobid: Any


# ─── Routes ─────────────────────────────────────────────────

@app.get("/logs")
def route_get_logs(run_id: str = Query(...)):
    try:
        return get_logs(run_id.strip())
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Query timed out — try again in a few seconds.")
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Application Insights error: {str(e)}")


@app.post("/scan")
def route_scan(body: ScanRequest):
    try:
        db_url = body.databricksUrl.rstrip("/")
        pat = body.personalAccessToken
        workspace_assets = list_workspace(db_url, pat)
        notebooks, folders = [], []
        for o in workspace_assets:
            path = o.get("path", "")
            enriched = {"name": path.split("/")[-1], "path": path, "language": o.get("language"), "object_type": o.get("object_type")}
            if o.get("object_type") == "NOTEBOOK":
                notebooks.append(enriched)
            elif o.get("object_type") == "DIRECTORY":
                folders.append(enriched)
        jobs = list_jobs(db_url, pat)
        clusters = list_clusters(db_url, pat)
        return {
            "databricksWorkspaceName": get_workspace_name(db_url, pat),
            "counts": {"notebooks": len(notebooks), "folders": len(folders), "jobs": len(jobs), "clusters": len(clusters)},
            "notebooks": notebooks, "folders": folders, "jobs": jobs, "clusters": clusters
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/migrate/notebooks")
def route_migrate_notebooks(body: MigrateNotebooksRequest):
    try:
        result = migrate_notebooks(
            tenant_id=body.tenantId, client_id=body.clientId, client_secret=body.clientSecret,
            workspace_id=body.workspaceId, db_url=body.databricksUrl, pat=body.personalAccessToken,
            notebooks=[nb.model_dump() for nb in body.notebooks],
            replace_if_exists=body.replaceIfExists
        )
        failed = result["failed"]
        success = result["migrated"] + result["replaced"] + result["already_exists"]
        status_code = 200 if failed == 0 else 207 if success > 0 else 500
        return JSONResponse(status_code=status_code, content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/migrate/clusters")
def route_migrate_clusters(body: MigrateClustersRequest):
    if not body.selectedClusters:
        raise HTTPException(status_code=400, detail="selectedClusters required")
    try:
        result = migrate_clusters(
            fabric_cfg=body.fabric,
            db_url=body.databricks.host,
            pat=body.databricks.pat,
            cluster_ids=body.selectedClusters
        )
        failed = len(result["Failed"])
        success = len(result["Success"])
        status_code = 200 if failed == 0 else 207 if success > 0 else 500
        return JSONResponse(status_code=status_code, content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/migrate/jobs")
def route_migrate_jobs(body: MigrateJobsRequest):
    job_input = body.jobid
    if isinstance(job_input, (str, int)):
        job_ids = [str(job_input).strip()]
    elif isinstance(job_input, list):
        job_ids = [str(j).strip() for j in job_input if str(j).strip()]
    else:
        raise HTTPException(status_code=400, detail="Invalid jobid format")
    if not job_ids:
        raise HTTPException(status_code=400, detail="No job IDs provided")

    try:
        result = migrate_jobs(
            tenant_id=body.tenantId, client_id=body.clientId, client_secret=body.clientSecret,
            workspace_id=body.workspaceId, db_url=body.databricksUrl, pat=body.personalAccessToken,
            job_ids=job_ids
        )
        status_code = 200 if result["failed"] == 0 else 207 if result["created"] + result["already_exist"] > 0 else 500
        return JSONResponse(status_code=status_code, content=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))