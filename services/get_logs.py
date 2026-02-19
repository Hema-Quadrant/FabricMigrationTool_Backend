import os
import requests

APP_INSIGHTS_APP_ID = os.environ.get("APP_INSIGHTS_APP_ID")
APP_INSIGHTS_API_KEY = os.environ.get("APP_INSIGHTS_API_KEY")


def get_logs(run_id: str) -> dict:
    if not APP_INSIGHTS_APP_ID or not APP_INSIGHTS_API_KEY:
        raise ValueError("Application Insights not configured")

    query = f"""
        union traces, exceptions
        | where timestamp > ago(4h)
        | where message contains "[RUN:{run_id}]"
        | project timestamp, message, severityLevel, itemType
        | order by timestamp asc
        | limit 10000
    """

    response = requests.get(
        f"https://api.applicationinsights.io/v1/apps/{APP_INSIGHTS_APP_ID}/query",
        headers={"x-api-key": APP_INSIGHTS_API_KEY, "Content-Type": "application/json"},
        params={"query": query},
        timeout=45
    )
    response.raise_for_status()

    data = response.json()
    logs = []

    if data.get("tables"):
        table = data["tables"][0]
        col_map = {col["name"]: idx for idx, col in enumerate(table.get("columns", []))}
        for row in table.get("rows", []):
            logs.append({
                "timestamp": row[col_map.get("timestamp", 0)],
                "message": row[col_map.get("message", 1)],
                "severity": row[col_map.get("severityLevel", 2)],
                "type": row[col_map.get("itemType", 3)] if "itemType" in col_map else "trace"
            })

    return {"run_id": run_id, "log_count": len(logs), "logs": logs}