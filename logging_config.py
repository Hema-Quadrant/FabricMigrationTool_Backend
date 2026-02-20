import logging
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

def setup_logging():
    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    logging.getLogger("azure").setLevel(logging.WARNING)     
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    connection_string = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")

    if connection_string:
        try:
            from azure.monitor.opentelemetry import configure_azure_monitor
            configure_azure_monitor(connection_string=connection_string)
            logging.info("Azure Monitor (App Insights) logging configured")
        except ImportError:
            logging.warning("azure-monitor-opentelemetry not installed — App Insights logging disabled. Run: pip install azure-monitor-opentelemetry")
    else:
        logging.warning("APPLICATIONINSIGHTS_CONNECTION_STRING not set — App Insights logging disabled")

