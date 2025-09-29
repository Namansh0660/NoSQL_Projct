# NOSQL/pipeline_runner.py
"""
Pipeline Runner for NOSQL KG Project
------------------------------------
- Runs Streamlit dashboard connecting to MongoDB Atlas
- Monitors MongoDB Atlas 'papers' collection for new documents
- Automatically triggers KG builder & edge builder
- Handles graceful shutdown
"""

import threading
import subprocess
import time
import signal
import sys
import logging
from api.db import papers_collection, nodes_collection as kg_nodes_collection, edges_collection as kg_edges_collection  # ✅ Atlas connection

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger("pipeline_runner")

# -----------------------------
# Graceful shutdown
# -----------------------------
_shutdown = False

def _signal_handler(signum, frame):
    global _shutdown
    logger.info(f"Received signal {signum} - shutting down...")
    _shutdown = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# -----------------------------
# Function to run a script in a separate thread
# -----------------------------
def run_script(script_path):
    """
    Run a Python script as a subprocess and keep it alive.
    """
    logger.info(f"Starting {script_path} ...")
    process = subprocess.Popen([sys.executable, script_path])
    return process

# -----------------------------
# Monitor papers collection and trigger KG builder
# -----------------------------
def monitor_and_build_kg(poll_interval=10):
    """
    Monitor papers collection for new papers and run KG builder.
    """
    last_count = papers_collection.count_documents({})
    logger.info(f"Initial papers count: {last_count}")

    while not _shutdown:
        time.sleep(poll_interval)
        current_count = papers_collection.count_documents({})
        if current_count > last_count:
            new_papers = current_count - last_count
            logger.info(f"Detected {new_papers} new papers → running KG builder...")

            # Run KG builder scripts sequentially
            subprocess.run([sys.executable, "kg_builder/kg_builder.py"])
            subprocess.run([sys.executable, "kg_builder/kg_edge_builder.py"])

            last_count = current_count

# -----------------------------
# Main pipeline
# -----------------------------
def main():
    # 1️⃣ Start Streamlit dashboard (connecting to MongoDB Atlas)
    logger.info("Starting Streamlit dashboard for Atlas sharding analysis...")
    streamlit_process = subprocess.Popen([
        sys.executable, "-m", "streamlit", "run", 
        "ui/app.py", 
        "--server.headless=true", 
        "--server.port=8501",
        "--global.developmentMode=false"
    ])

    # 2️⃣ Start monitor thread for KG builder
    monitor_thread = threading.Thread(target=monitor_and_build_kg, daemon=True)
    monitor_thread.start()

    # Keep main thread alive and handle shutdown
    try:
        while not _shutdown:
            time.sleep(1)
            # Check if streamlit process is still running
            if streamlit_process.poll() is not None:
                logger.warning("Streamlit process terminated unexpectedly")
                break
    finally:
        logger.info("Shutting down pipeline runner...")
        if streamlit_process.poll() is None:
            streamlit_process.terminate()
            streamlit_process.wait(timeout=10)
        monitor_thread.join()
        logger.info("Pipeline fully stopped.")

if __name__ == "__main__":
    main()
