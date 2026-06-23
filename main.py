"""
Airflow Glue Claude Analyst — Entry Point
"""

import logging
from dotenv import load_dotenv

load_dotenv()

from src.bot import create_app, start

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

if __name__ == "__main__":
    start()
