"""
Database connection information.
"""

import os

# connection information for the database
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_IP = "192.168.0.103"
POSTGRES_PORT = 5432
POSTGRES_DB = "postgres"
