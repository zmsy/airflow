"""
Airflow Utilities

Creates a number of reusable functions that can be rolled into
other airflow scripts.
"""

import os

def output_path(file_name):
    """
    Retrieves the global output folder and any files in it.
    """
    return os.path.join(os.environ.get('AIRFLOW_HOME'), 'output', file_name)