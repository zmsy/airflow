# Data Scripts

This contains a number of scheduled scripts for me to keep things updated. Mostly this is data scrapers.

At one point this was an airflow instance, but I abandoned that because it's miserable to run.

## Getting Started

- Install dependencies and lock the environment with `uv sync` (creates or updates `.venv`).
- Run any script via `uv run python <path/to/script.py>` once the requirements and `.env` values are in place.
- Format the codebase with `uv format` (Black is configured through the uv dev group).
