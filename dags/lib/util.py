import os


def output_path(file_name: str) -> str:
    """
    Retrieves the global output folder and any files in it.
    """
    return os.path.join(os.path.curdir, "output", file_name)
