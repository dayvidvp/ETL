import os
from time import perf_counter

from loguru import logger

from cls.write_file import func_create_directorys


@logger.catch
def check_if_build_is_processed_from_logfile(rootdir: str, buildid: str, customertoken: str, buildid_hash: str) -> bool:
    """
    Function to check the processed file to see if we already processed this trayid
    """
    path = func_create_directorys(rootdir, customertoken)
    file = f"{customertoken}_tpi_hash.csv"

    file_path = os.path.join(path, file)
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            for line in f:
                if buildid_hash in line:
                    return True
            else:
                return False
    else:
        with open(file_path, "w") as f:
            return False


@logger.catch
def check_if_build_content_is_processed_from_logfile(rootdir: str, buildid: str, customertoken: str, hash: str) -> bool:
    """
    Function to check the processed file to see if we already processed this trayid
    """
    path = func_create_directorys(rootdir, customertoken)
    file = f"{customertoken}.txt"

    file_path = os.path.join(path, file)
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            for line in f:
                if hash in line:
                    return True
            else:
                return False
    else:
        with open(file_path, "w") as f:
            return False


@logger.catch
def check_component_hash_in_cache(rootdir: str, hash: str) -> bool:
    """
    Function to check the processed file to see if we already processed this trayid
    """

    path = func_create_directorys(rootdir)
    file = "_component_cache.csv"

    file_path = os.path.join(path, file)

    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            for line in f:
                if hash in line:
                    return True
            else:
                return False
