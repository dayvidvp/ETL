import functools
import os
from time import perf_counter

from loguru import logger
from rich.console import Console

console = Console()


@logger.catch
@functools.lru_cache(maxsize=128)
def list_images_in_directory(directory: str):
    """
    List all images in a directory, cache the results
    """
    _timer_start = perf_counter()
    _list_images_in_directory: list = []
    for root, dirs, files in os.walk(directory, topdown=True):
        for file in files:
            _list_images_in_directory.append(file)
    logger.debug(f"Found {len(_list_images_in_directory)} images in {directory}")
    _timer_stop = perf_counter()
    logger.debug(f"cache images directory: {_timer_stop - _timer_start}s")
    return _list_images_in_directory


@logger.catch
def search_image_buildid(objectno: int, intern_buildid_number: int, server: str):
    """
    Search for an image in the current directory and return the image path
    """
    root_dir: str = "SPMiCustomData"
    media_dir: str = "Media"
    images_dir: str = "Images"
    image_path: str = os.path.join(
        os.sep + os.sep + server + os.sep + root_dir + os.sep + media_dir + os.sep + images_dir)
    image_list: list = []
    _prefix: str = "T-" + str(intern_buildid_number)
    _image_relative_path: str = os.path.join(os.sep + images_dir)
    for image in list_images_in_directory(image_path):
        if image.startswith(_prefix):
            _dict: dict = {
                "objectno": objectno,
                "internal_buildid_number": intern_buildid_number,
                "image_path": r"\images",
                "image": image,
            }
            image_list.append(_dict)
    return image_list


@logger.catch
def search_image_component(internal_article_number: int, server: str):
    """
    Search for an image in the current directory and return the image path
    """
    root_dir: str = "SPMiCustomData"
    media_dir: str = "Media"
    images_dir: str = "Images"
    image_path: str = os.path.join(
        os.sep + os.sep + server + os.sep + root_dir + os.sep + media_dir + os.sep + images_dir
    )
    image_list: list = []
    _image_relative_path: str = os.path.join(os.sep + images_dir)
    _prefix: str = str(internal_article_number)
    for image in list_images_in_directory(image_path):
        if image.startswith(_prefix):
            _dict: dict = {
                "image_path": r"\images",
                "image": image,
            }
            image_list.append(_dict)
    return image_list
