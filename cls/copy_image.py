import os
import shutil
import threading
from time import perf_counter
from loguru import logger


@logger.catch
def copy_image(file: str, source_dir: str, target_dir: str):
    """
    This function copies the images for the buildid to the buildid folder.
    """
    _timer_start = perf_counter()
    logger.debug(f"Copying image {file}")
    source_image_path = os.path.join(source_dir, file)
    target_image_path = os.path.join(target_dir, file)
    if os.path.exists(target_image_path):
        if os.stat(source_image_path).st_mtime - os.stat(target_image_path).st_mtime > 1:
            shutil.copy2(source_image_path, target_image_path)
            logger.debug(f"Copied {source_image_path} to {target_image_path}")
            _timer_stop = perf_counter()
            logger.debug(f"copy_images_for_buildid took {_timer_stop - _timer_start}s")
        else:
            logger.debug(f"Image {file} is up to date")
            _timer_stop = perf_counter()
            logger.debug(f"copy_images_for_buildid took {_timer_stop - _timer_start}s")
    else:
        shutil.copy2(source_image_path, target_image_path)
        logger.debug(f"Copied {source_image_path} to {target_image_path}")
        _timer_stop = perf_counter()
        logger.debug(f"copy_images_for_buildid took {_timer_stop - _timer_start}s")


@logger.catch
def copy_images_for_buildid(images: list, source_dir: str, target_dir: str):

    filelist = images
    p = []
    for f in filelist:
        t = threading.Thread(target=copy_image, args=(f, source_dir, target_dir))
        p.append(t)
        t.daemon = True
        t.start()

    for t in p:
        t.join()


@logger.catch
def remove_al_images_from_dir(target_dir: str):
    _timer_start = perf_counter()
    for root, dirs, files in os.walk(target_dir):
        logger.debug(f"removeing files from {target_dir}")
        for file in files:
            if file.lower().endswith('.png') or file.lower().endswith(".jpg"):
                os.remove(os.path.join(root, file))
                logger.debug(f"removed {root}{file}")
    _timer_stop = perf_counter()
    logger.debug(f"remove_all_images_from_dir took {_timer_start - _timer_stop}s")
