from loguru import logger
import cls.image_search as image_search
import concurrent.futures
from cls.copy_image import copy_images_for_buildid, remove_al_images_from_dir
import os

POSTGRES_SERVER = "KPRDCSA03"

@logger.catch
def get_images_for_buildid(objectno: int, internal_buildid_number: int) -> list:

    _images_list: list = image_search.search_image_buildid(
        objectno, internal_buildid_number, POSTGRES_SERVER
    )
    logger.debug(f"Found {len(_images_list)} images for buildid {objectno}")

    return _images_list


@logger.catch
def get_images_for_componentid(internal_article_number: int) -> list:

    _images_list: list = image_search.search_image_component(
        internal_article_number, POSTGRES_SERVER
    )
    return _images_list


@logger.catch
def process_images(
    buildid_images: list,
    bom_content_images: list,
    customertoken: str,
    build_id: str,
    department: str,
):

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(
            aggregate_build_and_component_images(
                buildid_images,
                bom_content_images,
                build_id,
                customertoken,
                department,
            )
        )


@logger.catch
def aggregate_build_and_component_images(
    buildid_images: list,
    component_images: list,
    buildid: str,
    customertoken: str,
    department: str,
) -> list:

    _final_image_list: list = []
    for _row in component_images:
        for key, value in _row.items():
            if key == "component_images":
                for _row2 in value["images"]:
                    for key2, value2 in _row2.items():
                        if key2 == "image":
                            _final_image_list.append(value2)
    for _row in buildid_images:
        for key, value in _row.items():
            if key == "image":
                if value not in _final_image_list and value != "":
                    _final_image_list.append(value)

    root_dir: str = "SPMiCustomData"
    media_dir: str = "Media"
    images_dir: str = "Images"

    source_dir: str = os.path.join(
        os.sep
        + os.sep
        + POSTGRES_SERVER
        + os.sep
        + root_dir
        + os.sep
        + media_dir
        + os.sep
        + images_dir
    )
    # target_dir = func_create_directorys(
    #     OUTPUT_DIR, customertoken, department, buildid, "images"
    # )
    target_dir = func_create_directorys(OUTPUT_DIR, customertoken, "images")
    # remove_al_images_from_dir(target_dir)
    copy_images_for_buildid(_final_image_list, source_dir, target_dir)
