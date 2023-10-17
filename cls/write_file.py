import base64
import json
import os
import shutil
import subprocess
from datetime import datetime
from sys import stderr
from time import perf_counter
from xml.dom.minidom import parseString

from loguru import logger

from cls.dicttoxml import dicttoxml

LOG_DIR: str = "log"
LOG_TIME: str = datetime.now().strftime("%Y-%m-%d")


class LogFilter:
    def __init__(self, level):
        self.level = level

    def __call__(self, record):
        levelno = logger.level(self.level).no
        return record["level"].no >= levelno


@logger.catch
def map_network_drive(backup_server: str, server: str):
    """
    Function to map the network drive to the local drive
    """
    networkpath: str = f"\\\\{backup_server}\\{server}"

    repr = base64.b64decode(b'ODg3dFlVajQxPVhYNA==')

    _password = repr.decode('utf-8')

    winCMD = 'NET USE ' + networkpath + ' /User:' + 'sterima\\usrbestsync_svc' + ' ' + _password
    # winCMD = 'NET USE ' + networkpath + ' /User:' + 'sterima\\usrbestsync_svc' + ' ' + '887tYUj41=XX4'

    subprocess.Popen(winCMD, stdout=subprocess.PIPE, shell=True)
    return networkpath


@logger.catch
def func_create_directorys(root_dir: str, *subdirs: str) -> str:

    root_dir: str = root_dir

    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
        logger.debug(f"Creating rootdirectory {root_dir}")

    for _dir in subdirs:
        root_dir = os.path.join(root_dir, _dir)

        if not os.path.exists(root_dir):
            logger.debug(f"Creating directory {root_dir}")
            os.makedirs(root_dir)

    return root_dir


@logger.catch
def create_logfile(rootdir: str, file: str, loglevel: str) -> None:
    """
    Write to the logfile
    """
    fmt = "{time} - {name} - {level} - {message}"
    logfile: str = os.path.join(rootdir, file)
    logger.remove(0)
    if loglevel == "DEBUG":
        logger.add(
            logfile,
            level="DEBUG",
            format=fmt,
            rotation="1 day",
            retention="7 days",
            compression="zip",
        )
        logger.debug("Loglevel is DEBUG")
        logger.add(stderr, format=fmt, level="DEBUG")
    elif loglevel == "INFO":
        logger.add(
            logfile,
            level="INFO",
            format=fmt,
            rotation="1 day",
            retention="7 days",
            compression="zip",
        )
        logger.info("Loglevel: INFO")
    elif loglevel == "WARNING":
        logger.add(
            logfile,
            level="WARNING",
            format=fmt,
            rotation="1 day",
            retention="7 days",
            compression="zip",
        )
        logger.warning("Loglevel: WARNING")
    elif loglevel == "ERROR":
        logger.add(
            logfile,
            level="ERROR",
            format=fmt,
            rotation="1 day",
            retention="7 days",
            compression="zip",
        )
        logger.warning("Loglevel: ERROR")


@logger.catch
def write_build_to_file(
    rootdir: str, buildid: str, revisionnumber: int, customertoken: str, hash: str
) -> None:

    path = func_create_directorys(rootdir, customertoken)
    file = f"{customertoken}_tpi_hash.csv"
    file_path = os.path.join(path, file)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(file_path, "a+", encoding="utf-8") as f:
        f.write(f"{now}: {buildid} revision:{revisionnumber} hash: {hash}\n")


@logger.catch
def write_trayid_to_file(
    rootdir: str,
    buildid: str,
    build_description: str,
    customertoken: str,
    department: str,
    revisionnumber: int,
    trayid: str
) -> None:
    _timer_start = perf_counter()
    path = func_create_directorys(rootdir, customertoken)
    file = "traylist.txt"
    file_path = os.path.join(path, file)
    xml_path = os.path.join(department, buildid)
    with open(file_path, "a+", encoding='utf-8') as f:
        f.write(f"{buildid};{build_description};{revisionnumber};{trayid};{xml_path}\n")
    _timer_stop = perf_counter()
    logger.debug(f"write_trayid_to_file took {_timer_stop - _timer_start}s")


@logger.catch
def write_json_file_to_disk(
    rootdir: str,
    customertoken: str,
    department: str,
    buildid: str,
    revisionnumber: int,
    data_list: dict,
) -> None:

    file_save_path: str = func_create_directorys(
        rootdir, customertoken, department, buildid
    )
    _timer_start = perf_counter()
    file_name: str = f"{customertoken}_{buildid}_{revisionnumber}.json"
    file_path: str = os.path.join(file_save_path + os.sep + file_name)
    with open(file_path, "w+", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=4)
    _timer_stop = perf_counter()
    logger.debug(f"write_json_file_to_disk took {_timer_stop - _timer_start}s")


@logger.catch
def create_xsl_directory(rootdir: str):

    file_save_path: str = func_create_directorys(rootdir, "xsl")
    file_save: str = os.path.join(file_save_path, "transform.xsl")
    _timer_start = perf_counter()
    logger.debug(f"Copying transform files to {file_save_path}")
    source_image_path = r"xsl\transform.xsl"
    if not os.path.exists(file_save):
        shutil.copy2(source_image_path, file_save_path)
        _timer_stop = perf_counter()
        logger.debug(f"create_xsl took {_timer_stop - _timer_start}s")


@logger.catch
def write_xml_file_to_disk(
    rootdir: str,
    customertoken: str,
    department: str,
    buildid: str,
    revisionnumber: int,
    data_list: dict,
) -> None:

    file_save_path: str = func_create_directorys(
        rootdir, customertoken, department, buildid
    )
    _timer_start = perf_counter()
    
    file_name: str = f"{customertoken}_{buildid}_{revisionnumber}.xml"
    file_path: str = os.path.join(file_save_path + os.sep + file_name)
    xml_string = dicttoxml(data_list, attr_type=False)
    dom = parseString(xml_string)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(dom.toprettyxml())
    _timer_stop = perf_counter()
    logger.debug(f"write_xml_file_to_disk took {_timer_stop - _timer_start}s")
