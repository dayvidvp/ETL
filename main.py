#! usr/bin/env python3

import argparse
import os
import sys
import uuid
from datetime import datetime
from functools import lru_cache
from itertools import groupby

import pandas as pd
from loguru import logger
from tqdm import tqdm

import data.broker as broker
from cls.bom_content import Bom_Content
from cls.buildid import Buildid
from cls.encode_json import get_json_sem_hash
from cls.helper import timeis
from cls.machine_programs import Machine_programs
from cls.read_config import read_config
from cls.read_file import (check_component_hash_in_cache,
                        check_if_build_is_processed_from_logfile)
from cls.trayid import Trayid
from cls.write_file import *
from component import (get_component_codes,
                    get_processinstructions_for_component,
                    process_component)
from customer import get_active_customers_from_spmi
from data.query import *

config = read_config("config.yaml")

# constants
NOW = datetime.now().strftime("%Y-%m-%d")
LOG_FILE: str = f"export_log_{NOW}.log"
COMPONENT_FILE: str = "_component_cache.csv"
LOG_DIR = config["LOG_DIR"]
LOG_LEVEL = config["LOG_LEVEL"]
MSSQL_SERVER = config["MSSQL_SERVER"]
POSTGRES_SERVER = config["POSTGRES_SERVER"]
BACKUP_SERVER = config["BACKUP_SERVER"]
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")


if not os.path.exists(OUTPUT_DIR):
    os.mkdir(OUTPUT_DIR)


@lru_cache(maxsize=1000)
@logger.catch
def get_active_trays_for_buildid(objectno: int) -> list:

    _trayid_list: list = []
    for _row in broker.retrieve_trayids_per_build(objectno, POSTGRES_SERVER):
        tray_id = Trayid(_row[0], _row[1], _row[2], _row[3])
        _dict = {
            "trayid": tray_id.trayid,
            "traystatus": tray_id.trayidstatus,
            "defaultstocklocation": tray_id.trayiddfaultstocklocation,
            "clipnumber": tray_id.traydidclipnumber,
        }
        _trayid_list.append(_dict)
    return _trayid_list


@logger.catch
def get_machineprograms_for_buildid(objectno: int) -> list:

    _machineprograms_list: list = []

    for _row in broker.retrieve_machine_programs_for_buildid(objectno, POSTGRES_SERVER):
        machineprograms = Machine_programs(_row[0], _row[1])
        _dict = {
            "machine_program_name": machineprograms.Machine_program_description,
            "machine_program_type": machineprograms.machine_program_type,
        }
        _machineprograms_list.append(_dict)
    return _machineprograms_list


@logger.catch
def create_article_hash(internal_article_number: str) -> str:

    _hash_dict: dict = {
        "server": POSTGRES_SERVER,
        "internal_article_number": internal_article_number,
    }
    _article_hash: str = get_json_sem_hash(_hash_dict)

    return _article_hash


@logger.catch
def check_bom_content_for_assembled_item(bom_content: list) -> list:

    _components_to_be_assembled: list = []
    _components_to_assemble: list = []
    _final_component_list: list = []

    for _sort in bom_content:
        if _sort["ctype"] == "workplan":
            if "|!" in _sort["workplan"]:

                _number_of_parts: list = _sort["workplan"].split("|!")
                _number_of_parts_count: int = int(_number_of_parts[1][0])
                _workplan_start_pos = _sort["sort"]

                if _number_of_parts_count > 0:
                    _start_position: int = lookup_next_free_start_position(bom_content, _workplan_start_pos, 0)
                    if _start_position not in _components_to_be_assembled:
                        _components_to_be_assembled.append((_start_position, _number_of_parts_count))

                    logger.debug(f"{_number_of_parts_count} articles from position {_start_position} need to be assembled")

                    _comp_set: set = set(_components_to_be_assembled)
                    _final_component_list: list = list(_comp_set)
                    _final_component_list.sort(key=lambda x: x[0])

    if len(_final_component_list) > 0:  # (start_position, number_of_parts)
        for _assembly in _final_component_list:
            for _component in bom_content:

                _assembly_start_position = _assembly[0]
                _assembly_number_of_parts = _assembly[1]

                for _i in range(_assembly_number_of_parts):
                    if _component["sort"] == _assembly_start_position + _i:
                        _components_to_assemble.append(
                            (
                                _component["sort"],
                                _assembly_number_of_parts,
                                _component["article_hash"],
                            )
                        )

    if len(_components_to_assemble) > 0:
        logger.debug(f"Components to assemble: {_components_to_assemble}")
        process_component_assembly(_components_to_assemble)


@logger.catch
def get_bom_content(objectno: int, revisionnumber: int) -> list:

    _bom_content_list: list = []
    _udi_code_list: list = []
    _process_instruction_list: list = []
    _article_hash: str = ""

    for _row in broker.retrieve_bom_for_build_and_version(objectno, revisionnumber, POSTGRES_SERVER):
        bom_content = Bom_Content(
            _row[0],
            _row[1],
            _row[2],
            _row[3],
            _row[4],
            _row[5],
            _row[6],
            _row[7],
            _row[8],
            _row[9],
            _row[10],
            _row[11],
            _row[12],
            _row[13],
            _row[14],
            _row[15],
            _row[16],
            _row[17],
            _row[18],
        )

        if bom_content.is_alternative:
            bom_content.sort = bom_content.sort - 1

        if bom_content.ctype == "component":
            _article_hash = create_article_hash(bom_content.internal_article_number)
            _udi_code_list = get_component_codes(bom_content.item_objectno)
            _process_instruction_list = get_processinstructions_for_component(bom_content.warningtext)

        _dict = {
            "sort": bom_content.sort,
            "componentname": bom_content.componentname,
            "componentidentifier": bom_content.componentidentifier,
            "manufacturer": bom_content.manufacturer_token,
            "manufacturer_description": bom_content.manufacturer_description,
            "standard_quantity": bom_content.standard_quantity,
            "component_erpreferencenumber": bom_content.component_erpreferencenumber,
            "internal_article_number": bom_content.internal_article_number,
            "alternative": bom_content.is_alternative,
            "udi_codes": {"codes": _udi_code_list},
            "processinstructions": {"instructions": _process_instruction_list},
            "packingnote": bom_content.packingnote,
            "workplan": bom_content.workplan,
            "ctype": bom_content.ctype,
            "article_hash": _article_hash,
        }
        _bom_content_list.append(_dict)

    return _bom_content_list


@logger.catch
def process_component_assembly(components_to_assemble: list) -> None:

    _final_components_assembled: list = []
    _previous_position = 0

    for _row in components_to_assemble:

        _position: int = _row[0]
        _count: int = _row[1]
        _article_hash: str = _row[2]

        _uuid = uuid.uuid4()
        _stored_uuid: str

        if int(_previous_position) + 1 != int(_position):
            _previous_position = _position
            _stored_uuid = _uuid

            _final_components_assembled.append((_position, _article_hash, _count, _uuid))

        elif int(_previous_position) + 1 == int(_position):

            _previous_position = _position
            _final_components_assembled.append((_position, _article_hash, _count, _stored_uuid))

            result = []
            #  group the components that needs to be assembled toghether and
            # create a new has for them so they are unique
            for _, values in groupby(_final_components_assembled, lambda x: x[3]):
                b = tuple(values)
                if len(b) >= 2:
                    result.append((abs(hash(tuple(j[1] for j in b))), tuple(j[1] for j in b)))
                else:
                    result.append(tuple(j for j in b)[0])

    for _row in result:
        _compound_article_hash = _row[0]
        _count = len(_row[1])
        for _article_hash in _row[1]:
            broker.save_component_assembly(_compound_article_hash, _article_hash, _count, MSSQL_SERVER)


@logger.catch
def get_all_active_buildids_for_customer(customertoken: str) -> list:

    logger.info(f"Getting all active buildids for customer {customertoken}")
    all_builds: list = broker.retrieve_active_builds_for_customer(customertoken, POSTGRES_SERVER)
    _all_builds_list: list = []
    for _row in all_builds:
        build_id = Buildid(
            _row[0],
            _row[1],
            _row[2],
            _row[3],
            _row[4],
            _row[5],
            _row[14],
            _row[7],
            _row[6],
            _row[12],
            _row[15],
            _row[10],
            _row[11],
            _row[8],
            _row[9],
            _row[13],
        )
        _dict: dict = {
            "objectno": build_id.build_objectno,
            "build_id": build_id.build_id,
            "build_description": build_id.build_description,
            "build_revisionnumber": build_id.build_revisionnumber,
            "timestamp_active": build_id.timestamp_active,
            "timestamp": build_id.build_timestamp,
            "build_workflow": build_id.build_workflow,
            "department": build_id.department,
            "costcenter": build_id.costcenter,
            "internal_buildid_number": build_id.internal_buildid_number,
            "precheck": build_id.build_precheck,
            "ste": build_id.build_ste,
            "ste_description": build_id.build_ste_description,
            "package": build_id.build_package,
            "package_storage_time_in_days": build_id.package_storage_time,
            "processing_priority": build_id.processing_priority,
        }

        _all_builds_list.append(_dict)

    return _all_builds_list


@logger.catch
def process_build(build_id: dict, customertoken: str) -> None:

    _buildid_hash: str = get_json_sem_hash(build_id)

    _objectno = build_id["objectno"]
    _build_id = build_id["build_id"]
    _build_description = build_id["build_description"]
    _revisionnumber = build_id["build_revisionnumber"]
    _timestamp_active = build_id["timestamp_active"]
    _timestamp = build_id["timestamp"]
    _workflow = build_id["build_workflow"]
    _department = build_id["department"]
    _costcenter = build_id["costcenter"]
    _internal_build_number = build_id["internal_buildid_number"]
    _precheck = build_id["precheck"]
    _ste = build_id["ste"]
    _ste_description = build_id["ste_description"]
    _build_package = build_id["package"]
    _build_package_storage_time = build_id["package_storage_time_in_days"]
    _processing_priority = build_id["processing_priority"]

    _traylist: list = get_active_trays_for_buildid(_objectno)

    if len(_traylist) > 0:

        _bom_content_list: list = get_bom_content(_objectno, _revisionnumber)
        # _machineprogram_list: list = get_machineprograms_for_buildid(_objectno)
        _machineprogram_list: list = []

        check_bom_content_for_assembled_item(_bom_content_list)

        logger.info(f"Processing: {_build_id:<15}| hash: {_buildid_hash} and {_traylist.__len__()} tray(s) and {_bom_content_list.__len__()} component(s)")

        _final_masterdata_dict: dict = {
            "customer": customertoken,
            "objectno": _objectno,
            "build_id": _build_id,
            "buildid_hash": _buildid_hash,
            "build_description": _build_description,
            "build_revisionnumber": _revisionnumber,
            "timestamp_active": _timestamp_active,
            "timestamp": _timestamp,
            "build_workflow": _workflow,
            "department": _department,
            "costcenter": _costcenter,
            "internal_build_number": _internal_build_number,
            "precheck": _precheck,
            "ste": _ste,
            "ste_description": _ste_description,
            "build_package": _build_package,
            "build_package_storage_time": _build_package_storage_time,
            "processing_priority": _processing_priority,
            "machine_programs": _machineprogram_list,
            "trayids": _traylist,
            "buildid_content": _bom_content_list,
        }

        save_tpi_to_database(_final_masterdata_dict, MSSQL_SERVER, POSTGRES_SERVER, customertoken)
        save_tpi_content_to_database(_buildid_hash, _bom_content_list, MSSQL_SERVER)

        for _tray in _traylist:
            delete_tray_from_database(_tray["trayid"], MSSQL_SERVER)

        for _tray in _traylist:
            save_tray_to_database(_buildid_hash, _tray["trayid"], _tray["traystatus"], _tray["defaultstocklocation"], _tray["clipnumber"], MSSQL_SERVER)

        for _line in _bom_content_list:
            if _line["ctype"] == "component":
                process_component(
                    (
                        POSTGRES_SERVER,
                        _line["internal_article_number"],
                        _line["article_hash"],
                        _line["componentname"],
                        _line["componentidentifier"],
                        _line["manufacturer"],
                        _line["manufacturer_description"],
                        _line["component_erpreferencenumber"],
                        0,
                        _line["udi_codes"],
                        _line["processinstructions"],
                        _line["packingnote"],
                        OUTPUT_DIR,
                    )
                )


@logger.catch
def lookup_next_free_start_position(bom_content: list, position: int, iteration: int) -> int:

    # get the next free position that is not in the workplan list
    # ex: workplan post =  15 with 3 articles. The next Free position is the first free position after 15
    # that is not a workplan (not in the workplan list)
    # TODO: add quantity check

    if iteration == 0:
        _next_position = position + 1
    else:
        _next_position = position + iteration

    for _row in bom_content:
        if _next_position == _row["sort"]:
            if _row["ctype"] == "component":
                logger.debug(f"Found component at position {_row['sort']} and iteration {iteration}")
                return _next_position
            else:
                logger.debug(f"Found a workplan at position {_row['sort']} and iteration {iteration}")
                return lookup_next_free_start_position(bom_content, _next_position, iteration + 1)


@timeis
@logger.catch
def cache_article_hash(server: str) -> None:

    _file_name: str = COMPONENT_FILE
    _file_path: str = os.path.join(OUTPUT_DIR, _file_name)

    if not os.path.exists(_file_path):
        func_create_directorys(OUTPUT_DIR)

    _article_hash_list: list = broker.cache_article_hashes(server)
    _article_hash_list_len: int = len(_article_hash_list)

    _df = pd.DataFrame(_article_hash_list)
    _df.to_csv(_file_path, index=False, sep=";", encoding="utf-8-sig", header=False)
    logger.info(f"Saved {_article_hash_list_len} article hashes to {_file_path}")


@logger.catch
def cache_tpi_hash(server: str, customertoken: str) -> list:

    logger.info(f"Start caching TPI hashes for customer {customertoken}")
    _tpi_hash_list: list = cache_builds_for_customer(server, customertoken)
    logger.info(f"Fount {_tpi_hash_list.__len__()} TPI hashes for customer {customertoken}")
    return _tpi_hash_list


@logger.catch
def cache_trayids(server: str, customertoken: str) -> list:

    logger.info(f"Start caching trayids for customer {customertoken}")
    _trayid_list: list = cache_trayids_for_customer(server, customertoken)
    logger.info(f"Found {_trayid_list.__len__()} trayids for customer {customertoken}")
    return _trayid_list

@logger.catch
def get_latest_packing_step() -> None:

    logger.info("Getting latest packing step, this may take a while ⌛...")
    _latest_packing_step_list: list = broker.get_latest_packing_step(POSTGRES_SERVER)

    for tray in _latest_packing_step_list:

        _timestamp = tray[0]
        _trayid = tray[1]

        save_latest_packing_step(_trayid, _timestamp)


@logger.catch
def save_latest_packing_step(trayid: str, timestamp: str) -> None:

    print(f"Saving latest packing step for tray {trayid} with timestamp {timestamp}", end="\r")

    broker.save_latest_packing_step(trayid, timestamp, MSSQL_SERVER, POSTGRES_SERVER)


@logger.catch
def process_customer(customertoken: str) -> None:

    _cache_build_list: list = cache_tpi_hash(POSTGRES_SERVER, customertoken)  # all builds already cached from masterdata_steam
    _cache_tray_list: list = cache_trayids(POSTGRES_SERVER, customertoken)  # all trayids already cached from masterdata_steam

    _all_builds_list: list = get_all_active_buildids_for_customer(customertoken)  # all active builds from spmi for this customer
    _all_tray_list: list = []

    logger.info(f"Getting all trayids for customer {customertoken} and {len(_all_builds_list)} builds")
    for _build in _all_builds_list:

        _traylist: list = get_active_trays_for_buildid(_build['objectno'])

        if len(_traylist) > 0:

            for _tray in _traylist:
                _trayid: str = _tray['trayid']
                _objectno: str = _build['objectno']
                _all_tray_list.append((_trayid, _objectno))

    logger.info(f"Found {_all_tray_list.__len__()} trayids for customer {customertoken}")

    _build_revision_list: list = []
    _cache_revision_list: list = []
    _builds_to_process: list = []
    _builds_to_delete: list = []
    _trays_to_process: list = []
    _trayids_to_delete: list = []

    for line in _all_builds_list:
        _build_revision_list.append((line["build_id"], line["build_revisionnumber"], line["build_description"]))

    for line in _cache_build_list:

        _cache_build_id = line[0]
        _cache_build_revision = line[1]
        _cache_build_description = line[3]

        _cache_revision_list.append((_cache_build_id, _cache_build_revision, _cache_build_description))

    for _build in _build_revision_list:

        _build_id: str = _build[0]
        _build_revisionnumber: int = _build[1]
        _build_description: str = _build[2]

        if _build_id is not None and _build_revisionnumber is not None:

            if (_build_id, _build_revisionnumber, _build_description) not in _cache_revision_list:

                logger.info(f"Build {_build_id:<15} | {_build_description} with revision {_build_revisionnumber} will be imported in masterdata_steam 🟩")
                _builds_to_process.append((_build_id, _build_revisionnumber))

    for _build in _cache_revision_list:

        _build_id: str = _build[0]
        _build_revisionnumber: int = _build[1]
        _build_description: str = _build[2]

        if _build_id is not None and _build_revisionnumber is not None:

            if (_build_id, _build_revisionnumber, _build_description) not in _build_revision_list:

                logger.info(f"Build {_build_id:<15} | {_build_description} with revision {_build_revisionnumber} will be deleted from masterdata_steam 🟥")
                _builds_to_delete.append((_build_id, _build_revisionnumber))

    for _tray in _cache_tray_list:

        _tray_id: str = _tray[0]
        _objectno: int = int(_tray[1])

        if _tray_id is not None:
            if (_tray_id, _objectno) not in _all_tray_list:
                _trayids_to_delete.append(_tray_id)

    for _tray in _all_tray_list:

        _tray_id: str = _tray[0]
        _objectno: int = _tray[1]

        if _tray_id is not None:
            if (_tray_id, _objectno) not in _cache_tray_list:

                _trays_to_process.append((_tray_id, _objectno))

    if len(_builds_to_process) > 0:

        logger.info(f"Found {len(_builds_to_process)} builds to export ▶")

        for _buildid in _all_builds_list:
            if (_buildid["build_id"], _buildid["build_revisionnumber"]) in _builds_to_process:

                _build_id: str = _buildid["build_id"]
                process_build(_buildid, customertoken)

    if len(_builds_to_delete) > 0:

        logger.info(f"Found {len(_builds_to_delete)} builds to delete")

        for _buildid in _cache_build_list:
            if (_buildid[0], _buildid[1]) in _builds_to_delete:

                _build_id: str = _buildid[0]
                _build_revisionnumber: int = _buildid[1]
                _build_hash: str = _buildid[4]

                mark_tpi_as_deleted(_build_id, _build_revisionnumber, MSSQL_SERVER)
                delete_trays_from_database_with_hash(_build_hash, MSSQL_SERVER)
                logger.info(f"Marked build {_build_id} with revision {_build_revisionnumber} as deleted")

    if len(_trayids_to_delete) > 0:

        logger.info(f"Found {len(_trayids_to_delete)} trayids to delete")

        for _trayid in _trayids_to_delete:
            delete_tray_from_database(_trayid, MSSQL_SERVER)
            logger.info(f"Deleted tray {_trayid} from database")

    if len(_trays_to_process) > 0:

        logger.info(f"Found {len(_trays_to_process)} trays to export ▶")

        for _trayid in _trays_to_process:
            # TODO: insert correct values
            save_tray_to_database("", _trayid[0], _trayid[1], "AVAILABLE", "", "", MSSQL_SERVER)
            logger.info(f"Saved tray {_trayid[0]} to database")


@timeis
@logger.catch
def main():

    global CUSTOMERTOKEN
    global POSTGRES_SERVER

    _excluded_customers_per_server = {}
    _excluded_customers_per_server["KPRDCSA01"] = [
        "ACMI",
        "AZ DELTA W",
        "MDT",
        "JNJ",
        "life",
        "MDC",
        "Mons",
        "STRAUMANN",
        "Warq",
        "Zimmer",
        "HHRM Rijs",
    ]
    _excluded_customers_per_server["KPRDCSA02"] = ["ANYS", "AZG", "HAINE", "EC", "EUR"]
    _excluded_customers_per_server["KPRDCSA03"] = ["JNJ"]

    logger.add(
        sys.stdout,
        colorize=True,
        format="<green>{time:HH:mm:ss:SSS}</green> <blue><level>{level}</level></blue>: <level>{message}</level>",
        level="INFO",
    )

    cache_article_hash(MSSQL_SERVER)

    if parse_args() != "":

        _server: str = parse_args()
        _allowed_servers: list = ["KPRDCSA01", "KPRDCSA02", "KPRDCSA03"]

        if _server not in _allowed_servers:
            logger.error(f"Server {_server} is not allowed")
            sys.exit(1)

        POSTGRES_SERVER = _server

        logger.info(f"Started export for server: {POSTGRES_SERVER}")

        _customers_to_process: list = get_active_customers_from_spmi(POSTGRES_SERVER)

        # get_latest_packing_step()

        logger.info(f"Found these active customers to process on server {POSTGRES_SERVER}")

        for _, _customername, _customertoken in _customers_to_process:

            if str(_customertoken) not in _excluded_customers_per_server[POSTGRES_SERVER] and _customertoken is not None:
                logger.info(f"{_server}: {_customername: <12} {_customertoken: <12}")

        for _, _customername, _customertoken in _customers_to_process:

            if str(_customertoken) not in _excluded_customers_per_server[POSTGRES_SERVER] and _customertoken is not None:

                logger.info(f"Started export for customer: {str(_customername)} on server {POSTGRES_SERVER}")

                CUSTOMERTOKEN = _customername

                process_customer(_customertoken)

                logger.info(f"Finished export for customer: {str(_customername)} on server {POSTGRES_SERVER}")


@logger.catch
def parse_args():

    parser = argparse.ArgumentParser(description="Retrieve active builds and trayids for a given customer")

    parser.add_argument(
        "--server",
        "-s",
        type=str,
        help="Spmi Server",
        required=False,
    )

    args = parser.parse_args()
    _server: str = ""

    if args.server is not None:
        _server = args.server.strip()

    return _server


if __name__ == "__main__":

    with logger.contextualize(task="main"):
        try:
            create_logfile(LOG_DIR, LOG_FILE, LOG_LEVEL)
            main()
        except KeyboardInterrupt:
            logger.error("\n\nProgram canceled by user. Exiting...\n")
            sys.exit(0)
