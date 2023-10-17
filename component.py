from loguru import logger
from rich import print

import data.broker as broker
from cls.item_codering import Component_Code
from cls.process_instructions import Process_Instructions
from cls.read_config import read_config
from cls.read_file import check_component_hash_in_cache

config = read_config("config.yaml")
POSTGRES_SERVER = config["POSTGRES_SERVER"]
MSSQL_SERVER = config["MSSQL_SERVER"]


@logger.catch
def save_packingnote(article_hash: str, packingnote: str, server: str):

    broker.save_packingnote(article_hash, packingnote, server)

@logger.catch
def process_component(data: tuple):

    _hashkey: str = data[2]
    _udi_code_list: list = data[9]
    _processinstruction_list: list = data[10]
    _packingnote: str = data[11]
    _output_directory: str = data[12]

    if check_component_hash_in_cache(_output_directory, _hashkey):
        logger.debug(f"Component {_hashkey} already processed")
        return

    if len(data[9]['codes']) > 0:
        logger.warning(f"UDI codes found for component {_hashkey}")
        process_udi_codes(_hashkey, _udi_code_list)

    if len(data[10]['instructions']) > 0:
        process_process_instructions(_hashkey, _processinstruction_list)

    if _packingnote != "NA":
        save_packingnote(_hashkey, _packingnote, MSSQL_SERVER)

    save_article_to_database(data, MSSQL_SERVER)


@logger.catch
def process_process_instructions(hashkey: str, processinstructions: list) -> None:

    _process_list: list = []
    for line in processinstructions['instructions']:
        if line['code']:
            _process_list.append(line['code']) if line['code'] not in _process_list else None

    for _process in _process_list:
        broker.save_processinstruction_for_article(hashkey, _process, MSSQL_SERVER)


@logger.catch
def process_udi_codes(article_hash: str, udi_codes: list):

    _udi_list: list = []
    for udi_code in udi_codes['codes']:
        for _, value in udi_code.items():
            save_udi_code_to_database(article_hash, value, MSSQL_SERVER)


@logger.catch
def save_article_to_database(data: tuple, server: str):

    broker.save_article(data, server)


@logger.catch
def save_udi_code_to_database(article_hash: str, udi_code: str, server: str):

    broker.save_udi_code(article_hash, udi_code.replace("'", "''"), server)


@logger.catch
def get_component_codes(item_objectno: int) -> list:

    _codes_list: list = []
    for _row in broker.retrieve_codes_for_component(item_objectno, POSTGRES_SERVER):
        _codes = Component_Code(_row[0], _row[1], _row[2], _row[3])

        _dict = {
            "udi_code": _codes.udi_code,
        }
        _codes_list.append(_dict)

    return _codes_list


@logger.catch
def get_processinstructions_for_component(warningcode: str) -> list:

    _processinstructions_list: list = []
    if warningcode is not None:
        for _row in broker.retrieve_processinstructions_for_warningcode(
            warningcode, MSSQL_SERVER
        ):
            process_instructions = Process_Instructions(_row[0], _row[1], _row[2])

            _dict = {
                "code": process_instructions.code,
                "language": process_instructions.instruction_language,
                "text": process_instructions.instruction_description,
            }
            _processinstructions_list.append(_dict)
        return _processinstructions_list
    else:
        return []


@logger.catch
def save_processinstruction_to_database(
    article_hash: str, processinstruction: str, server: str
):

    broker.save_processinstruction_for_article(article_hash, processinstruction, server)
