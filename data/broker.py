from loguru import logger
from rich.console import Console

import data.query as query

console = Console()


@logger.catch
def get_latest_packing_step(server: str) -> int:
    """
    get the latest packing step for a given buildid
    """

    latest_packing_step = query.get_latest_packing_step(server)

    return latest_packing_step


@logger.catch
def save_latest_packing_step(trayid: str, timestamp: str, server: str, spmiserver: str) -> None:
    """
    save the latest packing step for a given buildid
    """

    query.save_latest_packing_step(trayid, timestamp, server, spmiserver)

    return None

@logger.catch
def save_packingnote(article_hash: str, packingnote: str, server: str):

    query.save_packingnote_to_database(article_hash, packingnote, server)


@logger.catch
def retrieve_active_builds_for_customer(customertoken: str, server: str) -> list:
    """
    retrieve active builds for customer
    """
    builds = query.retrieve_active_builds_for_customer_spmi(customertoken, server)
    return builds


@logger.catch
def retrieve_trayids_per_build(objectno: int, server: str) -> list:
    """
    retrieve trayids per build
    """
    trayids = query.retrieve_trayid_from_build(objectno, server)
    return trayids


@logger.catch
def get_tray_count_per_tpi(objectno: int, server: str) -> int:
    """
    get tray count per TPI
    """
    traycount = query.get_tray_count_per_tpi(objectno, server)
    return traycount


@logger.catch
def retrieve_bom_for_build_and_version(
    objectno: int, revisionnumber: int, server: str
) -> list:
    """
    retrieve the bom from spmi for a given trayid and revisionnumber
    """
    bom_list = query.retrieve_bom_from_objectno_version(
        objectno, revisionnumber, server
    )
    return bom_list


@logger.catch
def retrieve_workplans_for_build_and_version(
    objectno: int, revisionnumber: int, server: str
) -> list:
    """
    retrieve the workplans for a build according to the revisionnumber
    """
    bom_workplans = query.retrieve_workplan_data_from_tpi_ojectno_revision(
        objectno, revisionnumber, server
    )
    return bom_workplans


@logger.catch
def retrieve_machine_programs_for_buildid(objectno: int, server: str) -> list:
    """
    retrieve the machine programs for a build
    """
    machine_programs = query.retrieve_machine_programs_for_buildid(objectno, server)
    return machine_programs


@logger.catch
def retrieve_processinstructions_for_warningcode(warningcode: str, server) -> list:
    """
    retrieve the processinstructions for a warningcode
    """
    processinstructions = query.retrieve_processinstruction_for_code(
        warningcode, server
    )
    return processinstructions


@logger.catch
def retrieve_codes_for_component(item_objectno: int, server: str) -> list:
    """
    retrieve the dm codes for a component
    """
    codes = query.retrieve_components_codes(item_objectno, server)
    return codes


@logger.catch
def save_article(data: tuple, server: str) -> None:

    """
    save the article to the database
    """
    query.save_article(data, server)


@logger.catch
def save_udi_code(article_hash: str, udi_code: str, server: str) -> None:

    """
    save the udicode to the database
    """
    query.save_udi_code(article_hash, udi_code, server)


@logger.catch
def save_processinstruction_for_article(
    article_hash: str, instruction_code: str, server: str
) -> None:

    """
    save the processinstructions for an article
    """
    query.save_processinstruction_for_article(article_hash, instruction_code, server)


@logger.catch
def retrieve_active_customers_from_spmi(server: str) -> list:

    """
    retrieve the active customers from spmi
    """
    return query.retrieve_active_customers_from_spmi(server)


@logger.catch
def save_component_relation(
    spmi_server: str,
    article_hash: str,
    customertoken: str,
    tpi_objectno: int,
    tpi_number: str,
    tpi_description: str,
    server: str,
) -> None:

    """
    save the component relation to the database
    """
    query.save_component_relation(
        spmi_server, article_hash, customertoken, tpi_objectno, tpi_number, tpi_description, server
    )


@logger.catch
def save_component_assembly(compound_article_id: str, article_hash: str, pieces: int, server: str) -> None:

    """
    save the component assembly to the database
    """
    query.save_component_assembly(compound_article_id, article_hash, pieces, server)


@logger.catch
def cache_article_hashes(server: str) -> None:

    """
    cache the article hash in the database
    """
    return query.cache_article_hashes(server)


