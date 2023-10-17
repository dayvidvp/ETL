from loguru import logger
from rich import print

import data.broker as broker
from cls.read_config import read_config

config = read_config("config.yaml")
POSTGRES_SERVER = config["POSTGRES_SERVER"]
MSSQL_SERVER = config["MSSQL_SERVER"]

@logger.catch
def get_active_customers_from_spmi(server: str) -> list:
    """
    retrieve the active customers from spmi
    """
    _customers: list = broker.retrieve_active_customers_from_spmi(server)
    return _customers
