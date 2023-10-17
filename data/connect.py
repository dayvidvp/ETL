import socket
import sys

import psycopg2
import pyodbc
from loguru import logger
from psycopg2 import OperationalError
from rich.console import Console
from cls.read_config import read_config

console = Console()


@logger.catch
def check_host_status(server, port, _has_run=[]):
    """check the host server if the socket is open"""

    if _has_run:
        return
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.settimeout(1)
        server_socket.connect((server, int(port)))
        server_socket.shutdown(2)
        server_socket.close()
        if socket.timeout == "timed out":
            logger.error(f"Connection to {server}:{port} timed out")
            sys.exit(1)
        _has_run.append(1)
    except socket.timeout as error:
        logger.error(error)
        sys.exit(1)
    except socket.gaierror as error:
        logger.error(error)
        sys.exit(1)

@logger.catch
def show_psycopg2_exception(err):
    err_type, traceback = sys.exc_info()
    line_n = traceback.tb_lineno
    logger.error("\npsycopg2 ERROR:", err, "on line number:", line_n)
    logger.error("psycopg2 traceback:", traceback, "-- type:", err_type)
    logger.error("\nextensions.Diagnostics:", err.diag)
    logger.error("pgerror:", err.pgerror)
    logger.error("pgcode:", err.pgcode, "\n")


class Connect_to_db:
    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        password: str,
        port: int,
        connectiontype: str,
    ):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connectiontype = connectiontype
        self.conn = ""

    def set_connection_string(self):
        try:
            if self.connectiontype == "POSTGRESSQL":
                check_host_status(self.host, self.port)
                self.conn = psycopg2.connect(
                    dbname=self.database,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                )
                return self.conn

            elif self.connectiontype == "MSSQL":
                check_host_status(self.host, self.port)
                try:
                    self.conn = pyodbc.connect(
                        "DRIVER={SQL Server};SERVER="
                        + self.host
                        + ";DATABASE="
                        + self.database
                        + ";Trusted_Connection=yes",
                        timeout=15,
                        autocommit=True,
                    )
                    return self.conn
                except KeyboardInterrupt:
                    logger.error("KeyboardInterrupt")
                    sys.exit(1)

        except OperationalError as err:
            show_psycopg2_exception(err)
            sys.exit(1)
        except pyodbc.Error as ex:
            sqlstate = ex.args[1]
            sqlstate = sqlstate.split(".")
            logger.error(sqlstate[-3])
            sys.exit(1)


@logger.catch
def set_config_parameters(server: str):

    config = read_config("ServerConfigs.yaml")

    host = (config[server]["host"],)
    database = (config[server]["database"],)
    user = (config[server]["user"],)
    password = (config[server]["password"],)
    port = config[server]["port"]
    connectiontype = config[server]["connectiontype"]

    return host, database, user, password, port, connectiontype


@logger.catch
def connection(server):

    (
        host,
        database,
        user,
        password,
        port,
        connectiontype,
    ) = set_config_parameters(server.upper())
    try:
        connection = Connect_to_db(
            host[0],
            database[0],
            user[0],
            password[0],
            port,
            connectiontype,
        )
        cnx = connection.set_connection_string()
        return cnx
    except Exception as e:
        logger.error(e)
        sys.exit(1)
