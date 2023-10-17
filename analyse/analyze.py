from loguru import logger
import pandas as pd
from cls.read_config import read_config
import data.connect as connect


config = read_config("config.yaml")
global MSSQL_SERVER
MSSQL_SERVER = config["MSSQL_SERVER"]

@logger.catch
def main():
    print(load_components_from_sql())


@logger.catch
def load_data_from_sql(sql: str, server: str) -> pd.DataFrame:

    """
    load data from sql
    """

    df = pd.read_sql(sql, server)
    return df


@logger.catch
def load_components_from_sql() -> pd.DataFrame:

    conn = connect.connection(MSSQL_SERVER)
    cursor = conn.cursor()
    query = ("""
        select [id]
    ,a.[Server]
    ,[internal_article_number]
    ,a.[article_hash]
    ,[componentname]
    ,[componentidentifier]
    ,[manufacturer]
    ,[manufacturer_description]
    ,[component_erpreferencenumber]
    ,[implant]
    ,b.[udi_code]
    ,c.[instruction_code]
    FROM [masterdata_steam].[dbo].[Components] a
    left join masterdata_steam.dbo.udicodes b on a.article_hash=b.article_id
    left join masterdata_steam.dbo.component_process_instructions c on c.article_hash = a.article_hash
    order by a.internal_article_number
    """
    )
    with conn:
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall()
        query_result = [
            dict(line)
            for line in [
                zip([column[0] for column in cursor.description], row)
                for row in cursor.fetchall()
            ]
        ]
        # df.columns = [
        #     "id",
        #     "server",
        #     "internal_article_number",
        #     "article_hash",
        #     "componentname",
        #     "componentidentifier",
        #     "manufacturer",
        #     "manufacturer_description",
        #     "component_erpreferencenumber",
        #     "implant",
        #     "udi_code",
        #     "instruction_code",
        # ]
    return df

if __name__ == "__main__":

    try:
        main()
    except KeyboardInterrupt:
        logger.error("\n\nProgram canceled by user. Exiting...\n")
        sys.exit(0)
