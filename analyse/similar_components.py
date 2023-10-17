import re
import time
from collections import deque
from functools import lru_cache
import pandas as pd
import pyodbc
from fuzzywuzzy import fuzz
from loguru import logger
from matplotlib import pyplot as plt
from rich import print
import concurrent.futures
from concurrent.futures import as_completed

MSSQL_SERVER = "STRDVPDBSQL01"
DATABASE_NAME = "masterdata_steam"

conn = pyodbc.connect(
    "DRIVER={SQL Server};SERVER="
    + MSSQL_SERVER
    + ";DATABASE="
    + DATABASE_NAME
    + ";Trusted_Connection=yes",
    timeout=15,
)


@logger.catch
def timeis(func):
    '''Decorator that reports the execution time.'''

    def wrap(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()

        print(f"{func.__name__}: Took {(end - start).__round__(4)} seconds {'':<250}", end="\r")
        return result
    return wrap

@logger.catch
@timeis
def load_components_from_sql() -> pd.DataFrame:

    print(f"Loading components from SQL Server... {'':<200}")
    cursor = conn.cursor()
    query = """
    select
    comp.[Server]
    ,comp.[internal_article_number]
    ,comp.[article_hash]
    ,comp.[componentname]
    ,comp.[componentidentifier]
    ,comp.[manufacturer]
    ,comp.[manufacturer_description]
    ,comp.[component_erpreferencenumber]
    ,comp.[implant]
    FROM [masterdata_steam].[dbo].[Components] comp
    order by comp.[Server]
    """
    with conn:
        cursor.execute(query)
        query_result = [
            dict(line)
            for line in [
                zip([column[0] for column in cursor.description], row)
                for row in cursor.fetchall()
            ]
        ]
        df = query_result
    return df


@lru_cache()
def process_manufacturers(manufacturers: pd.Series) -> pd.DataFrame:
    _similar_manufacturers = {}
    _count = len(manufacturers)
    # _count = 100

    print(f"{_count} manufacturers to check")
    _ratio: int = 85
    _queue_one = deque(manufacturers.keys())

    # compare all manufacturers with each other and pop them from the queue if they are similar
    for _ in range(_count):
        _queue_len = len(_queue_one)
        if _queue_len == 0:
            break
        _manufacturer_one = _queue_one.popleft()

        for _manufacturer_two in _queue_one:
            _similarity = fuzz.WRatio(_manufacturer_one, _manufacturer_two)
            if _similarity >= _ratio:
                if _manufacturer_one not in _similar_manufacturers:
                    _similar_manufacturers[_manufacturer_one] = []
                    print(f"L1: create {_manufacturer_one}", end="\r")
                if _manufacturer_two not in _similar_manufacturers[_manufacturer_one]:
                    _similar_manufacturers[_manufacturer_one].append(_manufacturer_two)
                    print(
                        f"L2: {_manufacturer_one} and {_manufacturer_two} are similar with {_similarity}% queue len: {_queue_len}",
                        end="\r",
                    )
                _queue_one.remove((_manufacturer_two))
                break

    print(" " * 200, end="\r")
    print(
        f"{len(_similar_manufacturers)} similar manufacturers found with {_ratio}% similarity"
    )

    df_similar_manufacturers = pd.DataFrame.from_dict(
        _similar_manufacturers, orient="index"
    )
    df_similar_manufacturers.to_csv(
        f"similar_manufacturers_with{_ratio}_ratio.csv", sep=";"
    )


@logger.catch
def save_similar_component(
    master_article_hash: str, article_hash: str, id_linked: bool, name_linked: bool
) -> None:

    cursor = conn.cursor()
    query = f"""
            if not exists (
            select master_article_hash
                from dbo.similar_components
            where master_article_hash = '{master_article_hash}'
            and article_hash = '{article_hash}'
            and id_linked = {0 if id_linked == False else 1}
            and name_linked = {0 if name_linked == False else 1}
            )
            insert into dbo.similar_components
            (
            [master_article_hash]
            , [article_hash]
            , [id_linked]
            , [name_linked]
            )
            values
            (
            '{master_article_hash}',
            '{article_hash}',
            { 0 if id_linked == False else 1},
            { 0 if name_linked == False else 1}
            )
            """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()


@logger.catch
def cache_previous_processed_article_hashes() -> list:

    _key_list = []
    cursor = conn.cursor()
    query = f"""
        SELECT distinct master_article_hash from masterdata_steam.dbo.similar_components
            """
    with conn.cursor() as cursor:
        cursor.execute(query)
        for row in cursor:
            _key_list.append(row[0])

    return _key_list


@logger.catch
def compare_component_id(
    componentidentifier_one: str,
    componentidentifier_one_hash: str,
    componentidentifier_two: str,
    componentidentifier_two_hash: str,
) -> bool:

    _compid_similarity = fuzz.ratio(componentidentifier_one, componentidentifier_two)
    if _compid_similarity >= RATIO:
        save_similar_component(
            str(componentidentifier_one_hash),
            str(componentidentifier_two_hash),
            True,
            False,
        )
        print(f"ID: {componentidentifier_one} and {componentidentifier_two} are similar with {_compid_similarity}% {'':<200}", end="\r")
        return True
    else:
        return False


@logger.catch
def compare_component_name(
    componentname_one: str,
    componentname_one_hash: str,
    componentname_two: str,
    componentname_two_hash: str,
) -> bool:

    _compname_similarity = fuzz.token_sort_ratio(componentname_one, componentname_two)
    if _compname_similarity >= RATIO:
        save_similar_component(
            str(componentname_one_hash),
            str(componentname_two_hash),
            False,
            True,
        )
        print(f"NAME: {componentname_one} and {componentname_two} are similar with {_compname_similarity}%{'':<200}", end="\r")
        return True
    else:
        return False


@logger.catch
def process_component(lookup_dict: dict, source_components_dict: dict) -> None:

    global RATIO
    RATIO = 85

    _count = len(source_components_dict)

    for _ in range(_count):

        _t = source_components_dict.popitem()
        _componentidentifier_one = _t[1][0]
        _componentidentifier_one_hash = _t[0]
        _componentname_one = _t[1][1]

        print(
            f"Processing: [dark_green]{_componentidentifier_one}[/] [white]{_componentname_one}[/] [dark_orange]{_componentidentifier_one_hash}[/]"
            f" remaining: Source:[dark_orange]{len(source_components_dict)}[/] Lookup:[dark_orange]{len(lookup_dict)}[/] {'':<200}", end="\r"
        )

        # check if the componentid is in _comp_id_lookup_dict
        for _componentidentifier_two_hash, _t in lookup_dict.items():

            _componentidentifier_two = _t[0]
            _componentname_two = _t[1]

            if _componentidentifier_one == _componentidentifier_two:
                continue
            if _componentname_one == _componentname_two:
                continue

            _compid_similarity = fuzz.ratio(_componentidentifier_one, _componentidentifier_two)
            _compname_similarity = fuzz.token_sort_ratio(_componentname_one, _componentname_two)

            if _compid_similarity >= RATIO:
                save_similar_component(str(_componentidentifier_one_hash), str(_componentidentifier_two_hash), True, False,)

            if _compname_similarity >= RATIO:
                save_similar_component(str(_componentidentifier_one_hash), str(_componentidentifier_two_hash), False, True,)


@logger.catch
@timeis
def main():

    _components_df = pd.DataFrame(load_components_from_sql())
    # _components_df.to_pickle("components.pkl", compression="gzip")
    # _components_df.to_csv("all_components.csv", index=False, encoding="utf-8", sep=";")
    print("\n")
    print(f"{len(_components_df)} components loaded")

    # manufacturers = (
    #     _components_df.groupby(["manufacturer_description"])["manufacturer_description"]
    #     .count()
    #     .sort_values(ascending=False)
    # )
    # manufacturers.explode().to_csv(
    #     "manufacturer_count.csv",
    #     sep=";",
    #     index_label=["manufacturer_description", "count"],
    # )
    # _df_manufacturer_count = (
    #     manufacturers.sort_values(ascending=False).explode().to_frame()
    # )

    # _manufacturers_with_low_component_count = _df_manufacturer_count.where(
    #     _df_manufacturer_count < 10
    # ).dropna()
    # _manufacturers_with_low_component_count.to_csv(
    #     "manufacturer_with_low_component_count.csv",
    #     sep=";",
    #     index_label=["manufacturer_description", "count"],
    # )
    # print(_manufacturers_with_low_component_count)
    # process_manufacturers(manufacturers)

    _unique_componentidentifiers = _components_df.groupby(["componentidentifier"])["componentidentifier"].count()
    _unique_componentidentifiers_dict = (_unique_componentidentifiers.sort_values(ascending=False).explode().to_dict())
    _unique_componentidentifiers_df = pd.DataFrame.from_dict(_unique_componentidentifiers_dict, orient="index", columns=["componentidentifier"])
    # _unique_componentidentifiers_df.to_csv("componentidentifier_count.csv", sep=";", index_label=["componentidentifier", "count"])

    print(f"{len(_unique_componentidentifiers)} unique componentidentifiers found")

    # _components_df.query("componentidentifier.str.startswith('GR')").to_csv("components_with_gr.csv", sep=";", encoding="utf-8")
    # _components_df.query("componentidentifier.str.startswith('IR')").to_csv("components_with_ir.csv", sep=";", encoding="utf-8")

    _filterd_component_identifiers = _components_df.copy()

    _filterd_component_identifiers = _filterd_component_identifiers[
        ~(_filterd_component_identifiers["componentidentifier"].str.startswith("GR"))
        & ~(_filterd_component_identifiers["componentidentifier"].str.startswith("IR"))
        # & ~(_filterd_component_identifiers['componentidentifier'].str.startswith('Unk'))
        # & ~(_filterd_component_identifiers['componentidentifier'].str.startswith('unknown'))
        # & ~(_filterd_component_identifiers['componentidentifier'].str.startswith('Unknown'))
        # & ~(_filterd_component_identifiers['componentidentifier'].str.startswith('unk'))
    ]

    # _filterd_component_identifiers.to_csv("components_without_gr_ir.csv", sep=";", index_label=["componentidentifier", "count"])
    _filterd_component_identifiers.drop(columns=["manufacturer"], inplace=True)
    _filterd_component_identifiers.drop(columns=["manufacturer_description"], inplace=True)
    _filterd_component_identifiers.drop(columns=["implant"], inplace=True)
    _filterd_component_identifiers.drop(columns=["component_erpreferencenumber"], inplace=True)

    _comp_dict = {}
    for _, row in _filterd_component_identifiers.iterrows():
        if row["article_hash"] not in _comp_dict:
            _comp_dict[row["article_hash"]] = []
            _comp_dict[row["article_hash"]].append(row["componentidentifier"])
            _comp_dict[row["article_hash"]].append(row["componentname"])
            _comp_dict[row["article_hash"]].append(row["Server"])

    _lookup_dict = {}
    for key, value in _comp_dict.items():
        _lookup_dict[key] = value

    _check_dict = _lookup_dict.copy()

    _previous_processed_article_hashes = cache_previous_processed_article_hashes()
    # remove the previous processed article hashes from the lookup dict

    _source_components_dict = {}

    for key, value in _check_dict.items():
        if key not in _previous_processed_article_hashes:
            _source_components_dict[key] = value

    print(f"Number of articles to process: [green]{len(_check_dict)}[/] before removing the previous processed articles")
    print(f"Number of articles to process: [green]{len(_source_components_dict)}[/], Number of articles already processed: [green]{len(_previous_processed_article_hashes)}[/]")

    process_component(_lookup_dict, _source_components_dict)


if __name__ == "__main__":

    try:
        main()
    except Exception as e:
        print(e)
    except KeyboardInterrupt:
        print("[red]User cancelled the process[/]")
