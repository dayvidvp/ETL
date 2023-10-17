# !usr/bin/env python3
import sys

from loguru import logger

import data.connect as connect


@logger.catch
def retrieve_active_builds_for_customer_spmi(customertoken: str, server: str) -> list:
    """
    Retrieve all active builds for a given customer.
    """
    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute(
            "SELECT DISTINCT"
            " CAST(TPI.objectNo AS BIGINT) AS build_Objectno"
            " , COALESCE(TPI.ERPReferenceNumber, '') AS build_id"
            " , TPI.description AS build_description"
            " , CAST(TPI.revisionNumber AS INT) AS build_revision"
            " , TPI.timestampactiveset AS build_timestamp_acctiveset"
            " , tpi.timestamp as build_timestamp"
            " , CostCenter.description AS build_costcenter"
            " , Department.description AS build_department"
            " , COALESCE(ContainerType.description,'') AS build_package"
            " , CAST(COALESCE(storagetime.storeagetimeindays, StoragetimeCustomer.storeagetimeindays, 0) AS INT)"
            " AS package_storage_time"
            " , CAST(COALESCE(TPI.STE,0) AS DECIMAL(25,2)) AS build_ste"
            " , REPLACE(COALESCE(EffortCategory.description, ''), ';', '-') AS build_ste_description"
            " , TPI.TPINumber as internal_build_number"
            " , CASE "
            " WHEN  tpi.processingpriority % 10000000000 = 1 THEN 'NORMAL'"
            " WHEN  tpi.processingpriority % 10000000000 = 2 THEN 'HIGH'"
            " ELSE 'UNDEFINED'"
            " END AS processing_priority"
            " , COALESCE("
            " workflowconfiguration.description,"
            " ("
            " SELECT DISTINCT description"
            " FROM workflowconfiguration"
            " WHERE deleted = 0"
            " AND modifiedat = -1"
            " AND standardconfiguration = 1"
            " LIMIT 1"
            " )"
            " ) AS default_workflowconfiguration"
            " , tpi.precheck as precheck"
            " FROM TPI"
            " JOIN CostCenter ON TPI.costCenter = CostCenter.objectNo"
            " AND CostCenter.deleted = 0 AND CostCenter.modifiedAt = -1"
            " JOIN Department ON CostCenter.department = Department.objectNo"
            " AND Department.deleted = 0 AND Department.modifiedAt = -1"
            " JOIN customer ON customer.objectno = department.customer"
            " AND customer.deleted=0 AND customer.modifiedat=-1"
            " LEFT JOIN TRAY ON tray.tpi = tpi.objectno AND tray.Modifiedat = -1"
            " AND tray.deleted = 0 AND tray.traystate % 10000000000 IN (100, 200, 300, 500)"
            " LEFT JOIN ContainerType ON TPI.containerType = ContainerType.objectNo"
            " AND ContainerType.deleted = 0 AND ContainerType.modifiedAt = -1"
            " LEFT JOIN EffortCategory ON TPI.effortCategory = EffortCategory.objectNo"
            " AND EffortCategory.deleted = 0 AND EffortCategory.modifiedAt = -1"
            " LEFT JOIN containertypecustomer ON containertypecustomer.containertype = containertype.objectno"
            " AND containertypecustomer.customer = customer.objectNo AND containertypecustomer.deleted = 0"
            " AND containertypecustomer.modifiedat = -1"
            " LEFT JOIN storagetime ON containertypecustomer.storagetime = storagetime.objectno"
            " AND storagetime.deleted = 0 AND storagetime.modifiedat = -1"
            " LEFT JOIN storagetime AS StoragetimeCustomer"
            " ON customer.defaultstoragetime = StoragetimeCustomer.objectno"
            " AND StoragetimeCustomer.deleted = 0 AND StoragetimeCustomer.modifiedat = -1"
            " LEFT JOIN workflowconfiguration on  tpi.standardworkflowconfiguration = workflowconfiguration.objectno"
            " AND workflowconfiguration.deleted = 0 AND workflowconfiguration.modifiedat = -1"
            " LEFT JOIN applicationuser on TPI.modificationuser = applicationuser.objectno"
            " AND applicationuser.deleted = 0 AND applicationuser.modifiedat = -1"
            f" WHERE customer.token = '{customertoken}' AND customer.modifiedat = -1 AND customer.deleted = 0 "
            # " WHERE customer.token = 'Ath' AND customer.modifiedat = -1 AND customer.deleted = 0 "
            # " AND TPI.objectno = 10010002376142"
            " AND TPI.modifiedAt = -1"
            " AND TPI.deleted = 0"
            " AND TPI.TPIPositionType = 1"
            " AND TPI.TPIType = 1"
            " AND TPI.active = 1"
            " AND TPI.implant = 0"
            " AND (Tray.tpi is not null OR customer.token = 'JNJ')"
            " GROUP BY TPI.objectNo,"
            " TPI.ERPReferenceNumber,"
            " TPI.description,"
            " TPI.revisionNumber,"
            " TPI.timestampactiveset,"
            " tpi.timestamp,"
            " CostCenter.description,"
            " Department.description,"
            " ContainerType.description,"
            " storagetime.storeagetimeindays,"
            " StoragetimeCustomer.storeagetimeindays,"
            " TPI.STE,"
            " EffortCategory.description,"
            " TPI.TPINumber,"
            " CASE "
            " WHEN  tpi.processingpriority % 10000000000 = 1 THEN 'NORMAL'"
            " WHEN  tpi.processingpriority % 10000000000 = 2 THEN 'HIGH'"
            " ELSE 'UNDEFINED'"
            " END,"
            " workflowconfiguration.description,"
            " tpi.precheck"
            " ORDER BY TPI.objectNo DESC"
        )
        return cursor.fetchall()


@logger.catch
def retrieve_trayid_from_build(objectno: int, server: str) -> list:
    """
    Retrieve the trayid(s) from a build.
    """
    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute(
            f"""
            SELECT DISTINCT
            tray.traynumber as TrayId,
            'AVAILABLE' as TrayStatus,
            COALESCE(stock.description,'') AS TrayIdDefaultStockLocation,
            COALESCE(clip.number, '') As TrayIdClipNumber
            FROM tray
            LEFT JOIN CLIP ON clip.tray = tray.objectno
            AND clip.deleted = 0 AND clip.modifiedat = -1
            AND clip.cliptype % 10000000000 = 4
            LEFT JOIN stock ON stock.objectno = tray.defaultstock
            AND stock.deleted=0 AND stock.modifiedat=-1
            WHERE tray.tpi = '{objectno}'
            and tray.traystate % 10000000000 IN (100, 500)
            and tray.modifiedat=-1
            and tray.deleted= 0
            GROUP BY  tray.traynumber,
            stock.description,
            clip.number
"""
        )
        return cursor.fetchall()


@logger.catch
def retrieve_bom_from_objectno_version(objectno: int, revisionnumber: int, server: str) -> list:
    """
    Retrieve the bom from a build.
    """
    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute(
            f"""SELECT
            COALESCE(Item.description,'NA') AS componentname
            , COALESCE(Item.manufacturerItemNumber,'NA') AS componentidentifier
            , CAST(TPI.tpipositiontype AS BIGINT) AS positiontype
            , CAST(TPI.positionid AS BIGINT) AS PositionId
            , COALESCE(Manufacturer.token,'NA') AS ManufacturerToken
            , COALESCE(Manufacturer.description,'NA') AS manufacturer_description
            , CAST(TPI.revisionnumber AS BIGINT) AS RevisionNumber
            , CAST(COALESCE(TPI.countOfPieces, 0) AS BIGINT) AS standard_quantity
            , COALESCE(Item.ERPReferenceNumber,'NA') AS ErpReferenceNumber
            , COALESCE(Item.packingNote,'NA') AS PackingNote
            , COALESCE(TPI.positionComment,'NA') AS position_comment
            , CAST(TPI.sorting AS BIGINT) AS Sort
            , Item.itemNumber AS internal_article_number
            , TPI.tpinumber AS internal_build_number
            , Item.WarningText AS warningtext
            , Item.objectno AS item_objectno
            ,tpi.AlternateItem as is_alternative
            , COALESCE(TPI.positionDescription, 'NA') AS workplan_description
            ,case when tpipositiontype = 3 then 'component'
            else 'workplan' end as ctype
            from TPI
            left JOIN Item ON TPI.item = Item.objectNo AND Item.modifiedAt = -1 AND Item.deleted = 0
            left JOIN Manufacturer ON Item.manufacturer = Manufacturer.objectNo AND
            Manufacturer.modifiedAt = -1 AND Manufacturer.deleted = 0
            where tpi.objectno= {objectno} and revisionnumber= {revisionnumber}
            and tpi.active = 1 and tpi.tpipositiontype in (2,3)
            GROUP BY
            TPI.ERPReferenceNumber
            , TPI.revisionnumber
            ,Item.description
            , Item.manufacturerItemNumber
            , TPI.positionId
            , TPI.AlternateItem
            , TPI.countOfPieces
            , TPI.positionComment
            , TPI.sorting
            , Item.ERPReferenceNumber
            , Item.packingNote
            , Item.itemNumber
            , Item.warningtext
            , Manufacturer.description
            , Manufacturer.token
            ,TPI.tpinumber
            ,Item.objectno
            ,TPI.tpipositiontype
            ,TPI.positionDescription
            ORDER BY
            TPI.ERPReferenceNumber
            ,TPI.sorting
            ,tpi.AlternateItem
            """
        )
        return cursor.fetchall()


@logger.catch
def retrieve_customer_data(customertoken: str, server: str) -> list:
    """
    Retrieve the bom from a build.
    """
    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute(
            "SELECT cust.CustomerToken"
            " , cust.CustomerDescripton"
            " , cust.CustomerGroup"
            " , cupl.PlantId"
            " , cupl.CustomerObjectNumber"
            " FROM dbo.tbl_Customer cust"
            " join dbo.tbl_customer_plant cupl on cupl.Customertoken = cust.CustomerToken"
            f" where cust.CustomerToken='{customertoken}'"
        )
        return cursor.fetchall()


@logger.catch
def retrieve_workplan_data_from_tpi_ojectno_revision(objectno: int, revisionnumber: int, server: str) -> list:
    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute(
            "SELECT distinct"
            " TPI.sorting AS Sort"
            " , COALESCE(TPI.positionDescription, '') AS workplan_description"
            " FROM"
            " TPI"
            " WHERE"
            " TPI.modifiedAt = -1"
            " AND"
            " TPI.deleted = 0"
            " AND"
            " TPI.ACTIVE = 1"
            " AND"
            f" TPI.objectno = {objectno}"
            " AND"
            f" TPI.revisionnumber = {revisionnumber}"
            " AND"
            " TPI.TPIPositionType = 2"
            " ORDER"
            " BY"
            " TPI.sorting"
        )
        return cursor.fetchall()


@logger.catch
def retrieve_machine_programs_for_buildid(objectno: int, server: str) -> list:
    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute(
            "SELECT"
            " MachineProgram.description Machine_program_description"
            " , CASE"
            " WHEN MachineProgramType.objectNo % 10000000000 = 1 THEN 'Sterilizer'"
            " WHEN MachineProgramType.objectNo % 10000000000 = 2 THEN 'Washer'"
            " END AS machine_program_type"
            " FROM MachineProgramTPI"
            " JOIN TPI ON MachineProgramTPI.TPI = TPI.objectno AND TPI.deleted = 0 AND TPI.modifiedat = -1 AND"
            " TPI.active = 1 AND TPI.TPIPositionType = 1 AND TPI.TPIType = 1"
            " JOIN costcenter ON costcenter.objectno = tpi.costcenter AND costcenter.deleted=0 AND"
            " costcenter.modifiedat=-1"
            " JOIN department ON department.objectno = costcenter.department AND department.deleted=0 AND"
            " department.modifiedat=-1"
            " JOIN customer ON customer.objectno = department.customer AND customer.deleted=0 AND"
            " customer.modifiedat=-1"
            " LEFT JOIN MachineProgram ON MachineProgramTPI.machineProgram=MachineProgram.objectNo AND"
            " MachineProgram.deleted=0 AND MachineProgram.modifiedAt=-1"
            " LEFT JOIN MachineProgramType ON MachineProgram.machineProgramType=MachineProgramType.objectNo AND"
            " MachineProgramType.deleted=0 AND MachineProgramType.modifiedAt=-1"
            " WHERE customer.modifiedat = -1 AND customer.deleted = 0"
            " AND MachineProgramType.objectNo % 10000000000 IN (1,2)"
            " AND MachineProgramTPI.modifiedAt=-1"
            " AND MachineProgramTPI.deleted=0"
            f" and tpi.objectno= {objectno}"
            " ORDER BY  TPI.ERPReferenceNumber,"
            " CASE"
            " WHEN MachineProgramType.objectNo % 10000000000 = 1 THEN 'Sterilizer'"
            " WHEN MachineProgramType.objectNo % 10000000000 = 2 THEN 'Washer'"
            " END"
        )
        return cursor.fetchall()


@logger.catch
def retrieve_processinstruction_for_code(warningcode: str, server: str) -> list:
    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute("select code, [Language], [Description] from ProcessInstructions_Desc" f" where code = '{warningcode}'")
        return cursor.fetchall()


@logger.catch
def retrieve_components_codes(item_objectno: str, server: str) -> list:
    """
    Retrieve the codes for a component
    """
    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute(
            f"""
            SELECT
            COALESCE(I.InstrumentNumber, '0') AS UDI_code
            FROM Instrument I
            WHERE
            I.modifiedat = -1
            AND I.deleted = 0
            and I.itemnumber='{item_objectno}'
            ORDER BY I.InstrumentNumber
            """
        )
        return cursor.fetchall()


@logger.catch
def save_article(data: tuple, server: str) -> None:

    _server = data[0]
    _internal_article_number = data[1]
    _article_hash = data[2]
    _componentname = data[3].replace("'", "''")  # escape single quote
    _componentidentifier = data[4]
    _manufacturer = data[5].replace("'", "''")
    _manufacturer_description = data[6].replace("'", "''")
    _component_erp_reference_number = data[7]
    _implant = data[8]

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        query = f"""
            IF NOT EXISTS (
                select 1 from masterdata_steam.dbo.Components
                where article_hash = '{_article_hash}'
                )
            insert into masterdata_steam.dbo.Components
            (
            [Server]
            , [internal_article_number]
            , [article_hash]
            , [componentname]
            , [componentidentifier]
            , [manufacturer]
            , [manufacturer_description]
            , [component_erpreferencenumber]
            , [implant]
            )
            values
            (
            '{_server}'
            ,'{_internal_article_number}'
            ,'{_article_hash}'
            ,'{_componentname}'
            ,'{_componentidentifier}'
            ,'{_manufacturer}'
            ,'{_manufacturer_description}'
            ,'{_component_erp_reference_number}'
            , {_implant}
            );
            """
        cursor.execute(query)
        conn.commit()


@logger.catch
def save_udi_code(article_hash: str, udi_code: str, server: str) -> None:

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            IF NOT EXISTS (
                select [article_id] from masterdata_steam.dbo.udicodes
                where article_id = '{article_hash}'
                and udi_code = '{udi_code}'
                )
            insert into masterdata_steam.dbo.udicodes
                    (
                    [article_id]
                    , [udi_code]
                    )
                    values
                    ('{article_hash}'
                    ,'{udi_code}'
                    );
                    """
        )
        conn.commit()


@logger.catch
def save_processinstruction_for_article(
    article_hash: str,
    instruction_code: str,
    server: str,
) -> None:

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            IF NOT EXISTS (
                select [article_hash] from masterdata_steam.dbo.component_process_instructions
                where article_hash = '{article_hash}'
                and instruction_code = '{instruction_code}'
                )
            insert into masterdata_steam.dbo.component_process_instructions
                (
                [article_hash]
                , [instruction_code]
                )
                values
                ('{article_hash}'
                ,'{instruction_code}'
                );
                """
        )
        conn.commit()


@logger.catch
def retrieve_active_customers_from_spmi(server: str):

    conn = connect.connection(server)
    cursor = conn.cursor()
    with conn:
        cursor.execute("SELECT objectno, description, token FROM customer where modifiedat =-1 and deleted=0")
        return cursor.fetchall()

 
@logger.catch
def save_component_assembly(compound_article_id: str, article_hash: str, pieces: int, server: str):

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        query = f"""
            IF NOT EXISTS (
            SELECT compound_article_id from masterdata_steam.dbo.component_assembly
            where compound_article_id = '{compound_article_id}'
            and article_hash = '{article_hash}'
            and pieces = {pieces}
            )
            insert into masterdata_steam.dbo.component_assembly
            (
            [compound_article_id]
            , [article_hash]
            , [pieces]
            )
            values
            (
            '{compound_article_id}'
            , '{article_hash}'
            , '{pieces}'
            );
            """
        cursor.execute(query)
        conn.commit()


@logger.catch
def cache_article_hashes(server: str) -> list:

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        cursor.execute("SELECT distinct article_hash FROM masterdata_steam.dbo.Components")
        return cursor.fetchall()


@logger.catch
def cache_builds_for_customer(server: str, customertoken) -> list:

    conn = connect.connection("STRDVPDBSQL01")
    with conn.cursor() as cursor:
        cursor.execute(
            f"""SELECT distinct tpi_erpreferencenumber, tpi_revision, tpi_Objectno, tpi_description,
            tpi_hash FROM masterdata_steam.dbo.tpi where Server='{server} '
            and customertoken = '{customertoken}' and deleted = 0
            """
        )

        return cursor.fetchall()


@logger.catch
def cache_trayids_for_customer(server: str, customertoken) -> list:

    conn = connect.connection("STRDVPDBSQL01")
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            select a.trayname, b.tpi_Objectno FROM masterdata_steam.dbo.traylist a
            left join masterdata_steam.dbo.TPI b on b.tpi_hash = a.tpi_hash
            where b.Server='{server}' and b.Customertoken='{customertoken}'
            """
        )

        return cursor.fetchall()

@logger.catch
def save_tpi_to_database(tpi: dict, sqlserver: str, spmiserver: str, customertoken: str) -> None:

    conn = connect.connection(sqlserver)
    with conn.cursor() as cursor:
        query = f"""
            IF NOT EXISTS (
            SELECT tpi_Objectno from masterdata_steam.dbo.TPI
            where tpi_hash = '{tpi["buildid_hash"]}'
            )
            insert into masterdata_steam.dbo.tpi
            (
            [Server]
            , [Customertoken]
            , tpi_Objectno
            , tpi_internal_number
            , tpi_erpreferencenumber
            , tpi_hash
            , tpi_timestamp
            , tpi_timestamp_acctiveset
            , tpi_description
            , tpi_revision
            , tpi_costcenter
            , tpi_department
            , tpi_package
            , package_storage_time
            , tpi_ste
            , tpi_ste_description
            , processing_priority
            , default_workflowconfiguration
            , precheck
            , deleted
            )
            values
            (
            '{spmiserver}'
            , '{customertoken}'
            , {tpi["objectno"]}
            , '{tpi["internal_build_number"]}'
            , '{tpi["build_id"].replace("'", "''")}'
            , '{tpi["buildid_hash"]}'
            , '{tpi["timestamp"]}'
            , '{tpi["timestamp_active"]}'
            , '{tpi["build_description"].replace("'", "''")}'
            , '{tpi["build_revisionnumber"]}'
            , '{tpi["costcenter"].replace("'", "''")}'
            , '{tpi["department"].replace("'", "''")}'
            , '{tpi["build_package"]}'
            , '{tpi["build_package_storage_time"]}'
            , '{tpi["ste"]}'
            , '{tpi["ste_description"]}'
            , '{tpi["processing_priority"]}'
            , '{tpi["build_workflow"]}'
            , '{tpi["precheck"]}'
            , 0
            );
            """
        cursor.execute(query)


@logger.catch
def mark_tpi_as_deleted(tpi_erpreferencenumber: str, _build_revisionnumber: int, sqlserver: str) -> None:

    conn = connect.connection(sqlserver)
    with conn.cursor() as cursor:
        query = f"""
            update masterdata_steam.dbo.tpi
            set deleted = 1
            where tpi_erpreferencenumber = '{tpi_erpreferencenumber}'
            and tpi_revision = {_build_revisionnumber}
            """
        cursor.execute(query)
        conn.commit()


@logger.catch
def delete_tray_from_database(trayid: str, server: str) -> None:

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        query = f"""
            delete from masterdata_steam.dbo.traylist where trayname = '{trayid}';
            """
        cursor.execute(query)


@logger.catch
def delete_trays_from_database_with_hash(tpihash: str, server: str) -> None:

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        query = f"""
            delete from masterdata_steam.dbo.traylist where tpi_hash = '{tpihash}';
            """
        cursor.execute(query)


@logger.catch
def save_tray_to_database(tpi_hash: str, trayname: str, traystatus: str, defaultstocklocation: str, clipnumber: str, server: str) -> None:

    conn = connect.connection(server)

    with conn.cursor() as cursor:

        query = f"""
            insert into masterdata_steam.dbo.traylist
            (
            tpi_hash
            , trayname
            , traystatus
            , defaultstocklocation
            , clipnumber
            )
            values
            (
            '{tpi_hash}'
            , '{trayname}'
            , '{traystatus}'
            , '{defaultstocklocation}'
            , '{clipnumber}'
            );
            """
        cursor.execute(query)


@logger.catch
def save_tpi_content_to_database(buildid_hash: str, tpi_content: list, server: str) -> None:

    _converted_tpi_content = []
    if tpi_content:
        for i in tpi_content:
            _sort_sql = i["sort"]
            _article_hash = i["article_hash"] if i["ctype"] == "component" else ""
            _sort = i["sort"]
            _alternative = i["alternative"] if i["ctype"] == "component" else ""
            _workplan: bool = False if i["ctype"] == "component" else True
            _workplan_text = i["workplan"] if i["ctype"] == "workplan" else ""
            _quantity = i["standard_quantity"] if i["ctype"] == "component" else 0

            _converted_tpi_content.append(
                (
                    _sort_sql,
                    _sort,
                    _alternative,
                    _article_hash,
                    _workplan,
                    _workplan_text.replace("'", "''"),
                    _quantity,
                )
            )

        conn = connect.connection(server)
        with conn.cursor() as cursor:
            cursor.fast_executemany = True
            query = f"""
                IF NOT EXISTS (
                SELECT tpi_hash from masterdata_steam.dbo.tpi_content
                where tpi_hash = '{buildid_hash}'
                and sort = ?
                )
                insert into masterdata_steam.dbo.tpi_content
                (
                tpi_hash
                , sort
                , alternative
                , article_hash
                , workplan
                , workplan_text
                , quantity
                )
                values
                (
                '{buildid_hash}'
                , ?
                , ?
                , ?
                , ?
                , ?
                , ?
                );
                """
            cursor.executemany(query, _converted_tpi_content)


@logger.catch
def save_packingnote_to_database(article_hash: str, packingnote: str, server: str):

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        query = f"""
        IF NOT EXISTS (
            select * from masterdata_steam.dbo.Component_packingnote
            where article_hash = '{article_hash}' and packingnote = '{packingnote.replace("'", "''")}'
            )
            insert into masterdata_steam.dbo.Component_packingnote
            (
            article_hash
            , packingnote
            )
            values
            (
            '{article_hash}'
            , '{packingnote.replace("'", "''")}'
            );
            """
        cursor.execute(query)


@logger.catch
def get_latest_packing_step(server: str):

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        query = """
        SELECT max(timestamp), traynumber FROM packing where  modifiedat = -1 and traynumber !=''
        group by traynumber;
        """
        cursor.execute(query)
        return cursor.fetchall()


@logger.catch
def save_latest_packing_step(trayid: str, timestamp: str, server: str, spmiserver: str):

    conn = connect.connection(server)
    with conn.cursor() as cursor:
        query = f"""
            IF EXISTS (
            select 1 from masterdata_steam.dbo.latest_packing where traynumber='{trayid}' and server ='{spmiserver}'
            )
            update masterdata_steam.dbo.latest_packing set timestamp='{timestamp}'
            where traynumber='{trayid}' and server ='{spmiserver}'
            else
            insert into masterdata_steam.dbo.latest_packing
            (
            traynumber
            , timestamp
            , server
            )
            values
<<<<<<< Updated upstream
            ('{trayid}', '{timestamp}', '{spmiserver}');
=======
            (
            '{tpi_hash}'
            , '{machineprogramname}'
            , '{machineprogramtype}'
            )
            --end-sql
            """
        cursor.execute(query)


def update_component_metadata(
    article_hash: str,
    server: str,
    language: str = None,
    article_group: str = None,
    measurement_unit: str = None,
    extra_information: str = None
) -> None:

    conn = connect.connection(server)
    with conn.cursor() as cursor:

        update_string = []
        insert_fields = ["ArticleHash"]
        insert_values = [f"'{article_hash}'"]

        if language is not None:
            update_string.append(f"Target.language = '{language}'")
            insert_fields.append("language")
            insert_values.append(f"'{language}'")

        if article_group is not None:
            update_string.append(f"Target.ArticleGroup = '{article_group}'")
            insert_fields.append("ArticleGroup")
            insert_values.append(f"'{article_group}'")

        if measurement_unit is not None:
            update_string.append(f"Target.MeasurementUnit = '{measurement_unit}'")
            insert_fields.append("MeasurementUnit")
            insert_values.append(f"'{measurement_unit}'")

        if extra_information is not None:
            update_string.append(f"Target.ExtraInformation = '{extra_information}'")
            insert_fields.append("ExtraInformation")
            insert_values.append(f"'{extra_information}'")

        # Construct the query
        query = f"""
        --begin-sql
        MERGE INTO masterdata_steam.dbo.ComponentMetaData AS Target
        USING (SELECT '{article_hash}' AS ArticleHash) AS Source
        ON Target.ArticleHash = Source.ArticleHash
        WHEN MATCHED THEN
            UPDATE SET {', '.join(update_string)}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(insert_fields)})
            VALUES ({', '.join(insert_values)});
        --end-sql
>>>>>>> Stashed changes
        """
        cursor.execute(query)
