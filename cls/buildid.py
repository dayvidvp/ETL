from dataclasses import dataclass

from loguru import logger


@logger.catch
@dataclass
class Buildid:
    """[dataclass to get the build information]"""

    def __init__(
        self,
        build_objectno: int,
        build_id: str,
        build_description: str,
        build_revisionnumber: int,
        timestamp_active: str,
        build_timestamp: str,
        build_workflow: str,
        department: str,
        costcenter: str,
        internal_buildid_number: int,
        build_precheck: str,
        build_ste: str,
        build_ste_description: str,
        build_package: str,
        package_storage_time: int,
        processing_priority: str,
    ):
        self.build_objectno = build_objectno
        self.build_id = build_id
        self.build_description = build_description
        self.build_revisionnumber = build_revisionnumber
        self.timestamp_active = timestamp_active
        self.build_timestamp = build_timestamp
        self.build_workflow = build_workflow
        self.department = department
        self.costcenter = costcenter
        self.internal_buildid_number = internal_buildid_number
        self.build_precheck = build_precheck
        self.build_ste = build_ste
        self.build_ste_description = build_ste_description
        self.build_package = build_package
        self.package_storage_time = package_storage_time
        self.processing_priority = processing_priority

    def __repr__(self):
        return (
            f"{self.build_objectno}, {self.build_id}, {self.build_description},"
            f"{self.build_revisionnumber},{self.timestamp_active},"
            f"{self.build_timestamp}, {self.build_workflow}, {self.department}, {self.costcenter}"
            f"{self.internal_buildid_number},  {self.build_precheck}, {self.build_ste}, {self.build_ste_description},"
            f"{self.build_package}, {self.package_storage_time}, {self.processing_priority}"
        )

    def __str__(self):

        return (
            f"{self.build_objectno}, {self.build_id}, {self.build_description},"
            f"{self.build_revisionnumber},{self.timestamp_active},"
            f"{self.build_timestamp}, {self.build_workflow}, {self.department}, {self.costcenter}"
            f"{self.internal_buildid_number},  {self.build_precheck}, {self.build_ste}, {self.build_ste_description},"
            f"{self.build_package}, {self.package_storage_time}, {self.processing_priority}"
        )
