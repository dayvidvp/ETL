from dataclasses import dataclass

from loguru import logger


@logger.catch
@dataclass
class Component_Code:
    def __init__(self, udi_code, modified_on, modified_date, modified_by):
        self.udi_code = udi_code
        self.modified_on = modified_on
        self.modified_date = modified_date
        self.modified_by = modified_by

    def __str__(self):
        return (
            f"{self.udi_code},"
            f"{self.modified_on},"
            f"{self.modified_date},"
            f"{self.modified_by}"
        )

    def __repr__(self):
        return (
            f"{self.udi_code},"
            f"{self.modified_on},"
            f"{self.modified_date},"
            f"{self.modified_by}"
        )
