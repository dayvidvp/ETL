from dataclasses import dataclass

from loguru import logger


@logger.catch
@dataclass
class Bom_workplans:
    def __init__(
        self,
        sort: int,
        workplan: str,
    ):
        self.sort = sort
        self.workplan = workplan

    def __repr__(self):
        return f"{self.sort},{self.workplan}"

    def __str__(self):
        return f"{self.sort},{self.workplan}"
