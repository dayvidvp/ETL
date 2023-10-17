from dataclasses import dataclass

from loguru import logger


@logger.catch
@dataclass
class Process_Instructions:
    def __init__(
        self,
        code: str,
        instruction_language: str,
        instruction_description: str
    ):
        self.code = code
        self.instruction_language = instruction_language
        self.instruction_description = instruction_description

    def __repr__(self):
        return f"{self.code}, {self.instruction_language}, {self.instruction_description}"

    def __str__(self):
        return f"{self.code}, {self.instruction_language}, {self.instruction_description}"

