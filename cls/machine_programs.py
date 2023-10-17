from dataclasses import dataclass

from loguru import logger


@logger.catch
@dataclass
class Machine_programs:
    def __init__(
            self,
            Machine_program_description: str,
            machine_program_type: str
    ):
        self.Machine_program_description = Machine_program_description
        self.machine_program_type = machine_program_type

    def __str__(self):
        return f"{self.Machine_program_description} {self.machine_program_type}"

    def __repr__(self):
        return f"{self.Machine_program_description} {self.machine_program_type}"
