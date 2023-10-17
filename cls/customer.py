from dataclasses import dataclass

from loguru import logger


@dataclass
@logger.catch
class Customer:
    def __init__(self, name: str, email: str, customer_token: str):
        self.name = name
        self.email = email
        self.customer_token = customer_token

    def __repr__(self):
        return f"{self.name}, {self.email}, {self.customer_token}"

    def __str__(self):
        return f"{self.name}, {self.email}, {self.customer_token}"
