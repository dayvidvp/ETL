from dataclasses import dataclass

from loguru import logger


@logger.catch
@dataclass
class Bom_Content:
    def __init__(
            self,
            componentname: str,
            componentidentifier: str,
            positiontype: str,
            positionid: int,
            manufacturer_token: str,
            manufacturer_description: str,
            annumber: str,
            standard_quantity: int,
            component_erpreferencenumber: str,
            packingnote: str,
            position_comment: str,
            sort: int,
            internal_article_number: int,
            internal_build_number: int,
            warningtext: str,
            item_objectno: int,
            is_alternative: bool,
            workplan: str,
            ctype: str
    ):

        self.componentname = componentname
        self.componentidentifier = componentidentifier
        self.positiontype = positiontype
        self.positionid = positionid
        self.manufacturer_token = manufacturer_token
        self.manufacturer_description = manufacturer_description
        self.annumber = annumber
        self.standard_quantity = standard_quantity
        self.component_erpreferencenumber = component_erpreferencenumber
        self.packingnote = packingnote
        self.position_comment = position_comment
        self.sort = sort
        self.internal_article_number = internal_article_number
        self.internal_build_number = internal_build_number
        self.warningtext = warningtext
        self.item_objectno = item_objectno
        self.is_alternative = is_alternative
        self.workplan = workplan
        self.ctype = ctype


def __repr__(self):
    return (
        f"{self.componentname},"
        f"{self.componentidentifier},"
        f"{self.positiontype},"
        f"{self.positionid},"
        f"{self.manufacturer_token},"
        f"{self.manufacturer_description},"
        f"{self.annumber},"
        f"{self.standard_quantity},"
        f"{self.component_erpreferencenumber},"
        f"{self.packingnote},"
        f"{self.position_comment},"
        f"{self.sort},"
        f"{self.internal_article_number},"
        f"{self.internal_build_number},"
        f"{self.warningtext},"
        f"{self.item_objectno},"
        f"{self.is_alternative}"
    )


def __str__(self):
    return (
        f"{self.componentname},"
        f"{self.componentidentifier},"
        f"{self.positiontype},"
        f"{self.positionid},"
        f"{self.manufacturer_token},"
        f"{self.manufacturer_description},"
        f"{self.annumber},"
        f"{self.standard_quantity},"
        f"{self.component_erpreferencenumber},"
        f"{self.packingnote},"
        f"{self.position_comment},"
        f"{self.sort},"
        f"{self.internal_article_number},"
        f"{self.internal_build_number},"
        f"{self.warningtext},"
        f"{self.item_objectno},"
        f"{self.is_alternative}"
        f"{self.workplan},"
        f"{self.ctype}"
    )
