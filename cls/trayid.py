from dataclasses import dataclass

from loguru import logger


@logger.catch
@dataclass
class Trayid:
    def __init__(
        self,
        trayid: str,
        trayidstatus: str,
        trayiddfaultstocklocation: str,
        traydidclipnumber: str,
    ):
        self.trayid = trayid
        self.trayidstatus = trayidstatus
        self.trayiddfaultstocklocation = trayiddfaultstocklocation
        self.traydidclipnumber = traydidclipnumber

    def __repr__(self):
        return f"{self.trayid}, {self.trayidstatus}, {self.trayiddfaultstocklocation}, {self.traydidclipnumber}"

    def __str__(self):
        return f"{self.trayid}, {self.trayidstatus}, {self.trayiddfaultstocklocation}, {self.traydidclipnumber}"
