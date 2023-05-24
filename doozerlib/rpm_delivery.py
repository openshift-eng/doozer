from typing import List, Optional

from pydantic import BaseModel, Field


class RPMDelivery(BaseModel):
    """ An RPMDelivery config
    """
    packages: List[str] = Field(min_items=1)
    integration_tag: str = Field(min_length=1)
    ship_ok_tag: str = Field(min_length=1)
    stop_ship_tag: str = Field(min_length=1)
    target_tag: Optional[str] = Field(min_length=1)


class RPMDeliveries(BaseModel):
    """ Represents rpm_deliveries field in group config
    """
    __root__: List[RPMDelivery]

    def __bool__(self):
        return bool(self.__root__)

    def __iter__(self):
        return iter(self.__root__)

    def __getitem__(self, item):
        return self.__root__[item]
