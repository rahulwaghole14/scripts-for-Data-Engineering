from pydantic import BaseModel,validator
from typing import Optional


class SheetDataRow(BaseModel):
    # Sales_REP_Field: Optional[str]
    # Agent: Optional[str]
    Agent: Optional[str] = None
    Listing_Provider: Optional[str] = None
    Live_Date: Optional[str] = None
    Office: Optional[str] = None
    Address: Optional[str] = None
    Geo_Location_Region: Optional[str] = None
    SALES_REGION: Optional[str] = None
    #ID: Optional[int] = None
    Price: Optional[str] = None
    #Weeks: Optional[int] = None
    # SC_: Optional[str]
    # SC_mob: Optional[str]
    # Checked_By: Optional[str]
    Offered_Applied: Optional[str] = None
    Notes: Optional[str] = None

    class Config:
        extra = "allow"

    @validator('*', pre=True, always=True)  # Applies to all fields
    def empty_str_to_none(cls, v):
        if isinstance(v, str) and v.strip() == '':
            return None  # Convert empty string to None
        return v
    