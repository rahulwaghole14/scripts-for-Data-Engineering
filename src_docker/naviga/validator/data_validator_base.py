"""
For data validation using Pydantic
"""

import json
import re
from datetime import datetime, date
from typing import Optional
from pydantic import BaseModel, Field, validator


# from common.validation.validators import MyBaseModel


class naviga_data(BaseModel):
    Print_Pub_Ind: Optional[str] = Field(alias="Print Pub Ind")
    Advertiser_Id: Optional[int] = Field(alias="Advertiser Id")
    Campaign_ID: Optional[str] = Field(alias="Campaign ID")
    Line_ID: Optional[int] = Field(alias="Line ID")
    Advertiser_Name: Optional[str] = Field(alias="Advertiser Name")
    Month_Actual_Amt: Optional[str] = Field(alias="Month Actual Amt")
    Month_Est_Amt: Optional[str] = Field(alias="Month Est Amt")
    Month_Start_Date: Optional[str] = Field(alias="Month Start Date")
    Month_Unique_ID: Optional[str] = Field(alias="Month Unique ID")
    Campaign_Type: Optional[str] = Field(alias="Campaign Type")
    Campaign_Status_Code: Optional[str] = Field(alias="Campaign Status Code")
    Line_Cancel_Status_ID: Optional[str] = Field(alias="Line Cancel Status ID")
    Currency_Exchange_Rate: Optional[str] = Field(
        alias="Currency Exchange Rate"
    )
    Currency_Code: Optional[str] = Field(alias="Currency Code")
    Agency_Commission_Percent: Optional[str] = Field(
        alias="Agency Commission Percent"
    )
    No_Agy_Comm_Ind: Optional[str] = Field(alias="No Agy Comm Ind")
    Actual_Line_Local_Amount: Optional[str] = Field(
        alias="Actual Line Local Amount"
    )
    Est_Line_Local_Amount: Optional[str] = Field(alias="Est Line Local Amount")
    Gross_Line_Local_Amount: Optional[str] = Field(
        alias="Gross Line Local Amount"
    )
    Net_Line_Local_Amount: Optional[str] = Field(alias="Net Line Local Amount")
    Gross_Line_Foreign_Amount: Optional[str] = Field(
        alias="Gross Line Foreign Amount"
    )
    Net_Line_Foreign_Amount: Optional[str] = Field(
        alias="Net Line Foreign Amount"
    )
    Current_Rep_ID: Optional[str] = Field(alias="Current Rep ID")
    Current_Rep_Pct: Optional[str] = Field(alias="Current Rep Pct")
    Current_Rep_Name: Optional[str] = Field(alias="Current Rep Name")
    Net_Rep_Amount: Optional[str] = Field(alias="Net Rep Amount")
    Product_ID: Optional[str] = Field(alias="Product ID")
    Product_Name: Optional[str] = Field(alias="Product Name")
    Primary_Group_ID: Optional[str] = Field(alias="Primary Group ID")
    Date_Entered: Optional[str] = Field(alias="Date Entered")
    Client_Type_ID: Optional[str] = Field(alias="Client Type ID")
    Ad_Type_ID: Optional[str] = Field(alias="Ad Type ID")
    Issue_Date: Optional[str] = Field(alias="Issue Date")
    Product_Grouping: Optional[str] = Field(alias="Product Grouping")
    Primary_Rep_Group_ID: Optional[str] = Field(alias="Primary Rep Group ID")
    Primary_Rep_Group: Optional[str] = Field(alias="Primary Rep Group")
    Agency_ID: Optional[str] = Field(alias="Agency ID")
    Agency_Name: Optional[str] = Field(alias="Agency Name")
    Brand_PIB_Code: Optional[str] = Field(alias="Brand PIB Code")
    AD_Internet_Campaigns_Brand_Id: Optional[str] = Field(
        alias="AD Internet Campaigns Brand Id"
    )
    PIB_Category_Desc: Optional[str] = Field(alias="PIB Category Desc")
    GL_Type_ID: Optional[str] = Field(alias="GL Type ID")
    GL_Types_Description: Optional[str] = Field(alias="GL Types Description")
    Size_Desc: Optional[str] = Field(alias="Size Desc")
    Advertiser_Legacy: Optional[str] = Field(alias="Advertiser Legacy")
    Agency_Legacy: Optional[str] = Field(alias="Agency Legacy")
    AD_Internet_Sections_Section_Description: Optional[str] = Field(
        alias="AD Internet Sections Section Description"
    )
    Orig_Rep_Report_Ids: Optional[str] = Field(alias="Orig Rep Report Ids")
    Curr_Rep_Report_Ids: Optional[str] = Field(alias="Curr Rep Report Ids")
    Orig_Rep_ID: Optional[str] = Field(alias="Orig Rep ID")
    Timestamp: Optional[str] = Field(alias="Timestamp")
    Est_Qty: Optional[str] = Field(alias="Est Qty")
    Month_Actual_Imps: Optional[str] = Field(alias="Month Actual Imps")

    @validator("*", pre=True)
    def convert_strings(cls, v):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return str(v)
        return v


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


# class DateTimeEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, datetime):
#             return obj.date()  # Ensure it's returned as a `date` object
#         elif isinstance(obj, date):
#             return obj  # Keep it as a `date` object
#         return super().default(obj)


class naviga_data_targets(BaseModel):
    Rep_ID: Optional[int] = Field(alias="Rep ID")
    Print_or_Digital: Optional[str] = Field(alias="Print Or Digital")
    AD_Sales_Reps_Salesrep_Name: Optional[str] = Field(
        alias="AD Salesreps Salesrep Name"
    )
    Month_Year: Optional[date] = Field(
        alias="Month Year"
    )  # This will be a datetime object
    Budget: Optional[int] = Field(alias="Budget")

    @validator("Month_Year", pre=True)
    def convert_month_year_to_datetime(cls, v):
        if isinstance(v, str):
            # Clean up the input string to remove extra spaces
            cleaned_v = v.replace(" ", "")
            cleaned_v = "1/" + cleaned_v
            cleaned_v = cleaned_v.replace("-", "/")
            components = cleaned_v.split("/")
            cleaned_v = "/".join(components[::-1])

            # Convert the cleaned string to a datetime object with default day '01'
            return datetime.strptime(cleaned_v, "%Y/%m/%d")
        return v

    @validator("Budget", pre=True)
    def parse_budget(cls, v):
        if isinstance(v, str):
            # Remove all non-digit characters and convert to integer
            v = re.sub(r"[^\d]", "", v)
            if v:
                return int(v)
        return v

    @validator("*", pre=True)
    def convert_strings(cls, v):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return str(v)
        return v
