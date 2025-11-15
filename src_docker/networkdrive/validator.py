"""
models
"""
# pylint: disable=E0213

from datetime import date, datetime
from typing import Optional
from pydantic import validator
from common.validation.validators import MyBaseModel


class MastheadArticlesByDay(MyBaseModel):
    """
    Masthead Articles By Day
    """

    itemId: Optional[int] = None
    value: Optional[str] = None
    Page_Views: Optional[float] = None
    Logged_In_Page_Views: Optional[float] = None
    Masthead: Optional[str] = None
    DataStartDate: Optional[date] = None
    DataEndDate: Optional[date] = None
    DataRefresh: Optional[datetime] = None

    @validator("itemId", pre=True, always=True)
    def parse_item_id(cls, v):
        """parse item id"""
        try:
            return int(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("value", pre=True, always=True)
    def parse_value(cls, v):
        """parse value"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Page_Views", pre=True, always=True)
    def parse_page_views(cls, v):
        """parse page views"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Logged_In_Page_Views", pre=True, always=True)
    def parse_logged_in_page_views(cls, v):
        """parse logged in page views"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Masthead", pre=True, always=True)
    def parse_masthead(cls, v):
        """parse masthead"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("DataStartDate", pre=True, always=True)
    def parse_data_start_date(cls, v):
        """parse data start date"""
        try:

            return date.fromisoformat(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("DataEndDate", pre=True, always=True)
    def parse_data_end_date(cls, v):
        """parse data end date"""
        try:
            return date.fromisoformat(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("DataRefresh", pre=True, always=True)
    def parse_data_refresh(cls, v):
        """parse data refresh"""
        try:
            return datetime.fromisoformat(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate


class MastheadLoggedInUsers(MyBaseModel):
    """Masthead Logged In Users"""

    itemId: Optional[int] = None
    value: Optional[str] = None
    Page_Views: Optional[float] = None
    Unique_Visitors: Optional[float] = None
    Page_Views___Articles: Optional[float] = None
    Masthead: Optional[str] = None
    DataStartDate: Optional[date] = None
    DataEndDate: Optional[date] = None
    DataRefresh: Optional[datetime] = None

    @validator("itemId", pre=True, always=True)
    def parse_item_id(cls, v):
        """parse item id"""
        try:
            return int(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("value", pre=True, always=True)
    def parse_value(cls, v):
        """parse value"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Page_Views", pre=True, always=True)
    def parse_page_views(cls, v):
        """parse page views"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Unique_Visitors", pre=True, always=True)
    def parse_unique_visitors(cls, v):
        """parse unique visitors"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Page_Views___Articles", pre=True, always=True)
    def parse_page_views_articles(cls, v):
        """parse page views articles"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Masthead", pre=True, always=True)
    def parse_masthead(cls, v):
        """parse masthead"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("DataStartDate", pre=True, always=True)
    def parse_data_start_date(cls, v):
        """parse data start date"""
        try:
            return date.fromisoformat(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("DataEndDate", pre=True, always=True)
    def parse_data_end_date(cls, v):
        """parse data end date"""
        try:
            return date.fromisoformat(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("DataRefresh", pre=True, always=True)
    def parse_data_refresh(cls, v):
        """parse data refresh"""
        try:
            return datetime.fromisoformat(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate


class MastheadDaily(MyBaseModel):
    """Masthead Articles By Day"""

    Mastheads_Page_Instance_ID: Optional[str] = None
    Page_Views: Optional[int] = None
    Unique_Visitors: Optional[int] = None
    Date: Optional[date] = None

    @validator("Mastheads_Page_Instance_ID", pre=True, always=True)
    def parse_mastheads_page_instance_id(cls, v):
        """parse mastheads page instance id"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Page_Views", pre=True, always=True)
    def parse_page_views(cls, v):
        """parse page views"""
        try:
            return int(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Unique_Visitors", pre=True, always=True)
    def parse_unique_visitors(cls, v):
        """parse unique visitors"""
        try:
            return int(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Date", pre=True, always=True)
    def parse_date(cls, v):
        """parse date"""
        if v is None:
            return None
        try:
            # Assuming the date format in your data is day/month/year
            parsed_date = datetime.strptime(v, "%d/%m/%Y").date()
            # If you need the date object itself, just return parsed_date
            return (
                parsed_date.isoformat()
            )  # Returns the date in ISO format (YYYY-MM-DD)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate


class Targetstableforpbi(MyBaseModel):
    """Targetstableforpbi"""

    Origin: Optional[str] = None
    Current_Rep_Name: Optional[str] = None
    Current_Rep_ID: Optional[int] = None
    Sales_Group: Optional[str] = None
    Primary_Rep_Group: Optional[str] = None
    IdOriginYearMonthKEY: Optional[str] = None
    Revenue: Optional[float] = None
    Year: Optional[float] = None
    Month: Optional[str] = None
    Date: Optional[date] = None
    Target: Optional[float] = None

    @validator("Origin", pre=True, always=True)
    def parse_origin(cls, v):
        """parse origin"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Current_Rep_Name", pre=True, always=True)
    def parse_current_rep_name(cls, v):
        """parse current rep name"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Current_Rep_ID", pre=True, always=True)
    def parse_current_rep_id(cls, v):
        """parse current rep id"""
        try:
            return int(v)
        except (ValueError, TypeError):
            return None

    @validator("Sales_Group", pre=True, always=True)
    def parse_sales_group(cls, v):
        """parse sales group"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Primary_Rep_Group", pre=True, always=True)
    def parse_primary_rep_group(cls, v):
        """parse primary rep group"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("IdOriginYearMonthKEY", pre=True, always=True)
    def parse_id_origin_year_month_key(cls, v):
        """parse id origin year month key"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Revenue", pre=True, always=True)
    def parse_revenue(cls, v):
        """parse revenue"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Year", pre=True, always=True)
    def parse_year(cls, v):
        """parse year"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Month", pre=True, always=True)
    def parse_month(cls, v):
        """parse month"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Date", pre=True, always=True)
    def parse_date(cls, v):
        """parse date"""
        try:
            return date.fromisoformat(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Target", pre=True, always=True)
    def parse_target(cls, v):
        """parse target"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate


class Navigatableforpbi(MyBaseModel):
    """Navigatableforpbi"""

    Origin: Optional[str] = None
    Primary_Rep_Group: Optional[str] = None
    Current_Rep_Name: Optional[str] = None
    Platform: Optional[str] = None
    Product_Type: Optional[str] = None
    Date: Optional[date] = None
    Paid_Advertising: Optional[str] = None
    SourceOrGL: Optional[str] = None
    Campaign_ID: Optional[str] = None
    Line_ID: Optional[str] = None
    Primary_Group_ID: Optional[str] = None
    Product_ID: Optional[str] = None
    Product_Name: Optional[str] = None
    Client_Type_ID: Optional[str] = None
    Advertiser_ID: Optional[str] = None
    Advertiser_Name: Optional[str] = None
    Month_Unique_ID: Optional[str] = None
    Campaign_Type: Optional[str] = None
    Campaign_Status_Code: Optional[str] = None
    Date_Entered: Optional[date] = None
    Line_Cancel_Status_ID: Optional[str] = None
    Currency_Exchange_Rate: Optional[float] = None
    Currency_Code: Optional[str] = None
    Agency_Commission_Percent: Optional[float] = None
    No_Agy_Comm_Ind: Optional[str] = None
    Actual_Line_Local_Amount: Optional[float] = None
    Est_Line_Local_Amount: Optional[float] = None
    Gross_Line_Local_Amount: Optional[float] = None
    Revenue: Optional[float] = None
    Gross_Line_Foreign_Amount: Optional[float] = None
    Net_Line_Foreign_Amount: Optional[float] = None
    Current_Rep_ID: Optional[int] = None
    Current_Rep_Pct: Optional[float] = None
    Net_Rep_Amount: Optional[float] = None
    Month_Actual_Amt: Optional[float] = None
    Month_Est_Amt: Optional[float] = None
    Ad_Type_ID: Optional[str] = None
    Product_Grouping: Optional[str] = None
    Primary_Rep_Group_ID: Optional[str] = None
    Agency_ID: Optional[str] = None
    Agency_Name: Optional[str] = None
    AD_Internet_Campaigns_Brand_Id: Optional[str] = None
    Brand_PIB_Code: Optional[str] = None
    PIB_Category_Desc: Optional[str] = None
    Size_Desc: Optional[str] = None
    GL_Type_ID: Optional[str] = None
    GL_Types_Description: Optional[str] = None
    FileUpdateDateTime: Optional[datetime] = None
    Advertiser_Legacy: Optional[str] = None
    Agency_Legacy: Optional[float] = None
    Section: Optional[str] = None
    PublicationType: Optional[str] = None
    Source: Optional[str] = None
    Sales_Group: Optional[str] = None
    Year: Optional[float] = None
    Month: Optional[str] = None
    IdOriginYearMonthKEY: Optional[str] = None

    @validator("FileUpdateDateTime", pre=True, always=True)
    def parse_file_update_date_time(cls, v):
        """parse file update date time"""
        try:
            return datetime.fromisoformat(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Primary_Group_ID", pre=True, always=True)
    def parse_primary_group_id(cls, v):
        """parse primary group id"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Product_ID", pre=True, always=True)
    def parse_product_id(cls, v):
        """parse product id"""
        try:
            return str(v)
        except (ValueError, TypeError):
            return None  # or return a default value if appropriate

    @validator("Net_Rep_Amount", pre=True, always=True)
    def parse_net_rep_amount(cls, v):
        """parse net rep amount"""
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    @validator("Current_Rep_ID", pre=True, always=True)
    def parse_current_rep_id(cls, v):
        """parse current rep id"""
        try:
            return int(v)
        except (ValueError, TypeError):
            return None


class Datetable(MyBaseModel):
    """Datetable"""

    DimDateKey: Optional[int] = None
    DateJln: Optional[int] = None
    CalYearKey: Optional[int] = None
    CalYearName: Optional[str] = None
    CalYearQuarterKey: Optional[int] = None
    CalQuarterKey: Optional[int] = None
    CalQuarterName: Optional[str] = None
    CalYearMonthKey: Optional[int] = None
    CalMonthKey: Optional[int] = None
    CalYearMonthName: Optional[str] = None
    CalWeekKey: Optional[int] = None
    CalWeekName: Optional[str] = None
    CalYearWeekKey: Optional[int] = None
    CalYrWeekName: Optional[str] = None
    CalDayOfMonth: Optional[int] = None
    CalDayOfWeekKey: Optional[int] = None
    FinYearKey: Optional[int] = None
    FinYearName: Optional[str] = None
    FinYearAltName: Optional[str] = None
    FinYearQuarterKey: Optional[int] = None
    FinQuarterKey: Optional[int] = None
    FinQuarterName: Optional[str] = None
    FinYearMonthKey: Optional[int] = None
    FinMonthKey: Optional[int] = None
    FinMonthName: Optional[str] = None
    FinYearWeekKey: Optional[int] = None
    FinWeekKey: Optional[int] = None
    FinWeekName: Optional[str] = None
    FinYearWeekName: Optional[str] = None
    FinDayOfWeekKey: Optional[int] = None
    GenDate: Optional[date] = None
