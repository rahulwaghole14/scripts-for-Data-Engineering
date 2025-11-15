"""
validators for webflow form data
"""
# pylint: disable=line-too-long

from datetime import datetime
from pydantic import ConfigDict
from common.validation.validators import MyBaseModel


class FormSubmission(MyBaseModel):
    """FormSubmission"""

    model_config = ConfigDict(extra="forbid")

    id: str
    displayName: str
    siteId: str
    formId: str
    formResponse: dict
    dateSubmitted: datetime
