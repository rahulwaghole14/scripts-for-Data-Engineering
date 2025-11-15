"""
this module contains the validation schema for the user profile data
"""

from datetime import datetime, date
from typing import Optional, List
from pydantic import Field, validator
from a_common.validation.validators import MyBaseModel


class UserProfileMeta(MyBaseModel):
    """user profile meta data"""

    resourceType: Optional[str] = None
    created: Optional[datetime] = None
    lastModified: Optional[datetime] = None


class UserProfileName(MyBaseModel):
    """user profile name data"""

    familyName: Optional[str] = None
    givenName: Optional[str] = None


class UserProfileEmail(MyBaseModel):
    """user profile email data"""

    value: Optional[str] = None
    type: Optional[str] = None
    primary: Optional[bool] = None


class UserProfileAddress(MyBaseModel):
    """user profile address data"""

    country: Optional[str] = None
    postalCode: Optional[str] = None
    primary: Optional[bool] = None
    street_address: Optional[str] = Field(None, alias="streetAddress")


class UserProfilePhoneNumber(MyBaseModel):
    """user profile phone number data"""

    value: Optional[str] = None
    type: Optional[str] = None
    primary: Optional[bool] = None


class UserNewsletterSubscription(MyBaseModel):
    """user newsletter subscription data"""

    acmNewsId: Optional[str] = None
    acmNewsInternalName: Optional[str] = None
    duration: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    communicationReference: Optional[str] = None


class UserCustomExtension(MyBaseModel):
    """user profile custom extension data"""

    consentReference: Optional[bool] = None
    dateOfBirth: Optional[date] = None
    yearOfBirth: Optional[date] = None
    emailVerified: Optional[bool] = None
    mobileNumberVerified: Optional[bool] = None
    emailVerifiedDate: Optional[datetime] = None
    gender: Optional[str] = None
    newsletterSubscription: Optional[List[UserNewsletterSubscription]] = None
    subscriberId: Optional[int] = None
    adobeId: Optional[str] = None

    @validator("yearOfBirth", pre=True)
    def parse_year_of_birth(
        cls, value
    ):  # pylint: disable=no-self-argument, invalid-name
        """parse yearOfBirth"""
        if isinstance(value, int):
            # Assuming the input is an integer year,
            # transform it to a date object with default month and day
            return date(value, 1, 1)
        if isinstance(value, str) and value.isdigit():
            # If the input is a string that represents a year, convert accordingly
            return date(int(value), 1, 1)
        if value is None:
            return value
        raise ValueError(
            "yearOfBirth must be a valid year as integer or string"
        )

    @validator("emailVerifiedDate", pre=True, always=True)
    def parse_email_verified_date(
        cls, value
    ):  # pylint: disable=no-self-argument, invalid-name
        """parse emailVerifiedDate"""
        if value is None:
            return None
        return value


class UserProfile(MyBaseModel):
    """am profile data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    schemas: List[str]
    id: int
    externalId: Optional[str] = None
    userName: Optional[str] = None
    displayName: Optional[str] = None
    meta: Optional[UserProfileMeta] = None
    name: Optional[UserProfileName] = None
    active: Optional[bool] = None
    emails: Optional[List[UserProfileEmail]] = None
    addresses: Optional[List[UserProfileAddress]] = None
    phoneNumbers: Optional[List[UserProfilePhoneNumber]] = None
    roles: List[str]
    timezone: Optional[str] = None
    user_custom_extension: Optional[UserCustomExtension] = Field(
        None, alias="urn:ietf:params:scim:schemas:extension:custom:2.0:User"
    )
    marketing_id: Optional[str] = None
    marketing_id_email: Optional[str] = None
    ppid: Optional[str] = None
    record_loaded_dts: Optional[datetime] = None
