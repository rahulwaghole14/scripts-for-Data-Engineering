"""
this module contains the validation schema for the user profile data
"""

from datetime import datetime, date
from typing import Optional, List
from pydantic import Field, validator
from common.validation.validators import MyBaseModel


class UserProfileMeta(MyBaseModel):
    """user profile meta data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    resourceType: Optional[str] = None
    created: Optional[datetime] = None
    lastModified: Optional[datetime] = None


class UserProfileName(MyBaseModel):
    """user profile name data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    familyName: Optional[str] = None
    givenName: Optional[str] = None


class UserProfileEmail(MyBaseModel):
    """user profile email data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    value: Optional[str] = None
    type: Optional[str] = None
    primary: Optional[bool] = None


class UserProfileAddress(MyBaseModel):
    """user profile address data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    country: Optional[str] = None
    postalCode: Optional[str] = None
    primary: Optional[bool] = None
    street_address: Optional[str] = Field(None, alias="streetAddress")


class UserProfilePhoneNumber(MyBaseModel):
    """user profile phone number data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    value: Optional[str] = None
    type: Optional[str] = None
    primary: Optional[bool] = None


class UserNewsletterSubscriptions(MyBaseModel):
    """user newsletter subscriptions data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    name: Optional[str] = None
    description: Optional[str] = None
    communicationName: Optional[str] = None
    communicationReference: Optional[str] = None


class UserNewsletterSubscription(MyBaseModel):
    """user newsletter subscription data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    acmNewsId: Optional[str] = None
    acmNewsInternalName: Optional[str] = None
    duration: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    communicationReference: Optional[str] = None


class UserCustomExtension(MyBaseModel):
    """user profile custom extension data"""

    class Config:  # pylint: disable=too-few-public-methods
        """config"""

        extra = "forbid"

    consentReference: Optional[bool] = None
    dateOfBirth: Optional[date] = None
    yearOfBirth: Optional[date] = None
    emailVerified: Optional[bool] = None
    mobileNumberVerified: Optional[bool] = None
    emailVerifiedDate: Optional[datetime] = None
    gender: Optional[str] = None
    newsletterSubscription: Optional[List[UserNewsletterSubscription]] = None
    subscriberId: Optional[int] = None
    mobileVerifiedDate: Optional[datetime] = None
    newsletterSubscriptions: Optional[List[UserNewsletterSubscriptions]] = None
    publication: Optional[str] = None
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
    user_custom_extension: Optional[UserCustomExtension] = Field(
        None, alias="urn:ietf:params:scim:schemas:extension:custom:2.0:User"
    )
    marketing_id: Optional[str] = None
    marketing_id_email: Optional[str] = None
    record_loaded_dts: Optional[datetime] = None
    timezone: Optional[str] = None
    ppid: Optional[str] = None
