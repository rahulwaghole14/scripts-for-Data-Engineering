""" define data model classes for pydantic """
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel
from google.cloud.bigquery import SchemaField


class MainPublicationChannel(BaseModel):
    """schema of MainPublicationChannel in ContentItemMeta"""

    name: Optional[str] = None
    key: Optional[str] = None


class ContentItem(BaseModel):
    """content item returned by api path api/content"""

    id: str
    published: bool = None
    createdDate: datetime = None
    updatedDate: datetime = None
    publishedDate: datetime = None
    contentType: str = None
    mainPublicationChannel: Optional[MainPublicationChannel] = None


class Byline(BaseModel):
    """schema of Byline in ContentItemMeta"""

    name: Optional[str] = None


class Teaser(BaseModel):
    """schema of Teaser in ContentItemMeta"""

    shortHeadline: Optional[str] = None
    intro: Optional[str] = None


class Body(BaseModel):
    """schema of Body in ContentItemMeta"""

    id: Optional[str] = None
    drupalId: Optional[str] = None
    contentBodyType: Optional[str] = None
    text: Optional[str] = None
    name: Optional[str] = None
    url: Optional[str] = None
    caption: Optional[str] = None
    credit: Optional[str] = None
    source: Optional[str] = None
    publishedDate: Optional[datetime] = None
    updatedDate: Optional[datetime] = None
    published: Optional[bool] = None


class SocialMediaHandle(BaseModel):
    """schema of SocialMediaHandle in ContentItemMeta"""

    type: Optional[str] = None
    value: Optional[str] = None


class ProfilePicture(BaseModel):
    """schema of ProfilePicture in ContentItemMeta"""

    id: str
    drupalId: Optional[str] = None
    url: Optional[str] = None
    caption: Optional[str] = None
    alt: Optional[str] = None
    source: Optional[str] = None
    publishedDate: Optional[datetime] = None
    updatedDate: Optional[datetime] = None
    contentType: Optional[str] = None
    name: Optional[str] = None
    author: Optional[str] = None


class Author(BaseModel):
    """schema of Author in ContentItemMeta"""

    id: Optional[str] = None
    contentType: Optional[str] = None
    name: Optional[str] = None
    givenName: Optional[str] = None
    familyName: Optional[str] = None
    email: Optional[str] = None
    url: Optional[str] = None
    socialMediaHandles: Optional[List[SocialMediaHandle]] = None
    jobTitle: Optional[str] = None
    biography: Optional[str] = None
    location: Optional[str] = None
    searchIndexed: Optional[bool] = None
    profileVisibility: Optional[bool] = None
    published: Optional[bool] = None
    profilePicture: Optional[ProfilePicture] = None
    phoneNumber: Optional[str] = None


class ContentItemMeta(BaseModel):
    """content item returned by api path api/display/content/{id}"""

    id: str
    drupalId: str = None
    contentType: str
    url: str
    headline: str = None
    headlinePrimary: bool = None
    slug: str = None
    byline: Optional[Byline] = None
    status: str = None
    createdDate: datetime
    publishedDate: datetime
    updatedDate: datetime
    teaser: Optional[Teaser] = None
    body: Optional[List[Body]] = None
    source: str = None
    section: str = None
    teams: list = None
    typeOfWorkLabel: list = None
    liveBlog: bool = None
    searchIndexed: bool = None
    topics: list = None
    entities: list = None
    sensitivity: str = None
    sentiment: bool = None
    newsValue: int = None
    lifetime: str = None
    comments: bool = None
    mainPublicationChannel: Optional[MainPublicationChannel] = None
    author: Optional[List[Author]] = None


def parse_content(response_json: List[dict]) -> List[ContentItem]:
    """Parse the list of content items using the Pydantic model"""
    all_items = [ContentItem(**item) for item in response_json]
    return list(all_items)


def parse_content_meta(response_json: dict) -> ContentItemMeta:
    """Parse the content item using the Pydantic model"""
    try:
        return ContentItemMeta(**response_json)
    except ValueError as e:
        # Handle or log the error as per your requirement
        print(f"Error in parsing content metadata: {e}")
        raise


byline_schema = [SchemaField("name", "STRING", mode="NULLABLE")]

teaser_schema = [
    SchemaField("shortHeadline", "STRING", mode="NULLABLE"),
    SchemaField("intro", "STRING", mode="NULLABLE"),
]

mainpublicationchannel_schema = [
    SchemaField("name", "STRING", mode="NULLABLE"),
    SchemaField("key", "STRING", mode="NULLABLE"),
]

# publicationchannel_schema = [
#     SchemaField("name", "STRING", mode="NULLABLE"),
#     SchemaField("key", "STRING", mode="NULLABLE")
# ]

body_schema = [
    SchemaField("id", "STRING", mode="NULLABLE"),
    SchemaField("drupalId", "STRING", mode="NULLABLE"),
    SchemaField("contentBodyType", "STRING", mode="NULLABLE"),
    SchemaField("text", "STRING", mode="NULLABLE"),
    SchemaField("name", "STRING", mode="NULLABLE"),
    SchemaField("url", "STRING", mode="NULLABLE"),
    SchemaField("caption", "STRING", mode="NULLABLE"),
    SchemaField("credit", "STRING", mode="NULLABLE"),
    SchemaField("source", "STRING", mode="NULLABLE"),
    SchemaField("publishedDate", "DATETIME", mode="NULLABLE"),
    SchemaField("updatedDate", "DATETIME", mode="NULLABLE"),
    SchemaField("published", "BOOL", mode="NULLABLE"),
]

social_media_handle_schema = [
    SchemaField("type", "STRING", mode="NULLABLE"),
    SchemaField("value", "STRING", mode="NULLABLE"),
]

profile_picture_schema = [
    SchemaField("id", "STRING", mode="NULLABLE"),
    SchemaField("drupalId", "STRING", mode="NULLABLE"),
    SchemaField("url", "STRING", mode="NULLABLE"),
    SchemaField("caption", "STRING", mode="NULLABLE"),
    SchemaField("alt", "STRING", mode="NULLABLE"),
    SchemaField("source", "STRING", mode="NULLABLE"),
    SchemaField("publishedDate", "DATETIME", mode="NULLABLE"),
    SchemaField("updatedDate", "DATETIME", mode="NULLABLE"),
    SchemaField("contentType", "STRING", mode="NULLABLE"),
    SchemaField("name", "STRING", mode="NULLABLE"),
    SchemaField("author", "STRING", mode="NULLABLE"),
]

author_schema = [
    SchemaField("id", "STRING", mode="NULLABLE"),
    SchemaField("contentType", "STRING", mode="NULLABLE"),
    SchemaField("name", "STRING", mode="NULLABLE"),
    SchemaField("givenName", "STRING", mode="NULLABLE"),
    SchemaField("familyName", "STRING", mode="NULLABLE"),
    SchemaField("email", "STRING", mode="NULLABLE"),
    SchemaField("url", "STRING", mode="NULLABLE"),
    SchemaField(
        "socialMediaHandles",
        "RECORD",
        mode="REPEATED",
        fields=social_media_handle_schema,
    ),
    SchemaField("jobTitle", "STRING", mode="NULLABLE"),
    SchemaField("biography", "STRING", mode="NULLABLE"),
    SchemaField("location", "STRING", mode="NULLABLE"),
    SchemaField("searchIndexed", "BOOL", mode="NULLABLE"),
    SchemaField("profileVisibility", "BOOL", mode="NULLABLE"),
    SchemaField("published", "BOOL", mode="NULLABLE"),
    SchemaField(
        "profilePicture",
        "RECORD",
        mode="NULLABLE",
        fields=profile_picture_schema,
    ),
    SchemaField("phoneNumber", "STRING", mode="NULLABLE"),
]

# Define the schema based on ContentItemMeta
BQ_SCHEMA = [
    SchemaField("id", "STRING", mode="NULLABLE"),
    SchemaField("drupalId", "STRING", mode="NULLABLE"),
    SchemaField("contentType", "STRING", mode="NULLABLE"),
    SchemaField("url", "STRING", mode="NULLABLE"),
    SchemaField("headline", "STRING", mode="NULLABLE"),
    SchemaField("headlinePrimary", "BOOL", mode="NULLABLE"),
    SchemaField("slug", "STRING", mode="NULLABLE"),
    SchemaField("byline", "RECORD", mode="NULLABLE", fields=byline_schema),
    SchemaField("status", "STRING", mode="NULLABLE"),
    SchemaField("createdDate", "DATETIME", mode="NULLABLE"),
    SchemaField("publishedDate", "DATETIME", mode="NULLABLE"),
    SchemaField("updatedDate", "DATETIME", mode="NULLABLE"),
    SchemaField("teaser", "RECORD", mode="NULLABLE", fields=teaser_schema),
    SchemaField("body", "RECORD", mode="REPEATED", fields=body_schema),
    SchemaField("source", "STRING", mode="NULLABLE"),
    SchemaField("section", "STRING", mode="NULLABLE"),
    SchemaField("teams", "STRING", mode="REPEATED"),
    SchemaField("typeOfWorkLabel", "STRING", mode="REPEATED"),
    SchemaField("liveBlog", "BOOL", mode="NULLABLE"),
    SchemaField("searchIndexed", "BOOL", mode="NULLABLE"),
    SchemaField("topics", "STRING", mode="REPEATED"),
    SchemaField("entities", "STRING", mode="REPEATED"),
    SchemaField("sensitivity", "STRING", mode="NULLABLE"),
    SchemaField("sentiment", "BOOL", mode="NULLABLE"),
    SchemaField("newsValue", "INT64", mode="NULLABLE"),
    SchemaField("lifetime", "STRING", mode="NULLABLE"),
    SchemaField("comments", "BOOL", mode="NULLABLE"),
    SchemaField(
        "mainPublicationChannel",
        "RECORD",
        mode="NULLABLE",
        fields=mainpublicationchannel_schema,
    ),
    SchemaField("author", "RECORD", mode="REPEATED", fields=author_schema),
]
