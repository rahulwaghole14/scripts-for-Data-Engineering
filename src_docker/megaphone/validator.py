from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class Impression(BaseModel):
    order_id: Optional[str] = Field(None, description="Order ID")
    load_dts: Optional[datetime] = Field(None, description="Load Timestamp")
    position: Optional[int] = Field(None, description="Position")
    organization_id: Optional[str] = Field(None, description="Organization ID")
    order_downloaded: Optional[bool] = Field(None, description="Order Downloaded")
    ad_source: Optional[str] = Field(None, description="Ad Source")
    network_id: Optional[str] = Field(None, description="Network ID")
    seconds_downloaded: Optional[int] = Field(None, description="Seconds Downloaded")
    blacklisted_ua: Optional[bool] = Field(None, description="Blacklisted User Agent")
    bytes_sent: Optional[int] = Field(None, description="Bytes Sent")
    episode_id: Optional[str] = Field(None, description="Episode ID")
    advertisement_id: Optional[str] = Field(None, description="Advertisement ID")
    type: Optional[str] = Field(None, description="Type")
    created_at: Optional[datetime] = Field(None, description="Created At")
    blacklisted_ip: Optional[bool] = Field(None, description="Blacklisted IP")
    normalized_user_agent: Optional[str] = Field(None, description="Normalized User Agent")
    podcast_id: Optional[str] = Field(None, description="Podcast ID")
    byte_offset: Optional[int] = Field(None, description="Byte Offset")
    metric_id: Optional[str] = Field(None, description="Metric ID")

    class Config:
        extra = "ignore"

class Metrics(BaseModel):
    load_dts: Optional[datetime] = Field(None, description="Load Timestamp")
    region: Optional[str] = Field(None, description="Region")
    id: Optional[str] = Field(None, description="ID")
    user_agent: Optional[str] = Field(None, description="User Agent")
    blacklisted_ip: Optional[bool] = Field(None, description="Blacklisted IP")
    blacklisted_ua: Optional[bool] = Field(None, description="Blacklisted User Agent")
    bytes_sent: Optional[int] = Field(None, description="Bytes Sent")
    episode_id: Optional[str] = Field(None, description="Episode ID")
    country: Optional[str] = Field(None, description="Country")
    filesize: Optional[int] = Field(None, description="Filesize")
    seconds_downloaded: Optional[int] = Field(None, description="Seconds Downloaded")
    source: Optional[int] = Field(None, description="Source")
    created_at: Optional[datetime] = Field(None, description="Created At")
    normalized_user_agent: Optional[str] = Field(None, description="Normalized User Agent")
    ip: Optional[str] = Field(None, description="IP")
    city: Optional[str] = Field(None, description="City")
    duration: Optional[float] = Field(None, description="Duration")
    dma_name: Optional[str] = Field(None, description="DMA Name")
    podcast_id: Optional[str] = Field(None, description="Podcast ID")

    class Config:
        extra = "ignore"
