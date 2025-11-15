import logging
import time
from .logger import logger
import psutil

from common.reports.site_section_report import call_section_report
from common.reports.region_report import call_region_report
from common.reports.logged_in_UVs_report import call_logged_in_UVs_report
from common.reports.referral_domain_report import call_referral_domain_report
from common.reports.platform_reports import call_platform_report
from common.reports.logged_in_users_report import call_logged_in_users_report
from common.reports.social_referral_traffic_report import (
    call_social_referral_traffic_report,
)
from common.reports.search_referral_traffic_report import (
    call_search_referral_traffic_report,
)
from common.processing.process_logged_in_users import process_logged_in_users
from common.reports.flow_report import call_flow_report
from common.processing.process_flow_queries import process_flows

logger("replatform")


def get_system_resources():
    # Get the number of logical CPUs
    cpu_count = psutil.cpu_count(logical=True)
    # Get the total memory in GB
    total_memory = psutil.virtual_memory().total / (
        1024**3
    )  # Convert bytes to GB
    return cpu_count, total_memory


def get_current_usage():
    # Get the current CPU usage percentage
    cpu_usage = psutil.cpu_percent(interval=1)
    # Get the current memory usage
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.used / (1024**3)  # Convert bytes to GB
    return cpu_usage, memory_usage


if __name__ == "__main__":
    cpu_usage, memory_usage = get_current_usage()
    print(f"Current CPU usage: {cpu_usage}%")
    print(f"Current memory usage: {memory_usage:.2f} GB")
    call_section_report()
    call_region_report()
    call_logged_in_UVs_report()
    call_referral_domain_report()
    call_platform_report()
    call_logged_in_users_report()
    call_social_referral_traffic_report()
    call_search_referral_traffic_report()
    process_logged_in_users()
    call_flow_report()
    process_flows()
