import logging
from logger import logger

from common.reports import * 
from common.processing import * 

logger('replatform-analysis')

""" logging.basicConfig(
   level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("Replatform_Analysis.log"),
        logging.StreamHandler()
    ])
"""


if __name__ == "__main__":
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
