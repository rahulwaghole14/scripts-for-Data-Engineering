import logging
import os
import pandas as pd
from datetime import timedelta,datetime,date
from common import DATA_FILES_PATH
from common.reports.report_query import process_report
from common.oberon_queries.queries.multi_dimension_query import srp_multi_dim
from common.oberon_queries.oberon_functions import update_query_limit, update_query_datetime
from common.big_query.create_client import create_bq_client
from common.big_query.check_dataset_exists import check_bq_table
from common.big_query.upload_to_bq import biq_query_upload
from common.big_query.check_multiple_dimension_BQ import get_BQ_table_multiple_dims
from common.big_query.remove_today_bq import delete_day_from_table
from common.oberon_queries.queries.editorial_metrics_bq_schema import logged_in_users_bq_schema
from common.segments.segments_queries import segment_info, update_segment_defintion
from common.segments.update_segments import update_segment_defn, update_segment_negative_defn
replatform = True


if replatform:
    numdays = 7
    query = srp_multi_dim
    file_name_root = 'srp_multi'
    day_limit = 1
    query_time_period = 1
    page_type = ['article','section','home']
    #region = ['Auckland (New Zealand)','Greater Wellington (New Zealand)','Canterbury (New Zealand)',
    #          'Waikato (New Zealand)', 'Otago (New Zealand)', 'Bay of Plenty (New Zealand)', 'Manawatu-Whanganui (New Zealand)','Hawkes Bay (New Zealand)', 'Taranaki (New Zealand)','Nelson (New Zealand)', 'Northland (New Zealand)', 'Southland (New Zealand)', 'Marlborough (New Zealand)']
    region = [3946610563,2532660862,3646184479,3176311920,4144815276,2157854076,4051110799,265552321,712305258,757256710,4225171186,485462375,493465973]
    sys_environs = ['web','android','ios']
    resolutions = ['desktop','mobile','tablet']
    vertical = ['business','nz-news','sports','quizzes','travel','world-news','culture','poltics','home-property',
                'money','motoring','wellbeing','food-drink','climate-change','home']

else:
    numdays = 180
    #query = old_referral_traffic
    file_name_root = 'referrals'
    day_limit = 1
    query_time_period = 1


a = datetime.today()
dateList = []
for x in range (day_limit, numdays):
    dateList.append(a - timedelta(days =  x))


update_query_limit(query, 10000)


###########################################
##  Set Big Query Dataset ID & Table ID  ##
###########################################

client = create_bq_client()
dataset_id = 'hexa_replatform_analysis'
table_id = 'multiple_dimension_report'




#bq_date_list = []
str_today = a.strftime("%Y-%m-%d")

#print(str_today)




def call_multi_dim_report():
        check_sy_env = False
        if check_sy_env:
            for s_e in sys_environs + ['other']:
                update_sys_env = True
                print("System Environment")
                sys_env_segment = 's200000657_65b2d4c37dd7b567b6c89564'
                sys_env_def = segment_info(sys_env_segment)
                print(sys_env_def)
                if s_e != 'other':
                    s_e_defn = update_segment_defn(sys_env_def['definition'], s_e)
                elif s_e == 'other':
                    s_e_defn = update_segment_negative_defn(sys_env_def['definition'], sys_environs)
                update_segment_defintion(sys_env_segment, s_e_defn)
                print(segment_info(sys_env_segment)['definition'])
            quit()
        
    
        check_regions = False
        if check_regions:
            region_segment = 's200000657_65b2d574176d3a4097d4b773'
            region_def = segment_info(region_segment)
            print(region_def['definition'])
            for a in region_def['definition']['container']['pred']['preds']:
                print(a['num'])

            quit()
        logging.info('STARTED - Content Audit Adobe Metrics')

        logging.info('Update Adobe Token')

        
        if check_bq_table(client, dataset_id, table_id):
            field_check = get_BQ_table_multiple_dims(client, dataset_id, table_id)
        else:
            field_check = []
        
        for v in vertical + ['other']:
            update_vertical = True
            print("Vertical")
            vert_segment = 's200000657_65b2d5dc7dd7b567b6c89567'
            vert_def = segment_info(vert_segment)

            for page_t in page_type + ['other']:
                update_page_type = True
                print("Page Type")
                page_type_segment = 's200000657_65b2d5537dd7b567b6c89566'
                page_type_def = segment_info(page_type_segment)
            
            

                for reg in region + ['other']:
                    update_region = True
                    print("Region")
                    region_segment = 's200000657_65b2d574176d3a4097d4b773'
                    region_def = segment_info(region_segment)
                

                    for s_e in sys_environs + ['other']:
                        update_sys_env = True
                        print("System Environment")
                        sys_env_segment = 's200000657_65b2d4c37dd7b567b6c89564'
                        sys_env_def = segment_info(sys_env_segment)
                    


                        for res in resolutions + ['other']:
                            update_resolution = True
                            print("Resolution")
                            res_segment = 's200000657_65b2d5337dd7b567b6c89565'
                            res_def = segment_info(res_segment)
                            
                        
                            
                            check_str = str(v) + '-' + str(s_e) + '-' +  str(res) + '-' + str(reg) + '-' + str(page_t)
                            if check_str not in field_check:

                                if update_vertical:
                                    if v != 'other':
                                            v_defn = update_segment_defn(vert_def['definition'], v)
                                    elif v == 'other':
                                        v_defn = update_segment_negative_defn(vert_def['definition'], vertical)
                                    update_segment_defintion(vert_segment, v_defn)
                                    update_vertical = False

                                if update_resolution:
                                    if res != 'other':
                                        res_defn = update_segment_defn(res_def['definition'], res)
                                    elif res == 'other':
                                        res_defn = update_segment_negative_defn(res_def['definition'], resolutions)
                                    update_segment_defintion(res_segment, res_defn)
                                    update_resolution = False
                                
                                if update_sys_env:
                                    if s_e != 'other':
                                        s_e_defn = update_segment_defn(sys_env_def['definition'], s_e)
                                    elif s_e == 'other':
                                        s_e_defn = update_segment_negative_defn(sys_env_def['definition'], sys_environs)
                                    update_segment_defintion(sys_env_segment, s_e_defn)
                                    update_sys_env = False
                                
                                if update_region:
                                    if reg != 'other':
                                        region_defn = update_segment_defn(region_def['definition'], reg)
                                    elif reg == 'other':
                                        region_defn = update_segment_negative_defn(region_def['definition'], region)
                                    update_segment_defintion(region_segment, region_defn)
                                    update_region = False
                                
                                if update_page_type:
                                    if page_t != 'other':
                                        new_page_type_defn = update_segment_defn(page_type_def['definition'], page_t)
                                    elif page_t == 'other':
                                        new_page_type_defn = update_segment_negative_defn(page_type_def['definition'], page_type)
                                    update_segment_defintion(page_type_segment, new_page_type_defn)
                                    update_page_type = False


                                logging.info(f'Downloading Report from Adobe Analytics for {check_str}')
                                df = process_report(query)
                                df['Vertical'] = v
                                df['Resolution'] = res
                                df['System Environment'] = s_e
                                df['Region'] = reg
                                df['Page Type'] = page_t
                                df['x'] = pd.to_datetime(df['Day'], format='%b %d, %Y')
                                df['date'] = df['x'].dt.strftime('%Y-%m-%d')
                                df = df.drop(['x'], axis=1)
                                #print(df)
                                biq_query_upload(client,df, dataset_id, table_id, logged_in_users_bq_schema)
                            else:
                                print(f'{check_str} in data already')

                                



            
            '''
            #update page type
            date_start = date_.strftime('%Y-%m-%d')
            date_end = (date_ + timedelta(days = query_time_period)).strftime('%Y-%m-%d')

            filename = file_name_root + '_' + date_start + '.csv'
            data_file = os.path.join(str(DATA_FILES_PATH), filename)

            logging.info(f'Creating Query for {date_start}')
            update_query_datetime(query, date_start, date_end)
            #if not(os.path.exists(data_file)):
            if date_start not in bq_date_list:
                logging.info('Downloading Report from Adobe Analytics')
                df = process_report(query)
                df['date'] = date_start
                biq_query_upload(client,df, dataset_id, table_id, logged_in_users_bq_schema)
                #df.to_csv(data_file, index=False)
            '''
