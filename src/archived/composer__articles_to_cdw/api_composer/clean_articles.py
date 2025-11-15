''' clean data '''
import pandas as pd

def add_columns(dataframe):
    ''' add columns to dataframe '''
    data_frame = dataframe
    current_time = pd.Timestamp.now()

    data_frame['hash_key'] = None
    data_frame['hash_diff'] = None
    data_frame['record_source'] = 'COMPOSER'
    data_frame['sequence_nr'] = None
    data_frame['record_load_dts_utc'] = current_time
    data_frame['record_load_dts_utc'] = pd.to_datetime(data_frame['record_load_dts_utc'], errors='coerce')
    # yyyy-mm-dd current_time.strftime('%Y-%m-%d')
    data_frame['load_dt'] = current_time.strftime('%Y-%m-%d')
    data_frame['load_dt'] = pd.to_datetime(data_frame['load_dt'], errors='coerce')
    data_frame['published_dts'] = pd.to_datetime(data_frame['published_dts'], errors='coerce')

    # author to uppercase
    data_frame['author'] = data_frame['author'].str.upper()

    # ints
    data_frame['content_id'] = data_frame['content_id'].astype(int)
    data_frame['word_count'] = data_frame['word_count'].astype(int)
    data_frame['image_count'] = data_frame['image_count'].astype(int)
    data_frame['video_count'] = data_frame['video_count'].astype(int)
    # convert False and True to 0 and 1
    data_frame['advertisement'] = data_frame['advertisement'].fillna(0.0).astype(int)
    data_frame['sponsored'] = data_frame['sponsored'].fillna(0.0).astype(int)
    data_frame['promoted_flag'] = data_frame['promoted_flag'].fillna(0.0).astype(int)
    data_frame['comments_flag'] = data_frame['comments_flag'].fillna(0.0).astype(int)
    data_frame['home_flag'] = data_frame['home_flag'].fillna(0.0).astype(int)

    return data_frame
