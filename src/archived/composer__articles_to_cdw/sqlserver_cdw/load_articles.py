'''Load articles from staging to cdw'''
import sqlalchemy
import logging

table_columns = {
    # sqlserver datatype
    'content_id': '[int] NULL',
    'title': '[nvarchar](max) NULL',
    'published_dts': '[datetime] NULL',
    'source': '[nvarchar](max) NULL',
    'brand': '[nvarchar](max) NULL',
    'category': '[nvarchar](max) NULL',
    'category_1': '[nvarchar](max) NULL',
    'category_2': '[nvarchar](max) NULL',
    'category_3': '[nvarchar](max) NULL',
    'category_4': '[nvarchar](max) NULL',
    'category_5': '[nvarchar](max) NULL',
    'category_6': '[nvarchar](max) NULL',
    'print_slug': '[nvarchar](max) NULL',
    'author': '[nvarchar](max) NULL',
    'word_count': '[int] NULL',
    'image_count': '[int] NULL',
    'video_count': '[int] NULL',
    'advertisement': '[int] NULL',
    'sponsored': '[int] NULL',
    'promoted_flag': '[int] NULL',
    'comments_flag': '[int] NULL',
    'home_flag': '[int] NULL',
    'hash_key': '[nvarchar](max) NULL',
    'hash_diff': '[nvarchar](max) NULL',
    'record_source': '[nvarchar](max) NULL',
    'sequence_nr': '[int] NULL',
    'record_load_dts_utc': '[datetime] NULL',
    'load_dt': '[datetime] NULL'
}

table_columns_sqlalchemy = {
    # sqlserver datatype
    'content_id': sqlalchemy.types.INTEGER,
    'title': sqlalchemy.types.NVARCHAR,
    'published_dts': sqlalchemy.types.DateTime(),
    'source': sqlalchemy.types.NVARCHAR,
    'brand': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.NVARCHAR,
    'category_1': sqlalchemy.types.NVARCHAR,
    'category_2': sqlalchemy.types.NVARCHAR,
    'category_3': sqlalchemy.types.NVARCHAR,
    'category_4': sqlalchemy.types.NVARCHAR,
    'category_5': sqlalchemy.types.NVARCHAR,
    'category_6': sqlalchemy.types.NVARCHAR,
    'print_slug': sqlalchemy.types.NVARCHAR,
    'author': sqlalchemy.types.NVARCHAR,
    'word_count': sqlalchemy.types.INTEGER,
    'image_count': sqlalchemy.types.INTEGER,
    'video_count': sqlalchemy.types.INTEGER,
    'advertisement': sqlalchemy.types.INTEGER,
    'sponsored': sqlalchemy.types.INTEGER,
    'promoted_flag': sqlalchemy.types.INTEGER,
    'comments_flag': sqlalchemy.types.INTEGER,
    'home_flag': sqlalchemy.types.INTEGER,
    'hash_key': sqlalchemy.types.NVARCHAR,
    'hash_diff': sqlalchemy.types.NVARCHAR,
    'record_source': sqlalchemy.types.NVARCHAR,
    'sequence_nr': sqlalchemy.types.INTEGER,
    'record_load_dts_utc': sqlalchemy.types.DateTime(),
    'load_dt': sqlalchemy.types.DateTime()
}


def create_table(table_name, schema_name, engine):
    '''Create table in sqlserver'''
    try:
        with engine.connect() as conn:
            # Create DROP TABLE statement and CREATE TABLE statement
            drop_create_table_sql = 'DROP TABLE IF EXISTS %s.%s; CREATE TABLE %s.%s (' % (schema_name, table_name, schema_name, table_name)
            for column_name, column_type in table_columns.items():
                drop_create_table_sql += column_name + ' ' + column_type + ','
            drop_create_table_sql = drop_create_table_sql[:-1] + ');'

            # Execute statements
            conn.execute(drop_create_table_sql)
    except Exception as error:
        raise error


def load_data(data_frame, table_name, schema_name, engine):
    '''Write to sqlserver'''

    # write to temp table
    try:
        temp_table_name = 'temp_' + table_name
        data_frame.to_sql(temp_table_name,
                          engine,
                          schema=schema_name,
                          if_exists='replace',
                          index=False,
                          dtype=table_columns_sqlalchemy,
                          chunksize=1000)
    except Exception as error:
        raise error

def merge_data(table_name, schema_name, engine):
    '''Merge data from temp table to table'''

    temp_table_name = 'temp_' + table_name
    primary_key = 'content_id'

    sql_insert = f"""
        INSERT INTO {schema_name}.{table_name} (
            record_load_dts_utc,
            sequence_nr,
            record_source,
            hash_key,
            hash_diff,
            category,
            {primary_key},
            title,
            author,
            published_dts,
            source,
            brand,
            category_1,
            category_2,
            category_3,
            category_4,
            category_5,
            category_6,
            word_count,
            print_slug,
            advertisement,
            sponsored,
            promoted_flag,
            comments_flag,
            home_flag,
            image_count,
            video_count,
            load_dt
        )
        SELECT
            t.record_load_dts_utc,
            t.sequence_nr,
            t.record_source,
            t.hash_key,
            t.hash_diff,
            t.category,
            t.{primary_key},
            t.title,
            t.author,
            t.published_dts,
            t.source,
            t.brand,
            t.category_1,
            t.category_2,
            t.category_3,
            t.category_4,
            t.category_5,
            t.category_6,
            t.word_count,
            t.print_slug,
            t.advertisement,
            t.sponsored,
            t.promoted_flag,
            t.comments_flag,
            t.home_flag,
            t.image_count,
            t.video_count,
            t.load_dt
        FROM {schema_name}.{temp_table_name} AS t
        LEFT JOIN {schema_name}.{table_name} AS c ON t.{primary_key} = c.{primary_key}
        WHERE c.{primary_key} IS NULL;
        """

    sql_update = f"""
        UPDATE {schema_name}.{table_name}
        SET
            [record_load_dts_utc] = s.[record_load_dts_utc],
            [sequence_nr] = s.[sequence_nr],
            [record_source] = s.[record_source],
            [hash_key] = s.[hash_key],
            [hash_diff] = s.[hash_diff],
            [category] = s.[category],
            {primary_key} = s.{primary_key},
            [title] = s.[title],
            [author] = s.[author],
            [published_dts] = s.[published_dts],
            [source] = s.[source],
            [brand] = s.[brand],
            [category_1] = s.[category_1],
            [category_2] = s.[category_2],
            [category_3] = s.[category_3],
            [category_4] = s.[category_4],
            [category_5] = s.[category_5],
            [category_6] = s.[category_6],
            [word_count] = s.[word_count],
            [print_slug] = s.[print_slug],
            [advertisement] = s.[advertisement],
            [sponsored] = s.[sponsored],
            [promoted_flag] = s.[promoted_flag],
            [comments_flag] = s.[comments_flag],
            [home_flag] = s.[home_flag],
            [image_count] = s.[image_count],
            [video_count] = s.[video_count],
            [load_dt] = s.[load_dt]
        FROM {schema_name}.{temp_table_name} s
        WHERE {table_name}.{primary_key} = s.{primary_key};
    """

    try:
        # merge data
        with engine.connect() as conn:
            conn.execute(sql_insert)
            conn.execute(sql_update)
            conn.execute(f"DROP TABLE IF EXISTS {schema_name}.{temp_table_name};")
    except Exception as error:
        logging.info(error)

#     stored_procedure(engine,SCHEMA_NAME,SP_NAME="load_vault")
