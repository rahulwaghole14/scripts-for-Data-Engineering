''' Run stored procedure to generate random number and insert into table '''
import logging
from sqlalchemy import text

def stored_procedure(engine, SCHEMA, SP_NAME):
    ''' Run stored procedure in SCHEMA called SP_NAME'''
    try:
        with engine.begin() as connection:
            logging.info(f"Running stored procedure {SCHEMA}.{SP_NAME}")
            sqlsp = text(f"EXEC {SCHEMA}.{SP_NAME};")
            connection.execute(sqlsp)
    except Exception as error:
        logging.info(f"Error when executing stored procedure {SP_NAME}: {error}")
        logging.info(f"Full exception: {repr(error)}")
        raise
