''' Run stored procedure to generate random number and insert into table '''
import logging

def stored_procedure(engine, SCHEMA, SP_NAME, component=''):
    ''' Run stored procedure in SCHEMA called SP_NAME'''
    try:
        with engine.begin() as connection:
            logging.info(f"Executing {SCHEMA}.{SP_NAME} {component}")
            connection.execute(f"EXEC {SCHEMA}.{SP_NAME} {component};")
    except Exception as error:
        logging.info(error)
