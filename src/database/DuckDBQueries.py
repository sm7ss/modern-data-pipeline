import duckdb 
import polars as pl
import os
from dotenv import load_dotenv

import logging 

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class DuckDBPostgresConnector: 
    _attached= False
    
    @classmethod
    def _secret_registro(cls): 
        if cls._attached: 
            return 
        
        load_dotenv()
        duckdb.connect()
        
        duckdb.sql("INSTALL postgres;")
        duckdb.sql("LOAD postgres;")
        
        host= os.getenv('POSTGRES_HOST')
        port= os.getenv('POSTGRES_PORT')
        db= os.getenv('POSTGRES_DB')
        user= os.getenv('POSTGRES_USER')
        password= os.getenv('POSTGRES_PASSWORD')
        
        duckdb.sql(f"""
        CREATE SECRET IF NOT EXISTS pg_secret_key (
            TYPE POSTGRES,
            HOST '{host}',
            PORT {port},
            DATABASE '{db}',
            USER '{user}',
            PASSWORD '{password}'
        );
        """)
        
        try: 
            duckdb.sql("ATTACH '' AS pg_main (TYPE POSTGRES, SECRET pg_secret_key);")
        except Exception as e: 
            pass
        
        cls._attached= True
        logger.info('\nSecreto de PostgreSQL registrado en DuckDB')
    
    @classmethod
    def query(cls, sql: str) -> pl.DataFrame: 
        cls._secret_registro()
        try: 
            result= duckdb.sql(sql)
            logger.info('\nConsulta ejecutada con exito')
            return result.pl()
        except Exception as e: 
            logger.error(f'\nOcurrio un error en la consulta:\n{e}')
            raise


