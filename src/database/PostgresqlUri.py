from dotenv import load_dotenv
import os
import polars as pl
import gc
from typing import Dict, Any
import logging
import psutil
from pathlib import Path

import pyarrow.csv as pv

import psycopg2
import io 

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class PostgresDatabase: 
    def __init__(self, table_name: str, file_overhead: Dict[str, Any], if_table_exists: str):
        self.table_name= table_name
        self.file_overhead= file_overhead
        self.if_table_exists= if_table_exists
    
    def uri_database(self) -> str: 
        ruta= Path(__file__).resolve().parent.parent.parent
        ruta_env= ruta / ".env"
        load_dotenv(ruta_env)
        
        user= os.getenv('POSTGRES_USER')
        password= os.getenv('POSTGRES_PASSWORD')
        db= os.getenv('POSTGRES_DB')
        postgresql_host_localhost= os.getenv('POSTGRES_HOST', 'localhost')
        postgresql_port= os.getenv('POSTGRES_PORT', '5432')
        
        uri= f'postgresql://{user}:{password}@{postgresql_host_localhost}:{postgresql_port}/{db}'
        return uri
    
    def current_memory(self) -> int: 
        process= psutil.Process()
        return process.memory_info().rss
    
    def optimal_batch_size(self, memoria_del_proceso: int, batch_size: int=2_000_000) -> int: 
        memoria_disponible= psutil.virtual_memory().available
        
        memory_pressure= memoria_del_proceso/memoria_disponible
        
        if memory_pressure >= 0.9: 
            batch_size= 500_000
            logger.warning(f'Alta presion en memoria {memory_pressure:.2f}. Batch size nuevo {batch_size}')
        elif memory_pressure < 0.6: 
            batch_size=1_000_000
            logger.warning(f'Baja presion en memoria {memory_pressure:.2f}. Batch size nuevo {batch_size}')
        elif memory_pressure <= 0.3: 
            batch_size= int(batch_size*1.3)
            logger.warning(f'Baja presion en memoria {memory_pressure:.2f}. Batch size nuevo {batch_size}')
        else: 
            batch_size= batch_size
        
        return batch_size
    
    def insert_data_to_database(self, frame: pl.LazyFrame) -> None: 
        filas_totales= self.file_overhead['total_de_filas']
        
        uri= self.uri_database()
        conn= None
        
        try: 
            conn= psycopg2.connect(uri)
            logger.info('\nSe conecto correctamente a la base de datos')
            
            frame.slice(0,0).collect(engine='streaming').write_database(
                table_name=self.table_name, 
                connection=uri, 
                if_table_exists=self.if_table_exists
            )
            
            offset= 0
            batch= 0
            
            while offset < filas_totales:
                mp= self.current_memory()
                optimal_batch_size= self.optimal_batch_size(memoria_del_proceso=mp)
                
                df= frame.slice(offset, optimal_batch_size).collect(engine='streaming').to_arrow()
                
                csv_buff= io.BytesIO()
                pv.write_csv(df, csv_buff)
                csv_buff.seek(0)
                
                with conn.cursor() as cur: 
                    cur.copy_expert(f'COPY {self.table_name} FROM STDIN WITH CSV HEADER', csv_buff)
                logger.info(f'Batch {batch+1} insertado ({len(df)} filas)')
                
                del df
                gc.collect()
                
                batch+=1
                offset+=optimal_batch_size
            conn.commit()
        except Exception as e: 
            logger.error(f'Ocurrio un error al querer insertar los datos a la tabla {self.table_name}.\n{e}')
            conn.rollback()
            raise 
        finally: 
            if conn: 
                conn.close()
    
    def database_insert_data(self, frame: pl.LazyFrame) -> None: 
        decision= self.file_overhead['decision']
        
        if decision in ['lazy', 'eager']: 
            self.insert_data_to_database(frame= frame)
        else: 
            logger.error('Sin soprte para ingesta de datos en streming')
            raise 'Sin soprte para ingesta de datos en streming'


