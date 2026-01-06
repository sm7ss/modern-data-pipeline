from dotenv import load_dotenv
import os
import polars as pl
import gc
from typing import Dict, Any, Callable, Optional
import logging

import psycopg2
import io 

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class PostgresDatabase: 
    @classmethod
    def uri_database(cls) -> str: 
        load_dotenv()
        
        user= os.getenv('POSTGRES_USER')
        password= os.getenv('POSTGRES_PASSWORD')
        db= os.getenv('POSTGRES_DB')
        postgresql_host_localhost= os.getenv('POSTGRES_HOST', 'localhost')
        postgresql_port= os.getenv('POSTGRES_PORT', '5432')
        
        uri= f'postgresql://{user}:{password}@{postgresql_host_localhost}:{postgresql_port}/{db}'
        return uri
    
    @classmethod
    def optimal_batch_size(cls, StreamingCSVHandler: Callable, file_overhead: Dict[str, Any]) -> int: 
        risk_factor= StreamingCSVHandler
        bytes_total_file= file_overhead.get('memoria_total_estimada', 'archivo_descomprimido')
        filas_totales= file_overhead['total_de_filas']
        safety_memory= file_overhead['safety_memory']

        bytes_per_row= bytes_total_file/filas_totales
        max_batch= int((safety_memory*risk_factor)/bytes_per_row)

        batch_size= min(max_batch, 250000)
        batch_size= max(batch_size, 10000)
        batch_size= min(batch_size, filas_totales)
        logger.info(f'El batch_size oprimto es de {batch_size}')
        return batch_size
    
    @classmethod
    def optimal_batch_size_streaming(cls, StreamingCSVHandler: Callable, frame: pl.LazyFrame, n_rows_size: int, file_overhead: Dict[str, Any]) -> int: 
        risk_factor= StreamingCSVHandler
        bytes_total_frame= frame.estimated_size()
        safety_memory= file_overhead['safety_memory']
        
        bytes_per_row= bytes_total_frame/n_rows_size
        max_batch= ((risk_factor*safety_memory)/bytes_per_row)
        
        batch_size= min(max_batch, 250000)
        batch_size= max(batch_size, 10000)
        batch_size= min(batch_size, n_rows_size)
        logger.info(f'El batch_size oprimto es de {batch_size}')
        return batch_size
    
    @classmethod
    def insert_data_to_database(cls, StreamingCSVHandler: Callable, frame: pl.LazyFrame,  table_name: str, file_overhead: Dict[str, Any], if_table_exists: str, n_rows: Optional[int]=None) -> None: 
        decision= file_overhead['decision']
        if decision in ['eager', 'lazy']: 
            filas_totales= file_overhead['total_de_filas']
            optimal_batch_size= cls.optimal_batch_size(StreamingCSVHandler=StreamingCSVHandler, file_overhead=file_overhead)
        else: 
            filas_totales= n_rows
            #esto consume demasiado, no apto realmente para streaming, a menos que los bathces de procesameinto en streaming sean demasiado grandes
            optimal_batch_size= cls.optimal_batch_size_streaming(StreamingCSVHandler=StreamingCSVHandler, frame=frame, n_rows_size=filas_totales, file_overhead=file_overhead)
        
        uri= cls.uri_database()
        conn= None
        
        try: 
            conn= psycopg2.connect(uri)
            
            frame.slice(0,0).collect(engine='streaming').write_database(
                table_name=table_name, 
                connection=uri, 
                if_table_exists=if_table_exists
            )
            
            for offset in range(0, filas_totales, optimal_batch_size): 
                df= frame.slice(offset, optimal_batch_size).collect(engine='streaming')
                
                csv_buff= io.StringIO()
                df.write_csv(csv_buff)
                csv_buff.seek(0)
                
                with conn.cursor() as cur: 
                    cur.copy_expert(f'COPY {table_name} FROM STDIN WITH CSV HEADER', csv_buff)
                logger.info(f'Batch {offset//optimal_batch_size+1} insertado ({len(df)} filas)')
                
                del df
                gc.collect()
            conn.commit()
        except Exception as e: 
            logger.error(f'Ocurrio un error al querer insertar los datos a la tabla {table_name}.\n{e}')
            conn.rollback()
            raise 
        finally: 
            if conn: 
                conn.close()
