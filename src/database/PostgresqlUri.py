from dotenv import load_dotenv
import os
import polars as pl
import gc
from typing import Dict, Any, Callable, Optional
import logging
import psutil

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
        load_dotenv()
        
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
            batch_size= max(500_000, int(batch_size*0.3))
            logger.warning(f'Alta presion en memoria {memory_pressure:.2f}. Batch size nuevo {batch_size}')
        
        elif memory_pressure <= 0.4: 
            batch_size= min(batch_size, int(batch_size*1.3))
            logger.warning(f'Baja presion en memoria {memory_pressure:.2f}. Batch size nuevo {batch_size}')
        
        else: 
            batch_size= batch_size
        
        return batch_size
    
    def optimal_batch_size_streaming(self, StreamingCSVHandler: Callable, frame: pl.LazyFrame, n_rows_size: int) -> int: 
        risk_factor= StreamingCSVHandler
        bytes_total_frame= frame.estimated_size()
        safety_memory= self.file_overhead['safety_memory']
        
        bytes_per_row= bytes_total_frame/n_rows_size
        max_batch= ((risk_factor*safety_memory)/bytes_per_row)
        
        batch_size= min(max_batch, 500000)
        batch_size= max(batch_size, 250000)
        batch_size= min(batch_size, n_rows_size)
        logger.info(f'El batch_size oprimto es de {batch_size}')
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
    
    def insert_data_to_database_streaming(self, StreamingCSVHandler: Callable, frame: pl.LazyFrame, n_rows: int) -> None: 
        filas_totales= n_rows
        #esto consume demasiado, no apto realmente para streaming, a menos que los bathces de procesameinto en streaming sean demasiado grandes
        optimal_batch_size= self.optimal_batch_size_streaming(StreamingCSVHandler=StreamingCSVHandler, frame=frame, n_rows_size=filas_totales)
        
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
            
            for offset in range(0, filas_totales, optimal_batch_size): 
                df= frame.slice(offset, optimal_batch_size).collect(engine='streaming')
                
                csv_buff= io.StringIO()
                df.write_csv(csv_buff)
                csv_buff.seek(0)
                
                with conn.cursor() as cur: 
                    cur.copy_expert(f'COPY {self.table_name} FROM STDIN WITH CSV HEADER', csv_buff)
                logger.info(f'Batch {offset//optimal_batch_size+1} insertado ({len(df)} filas)')
                
                del df
                gc.collect()
            conn.commit()
        except Exception as e: 
            logger.error(f'Ocurrio un error al querer insertar los datos a la tabla {self.table_name}.\n{e}')
            conn.rollback()
            raise 
        finally: 
            if conn: 
                conn.close()
    
    def database_insert_data(self, 
            frame: pl.LazyFrame, 
            StreamingCSVHandler: Optional[Callable]= None, 
            n_rows: Optional[int]=None) -> None: 
        decision= self.file_overhead['decision']
        
        if decision in ['lazy', 'eager']: 
            self.insert_data_to_database(frame= frame)
        else: 
            self.insert_data_to_database_streaming(
                StreamingCSVHandler=StreamingCSVHandler, 
                frame=frame, 
                n_rows=n_rows
            )


