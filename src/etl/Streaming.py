import logging 
import polars as pl 
from typing import Dict, Any, Callable
from pathlib import Path
from pydantic import BaseModel
import gc
import psutil
import time
import tracemalloc

from..validation.PanderaSchema import PanderaSchema
from ..memory_optimizer.PathDecisionMaker import PipelineEstimatedSizeFiles
from ..database.PostgresqlUri import PostgresDatabase

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class StreamingCSVHandler: 
    def __init__(self, archivo: Path, file_overhead: Dict[str, Any], os_margin: float=0.3, n_rows_sample: int=1000):
        self.os_margin= os_margin
        self.n_rows_sample= n_rows_sample
        
        self.archivo= archivo
        self.file_overhead= file_overhead
    
    def _file_overhead(self, archivo: str) -> Dict[str, Any]: 
        archivo= Path(archivo)
        diccionario= PipelineEstimatedSizeFiles(archivo=archivo, os_margin=self.os_margin, n_rows_sample=self.n_rows_sample).estimated_size_file()
        return diccionario, archivo
    
    def estimate_batch_size(self) -> float: 
        ratio= self.file_overhead['ratio']
        
        if ratio > 5: 
            return 0.15 
        elif ratio > 2: 
            return 0.25
        elif ratio > 0.5: 
            return 0.4
        else: 
            return 0.7
    
    def csv_batch_size_row(self) -> int: 
        batch_float= self.estimate_batch_size()
        total_rows= self.file_overhead['total_de_filas']
        
        batch_rows= int(total_rows*batch_float)
        
        batch_rows= max(batch_rows, 10000)
        batch_rows= min(batch_rows, total_rows)
        return batch_rows
    
    def run_streaming(self, ETL: Callable, model: BaseModel) -> None: 
        #Hacer otro engine aquí en caso de que las rows sean demasiadas para procesar en eager o lazy mode
        row_size= self.csv_batch_size_row()
        table_name= model.database.table_name
        if_table_exists= model.database.if_table_exists
        
        first_chunk= pl.read_csv(self.archivo, n_rows=row_size)
        etl= ETL(Frame= first_chunk, model=model)
        frame= etl.etl()
        total_filas= len(frame)
        
        logger.info(f'Columnas {frame.height} procesadas exitosamente')
        
        PostgresDatabase.insert_data_to_database(
            StreamingCSVHandler=self.estimate_batch_size(), 
            frame=frame.lazy(), 
            table_name=table_name, 
            file_overhead=self.file_overhead, 
            if_table_exists=if_table_exists, 
            n_rows=total_filas
        )
        
        schema= frame.schema
        frame.write_parquet('pandera_report.parquet')
        diccionario, archivo= self._file_overhead(archivo='pandera_report.parquet')
        try: 
            PanderaSchema(model=model, archivo=archivo, file_overhead=diccionario)
        except Exception as e: 
            logger.error(f'El schema no es compatible. Ocurrio un error en la ejecucion:\n{e}')
            raise
        
        skip_chunk=row_size
        while True: 
            try: 
                next_chunk= pl.read_csv(
                    self.archivo, 
                    skip_rows=skip_chunk, 
                    n_rows=row_size, 
                    schema_overrides=schema
                )
                total_filas= len(next_chunk)
                
                if next_chunk.height==0: 
                    logger.warning('Archivo sin más filas a procesar')
                    return next_chunk
                
                frame= etl.etl()
                PostgresDatabase.insert_data_to_database(
                    StreamingCSVHandler=self.estimate_batch_size(), 
                    frame=next_chunk.lazy(), 
                    table_name=table_name, 
                    file_overhead=self.file_overhead, 
                    if_table_exists=if_table_exists, 
                    n_rows=total_filas
                )
                
                logger.info(f'Columnas {frame.height} procesadas exitosamente.\n{skip_chunk} saltadas')
                skip_chunk+=row_size
                
                del frame
                gc.collect()
            except Exception as e: 
                logger.warning(f'Fin del archivo u ocurrio un error: \n{e}')
                break

class StreamingParquetHanlder: 
    def __init__(self, archivo: Path, file_overhead: Dict[str, Any], os_margin: float=0.3, n_rows_sample: int=1000):
        self.os_margin= os_margin
        self.n_rows_sample= n_rows_sample
        
        self.archivo= archivo
        self.file_overhead= file_overhead['parquet_file_pyarrow']
        self.row_group= file_overhead['parquet_file_pyarrow'].num_row_groups
    
    def _file_overhead(self, archivo: str) -> Dict[str, Any]: 
        archivo= Path(archivo)
        diccionario= PipelineEstimatedSizeFiles(archivo=archivo, os_margin=self.os_margin, n_rows_sample=self.n_rows_sample).estimated_size_file()
        return diccionario, archivo
    
    def run_streaming(self, ETL: Callable, model: BaseModel) -> None: 
        table_name= model.database.table_name
        if_table_exists= model.database.if_table_exists
        streaming_csv_handler= StreamingCSVHandler(
            archivo=self.archivo, 
            file_overhead=self.file_overhead, 
            os_margin=self.os_margin, 
            n_rows_sample=self.n_rows_sample
        )
        
        schema_validado= False
        
        for i in range(self.row_group):
            logger.info(f'Procesando {i+1} de {self.row_group} totales de grupos')
            table= self.file_overhead.read_row_group(i)
            df= pl.from_arrow(table)
            
            transformed= ETL(Frame= df, model=model).etl()
            
            row_size= len(transformed)
            
            PostgresDatabase.insert_data_to_database(
                StreamingCSVHandler=streaming_csv_handler.estimate_batch_size(), 
                frame=transformed, 
                table_name=table_name, 
                file_overhead=self.file_overhead, 
                if_table_exists=if_table_exists, 
                n_rows=row_size
            )
            
            if not schema_validado: 
                transformed.write_parquet('pandera_report.parquet')
                diccionario, archivo= self._file_overhead(archivo='pandera_report.parquet')
                try: 
                    PanderaSchema(model=model, archivo=archivo, file_overhead=diccionario)
                    logger.info('El schema se conserva igual')
                except Exception as e: 
                    logger.error(f'El schema no es compatible. Ocurrio un error en la ejecucion:\n{e}')
                    raise
                schema_validado= True
                
                del df
                del transformed
                gc.collect()
            else: 
                del df
                del transformed
                gc.collect()

class PipelineStreaming:
    #Hacer otro engine aquí en caso de que las rows sean demasiadas para procesar en eager o lazy mode
    # Hacer un Profiiling para esto más profesional 
    # Hacer mejor logging para tracing 
    
    def __init__(self, archivo: Path, file_overhead: Dict[str, Any]):
        self.archivo= archivo
        self.file_overhead= file_overhead
    
    def run_streaming_engine(self,  ETL: Callable, model: BaseModel) -> Dict[str, Any]: 
        process = psutil.Process()
        mem_before = process.memory_info().rss
        cpu_before = process.cpu_percent(interval=None)
        tracemalloc.start()
        start_time = time.perf_counter()
        
        if self.archivo.suffix == '.csv': 
            StreamingCSVHandler(archivo=self.archivo, file_overhead=self.file_overhead).run_streaming(ETL=ETL, model=model)
        else: 
            StreamingParquetHanlder(archivo=self.archivo, file_overhead=self.file_overhead).run_streaming(ETL=ETL, model=model)
        
        elapsed_time = time.perf_counter() - start_time
        mem_after = process.memory_info().rss
        mem_used = mem_after - mem_before
        cpu_after = process.cpu_percent(interval=None)
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        io_counters = process.io_counters()
        
        return {
            'tiempo_segundos': elapsed_time,
            'memoria_rss_bytes': mem_used,
            'memoria_rss_mb': mem_used / (1024**2),
            'cpu_percent': cpu_after - cpu_before,
            'tracemalloc_current_mb': current / (1024**2),
            'tracemalloc_peak_mb': peak / (1024**2),
            'io_read_bytes': io_counters.read_bytes,
            'io_write_bytes': io_counters.write_bytes
        }
