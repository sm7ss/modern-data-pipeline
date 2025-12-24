import logging 
import polars as pl 
import pyarrow as pa
import pyarrow.parquet as pp
from typing import Dict, Any, Union
from pathlib import Path
from pydantic import BaseModel

from .ETL import PipelineETL

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class StreamingCSVHandler: 
    def __init__(self, path: Path, file_overhead: Dict[str, Any]):
        self.archivo= path
        self.file_overhead= file_overhead
    
    def estimate_ratio_batch_size(self) -> float: 
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
        total_rows= self.file_overhead['total_rows']
        
        batch_rows= int(total_rows*batch_float)
        
        batch_rows= max(batch_rows, 10000)
        batch_rows= min(batch_rows, total_rows)
        return batch_rows
    
    def run_streaming(self, ETL: PipelineETL, model: BaseModel) -> None: 
        #Hacer otro engine aquí en caso de que las rows sean demasiadas para procesar en eager o lazy mode
        row_size= self.csv_batch_size_row()
        
        first_chunk= pl.read_csv(self.archivo, n_rows=row_size)
        etl= ETL(Frame= first_chunk, model=model)
        frame= etl.etl()
        
        logger.info(f'Columnas {frame.height} procesadas exitosamente')
        
        #Experimental
        if frame.height==row_size: 
            return frame
        
        schema= frame.schema
        frame.write_csv('pandera_report.csv')
        
        skip_chunk=row_size
        while True: 
            try: 
                next_chunk= pl.read_csv(
                    self.archivo, 
                    skip_rows=skip_chunk, 
                    n_rows=row_size, 
                    schema_overrides=schema
                )
                
                if next_chunk.height==0: 
                    logger.warning('Archivo sin más filas a procesar')
                    return next_chunk
                
                frame= etl.etl()
                
                logger.info(f'Columnas {frame.height} procesadas exitosamente.\n{skip_chunk} saltadas')
                skip_chunk+=row_size
            except Exception as e: 
                logger.warning(f'Fin del archivo u ocurrio un error: \n{e}')
                break

class StreamingParquetHanlder: 
    def __init__(self, archivo: Path, file_overhead: Dict[str, Any]):
        self.archivo= archivo
        self.file_overhead= file_overhead['parquet_metadata']
    
    def calculate_row_size(self) -> int: 
        pass
    
    def bytes_row_group(self, target_size: Union[int, float]) -> int: 
        rows=0
        bytes_used= 0
        
        for rg in range(self.row_group.num_row_groups): 
            rg_meta= self.row_group.row_group(rg)
            rg_size=sum(
                rg_meta.column(col).total_uncompressed_size
                for col in range(self.row_group.num_columns)
            )
            return rg_size

class StreamingCleaningData: 
    #orquestador 
    pass
