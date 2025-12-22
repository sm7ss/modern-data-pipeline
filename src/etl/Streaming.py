import logging 
import polars as pl 
import pyarrow as pa
import pyarrow.parquet as pp
import psutil
from typing import Dict, Any
from pathlib import Path

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
        
        batch_rows= max(batch_rows)
        batch_rows= min(batch_rows, total_rows)
        return batch_rows
    
    def run_streaming(self) -> None: 
        pass

class StreamingParquetHanlder: 
    def __init__(self, archivo: Path, file_overhead: Dict[str, Any]):
        self.archivo= archivo
        self.file_overhead= file_overhead
    
    def read_parquet_streaming(self) -> float: 
        pass
