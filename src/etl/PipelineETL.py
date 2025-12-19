import polars as pl 
from typing import Dict, Any, Union
import logging 
import psutil
from pydantic import BaseModel
from pathlib import Path

from ETL import PipelineETL

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class ChunkFileProcessor: 
    def __init__(self, path: Path, file_size: Union[int, float]):
        self.path= path
        
        self.size_file= file_size
        self.available_ram= psutil.virtual_memory().available
        self.num_cpu= psutil.cpu_count(logical=False)
    
    def estimate_max_chunk_radio(self) -> float: 
        ratio= self.size_file / self.available_ram
        
        if ratio > 5 : 
            return 0.15 
        elif ratio > 2: 
            return 0.25
        elif ratio > 0.5: 
            return 0.4
        else: 
            return 0.7
    
    def estimate_min_chunks(self) -> int: 
        return max(5, self.num_cpu*2)
    
    def calculate_optimal_chunk_size(self, safe_margin: float) -> int: 
        ram_for_use= self.available_ram * safe_margin
        max_chunk_ratio= self.estimate_max_chunk_radio()
        min_chunks= self.estimate_min_chunks()
        
        ram_limit= int(ram_for_use * max_chunk_ratio)
        partition_limit= self.size_file // min_chunks
        
        chunk_size= min(ram_limit, partition_limit)
        chunk_size= max(chunk_size, 1024**2)
        chunk_size= min(chunk_size, self.size_file)
        
        return chunk_size

class ETL: 
    def __init__(self, model: BaseModel, file_overhead: Dict[str, Any]):
        self.model= model
        self.archivo = self.model.path.input_path
        self.decision= file_overhead['decision']
    
    def _load_eager_frame(self) -> pl.DataFrame: 
        if self.archivo.suffix == '.csv': 
            return pl.read_csv(self.archivo)
        else: 
            return pl.read_parquet(self.archivo)
    
    def _load_lazy_frame(self) -> pl.LazyFrame: 
        if self.archivo.suffix == '.csv': 
            return pl.scan_csv(self.archivo)
        else: 
            return pl.scan_parquet(self.archivo)
    
    def _run_streaming_handler(self) -> None:
        pass #Aqui la clase para manejar streming
    
    def orquestador_pipeline(self) -> None: 
        if self.decision == 'eager': 
            frame= self._load_eager_frame()
            etl_frame= PipelineETL(Frame=frame, model=self.model)
        elif self.decision == 'lazy': 
            frame= self._load_lazy_frame() 
            etl_frame= PipelineETL(Frame=frame, model=self.model)
        else: 
            self._run_streaming_handler() #run la clase de streming

