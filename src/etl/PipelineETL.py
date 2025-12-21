import polars as pl 
from typing import Dict, Any
import logging 
from pydantic import BaseModel

from .ETL import PipelineETL

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class ETL: 
    def __init__(self, model: BaseModel, file_overhead: Dict[str, Any]):
        self.model= model
        self.archivo = self.model.path.input_path
        
        self.decision= file_overhead['decision']
        self.file_size= file_overhead['tamaño_archivo']
        self.os_margin= file_overhead['os_margin']
        
        #self.chunk_size= ChunkFileProcessor(path=self.archivo, file_size=self.file_size)
    
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
        # Dividimos la logica de csv y parquet 
        pass
    
    def orquestador_pipeline(self) -> None: 
        if self.decision == 'eager': 
            frame= self._load_eager_frame()
            etl_frame= PipelineETL(Frame=frame, model=self.model)
        elif self.decision == 'lazy': 
            frame= self._load_lazy_frame() 
            etl_frame= PipelineETL(Frame=frame, model=self.model)
        else: 
            self._run_streaming_handler() # Aun no soportado 
            logger.warning('Metodo aun no soportado para streaming')
            raise 

