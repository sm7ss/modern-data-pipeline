import polars as pl 
from typing import Dict, Any, Optional
import logging 
from pydantic import BaseModel

from .ETL import PipelineETL
from .Streaming import PipelineStreaming

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class EngineDecision: 
    def __init__(self, model: BaseModel, file_overhead: Dict[str, Any]):
        self.model= model
        self.archivo = self.model.path.input_path
        self.file_overhead= file_overhead
    
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
    
    def _run_streaming_handler(self) -> Dict[str, Any]:
        pipeline_etl= PipelineETL
        
        streaming= PipelineStreaming(archivo=self.archivo, file_overhead=self.file_overhead)
        diccionario= streaming.run_streaming_engine(ETL=pipeline_etl, model=self.model)
        return diccionario
    
    def orquestador_pipeline(self) -> Optional[Dict[str, Any]]: 
        decision= self.file_overhead['decision']
        
        if decision == 'eager': 
            frame= self._load_eager_frame()
            PipelineETL(Frame=frame, model=self.model)
        elif decision == 'lazy': 
            frame= self._load_lazy_frame() 
            PipelineETL(Frame=frame, model=self.model)
        else: 
            return self._run_streaming_handler() 

