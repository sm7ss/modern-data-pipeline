import polars as pl 
from typing import Dict, Any, Optional
import logging 
from pathlib import Path

from .ETL import PipelineETL
from .Streaming import PipelineStreaming
from ..validation.ReadYamlValidation import ReadSchemaValidation
from ..memory_optimizer.PathDecisionMaker import PipelineEstimatedSizeFiles
from ..validation.PanderaSchema import PanderaSchema

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class EngineDecision: 
    def __init__(self, archivo: str, os_margin: float=0.3, n_rows_sample: int=1000):
        self.os_margin= os_margin
        self.n_rows_sample= n_rows_sample
        
        self.model= ReadSchemaValidation(archivo=archivo).read_file()
        self.archivo = self.model.path.input_path
        self.file_overhead= self.file_overhead(archivo=self.archivo)[0]
    
    def file_overhead(self, archivo: str) -> Dict[str, Any]: 
        archivo= Path(archivo)
        diccionario= PipelineEstimatedSizeFiles(archivo=archivo, os_margin=self.os_margin, n_rows_sample=self.n_rows_sample).estimated_size_file()
        logger.info(f'Se obtuvo el file_overhead para el archivo {self.archivo.name}')
        return diccionario, archivo
    
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
            logger.info(f'Se obtuvo el frame exitosamente con la decision {decision}')
            
            frame= PipelineETL(Frame=frame, model=self.model).etl()
            frame.write_parquet('pandera_report.parquet')
            
            logger.info(f'Se tranformo el frame exitosamente para el archivo {self.archivo.name}')
            diccionario, archivo= self.file_overhead(archivo='pandera_report.parquet')
            PanderaSchema(model=self.model, archivo=archivo, file_overhead=diccionario).validation_schema()
        elif decision == 'lazy': 
            frame= self._load_lazy_frame() 
            logger.info(f'Se obtuvo el frame exitosamente con la decision {decision}')
            
            porcentaje= self.model.validation_data.sample_size
            total_filas_slice= int(self.file_overhead_model['total_de_filas']*porcentaje)
            
            frame= PipelineETL(Frame=frame, model=self.model).etl()
            frame.slice(0, total_filas_slice).collect(engine='streaming').write_parquet('pandera_report.parquet')
            
            logger.info(f'Se tranformo el frame exitosamente para el archivo {self.archivo.name}')
            diccionario, archivo = self.file_overhead(archivo='pandera_report.parquet')
            PanderaSchema(model=self.model, archivo=archivo, file_overhead=diccionario).validation_schema()    
        else: 
            diccionario= self._run_streaming_handler() 
            return diccionario

