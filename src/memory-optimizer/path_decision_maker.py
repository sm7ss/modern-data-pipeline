import psutil
from typing import Dict, Any
import logging 
from pathlib import Path

from .csv_overhead import CsvOverhead, CsvOverheadEstimator
from .parquet_overhead import ParquetOverheadEstimator

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger = logging.getLogger(__name__)

class FileSizeEstimator: 
    def __init__(self, os_margin: float=0.3, n_rows_sample: int=1000):
        self.n_rows_sample= n_rows_sample
        self.os_margin= os_margin
    
    def estimate_parquet_size(self, class_overhead_parquet) -> Dict[str, Any]: 
        #file overhead and unconmpressed
        uncompressed_data_size= class_overhead_parquet.uncompressed_data_size()
        overhead_estimated= class_overhead_parquet.parquet_algorithm_overhead()
        total_filas= class_overhead_parquet.metadata.num_rows
        
        #resources available and estimated
        estimated_memory= (overhead_estimated*uncompressed_data_size) / (1024**3)
        memoria_disponible=psutil.virtual_memory().available/(1024**3)
        total_memory=psutil.virtual_memory().total/(1024**3)
        
        #margin of safety
        safety_memory= total_memory*self.os_margin
        usable_ram= memoria_disponible-safety_memory
        
        ratio= estimated_memory/usable_ram
        
        return {
            'os_margin': self.os_margin, 
            'ratio': round(ratio, 3),
            'overhead_estimado':round(overhead_estimated, 3), 
            'safety_memory':round(safety_memory, 3),
            'archivo_descomprimido_gb': round(uncompressed_data_size/(1024**3), 3),
            'total_de_filas_gb': total_filas, 
            'memoria_total_estimada_gb':round(estimated_memory, 3), 
            'memoria_disponible':round(memoria_disponible, 3), 
            'total_memory':round(total_memory, 3)
        }
    
    def estimate_csv_size(self, csv_overhead_estimator_class, csv_overhead_class) -> Dict[str, Any]: 
        #bytes and num rows
        num_rows= csv_overhead_estimator_class.total_rows_csv()
        bytes_per_column= csv_overhead_estimator_class.csv_bytes_per_column()
        csv_overhead= csv_overhead_class.overhead_csv()
        
        #resources and estimated resources
        estimated_memory= (num_rows*csv_overhead*bytes_per_column) / (1024**3)
        memoria_disponible= psutil.virtual_memory().available/(1024**3)
        total_memory=psutil.virtual_memory().total/(1024**3)
        
        #margin of safety
        safety_memory= total_memory*self.os_margin
        usable_ram= memoria_disponible-safety_memory
        
        ratio= estimated_memory/usable_ram
        
        return {
            'os_margin': self.os_margin, 
            'ratio': round(ratio, 3),
            'total_rows':num_rows, 
            'bytes_por_columna':bytes_per_column,
            'safety_memory':round(safety_memory, 3),
            'memoria_total_estimada_gb':round(estimated_memory, 3), 
            'memoria_disponible':round(memoria_disponible, 3), 
            'total_memory':round(total_memory, 3)
        }

class PipelineEstimatedSizeFiles: 
    def __init__(self, archivo: str, os_margin: float=0.3, n_rows_sample: int=1000):
        self.archivo= Path(archivo)
        self.n_rows_sample= n_rows_sample
        self.estimator=FileSizeEstimator(os_margin=os_margin, n_rows_sample=n_rows_sample)
    
    def estimated_size_file(self) -> Dict[str, Any]: 
        if self.archivo.suffix == '.csv': 
            overhead_csv_class= CsvOverheadEstimator(archivo=self.archivo, n_rows_sample=self.n_rows_sample)
            overhead_csv= CsvOverhead(path=self.archivo, n_rows_sample=self.n_rows_sample)
            resources_csv= self.estimator.estimate_csv_size(csv_overhead_class=overhead_csv, csv_overhead_estimator_class=overhead_csv_class)
            
            resources_csv['tamaño_archivo_gb']=round(self.archivo.stat().st_size/(1024**4), 3)
            if resources_csv['ratio'] <= 0.65: 
                resources_csv['decision']= 'eager'
            elif resources_csv['ratio'] <= 2.0:
                resources_csv['decision']= 'lazy'
            else: 
                resources_csv['decision']= 'streaming'
            return resources_csv
        else: 
            overhead_parquet= ParquetOverheadEstimator(archivo=self.archivo, n_rows_sample=self.n_rows_sample)
            resources_parquet=self.estimator.estimate_parquet_size(class_overhead_parquet=overhead_parquet)
            
            resources_parquet['tamaño_archivo']=round(self.archivo.stat().st_size/(1024**3), 3)
            if resources_parquet['ratio'] <= 0.65: 
                resources_parquet['decision']= 'eager'
            elif resources_parquet['ratio'] <= 2.0:
                resources_parquet['decision']= 'lazy'
            else: 
                resources_parquet['decision']= 'streaming'
            return resources_parquet