import psutil
from typing import Dict, Any
import logging 
from pathlib import Path

from .CsvOverhead import CsvOverhead, CsvOverheadEstimator
from .ParquetOverhead import ParquetOverheadEstimator

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
        estimated_memory= (overhead_estimated*uncompressed_data_size) 
        memoria_disponible=psutil.virtual_memory().available
        total_memory=psutil.virtual_memory().total
        
        #margin of safety
        safety_memory= total_memory*self.os_margin
        
        ratio= estimated_memory/memoria_disponible
        
        return {
            'os_margin': self.os_margin, 
            'ratio': ratio,
            'overhead_estimado':overhead_estimated, 
            'safety_memory':safety_memory,
            'archivo_descomprimido': uncompressed_data_size, 
            'total_de_filas': total_filas, 
            'memoria_total_estimada':estimated_memory, 
            'memoria_disponible':memoria_disponible, 
            'total_memory':total_memory
        }
    
    def estimate_csv_size(self, csv_overhead_estimator_class, csv_overhead_class) -> Dict[str, Any]: 
        #bytes and num rows
        num_rows= csv_overhead_estimator_class.total_rows_csv()
        bytes_per_column= csv_overhead_estimator_class.csv_bytes_per_column()
        csv_overhead= csv_overhead_class.overhead_csv()
        
        #resources and estimated resources
        estimated_memory= (num_rows*csv_overhead*bytes_per_column) 
        memoria_disponible= psutil.virtual_memory().available
        total_memory=psutil.virtual_memory().total
        
        #margin of safety
        safety_memory= total_memory*self.os_margin
        
        ratio= estimated_memory/memoria_disponible
        
        return {
            'os_margin': self.os_margin, 
            'csv_overhead': csv_overhead, 
            'ratio': ratio, 
            'total_de_filas':num_rows,
            'safety_memory':safety_memory, 
            'memoria_total_estimada':estimated_memory, 
            'memoria_disponible':memoria_disponible, 
            'total_memory':total_memory
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
            
            resources_csv['tamaño_archivo']=self.archivo.stat().st_size
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
            
            resources_parquet['parquet_file_pyarrow']= overhead_parquet.parquet_file
            resources_parquet['tamaño_archivo']=self.archivo.stat().st_size
            if resources_parquet['ratio'] <= 0.65: 
                resources_parquet['decision']= 'eager'
            elif resources_parquet['ratio'] <= 2.0:
                resources_parquet['decision']= 'lazy'
            else: 
                resources_parquet['decision']= 'streaming'
            return resources_parquet
