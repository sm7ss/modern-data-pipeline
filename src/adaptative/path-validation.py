import psutil
import polars as pl  
import pyarrow.parquet as pp
import pyarrow as pa
from typing import Dict, Any
import subprocess

import logging 
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger = logging.getLogger(__name__)

class ParquetOverheadEstimator: 
    def __init__(self, archivo: Path):
        self.path = archivo
        self.metadata = pp.ParquetFile(self.path).metadata
        self.schema= self.metadata.schema.to_arrow_schema()
    
    def parquet_string_overhead_estimator(self, sample_n_rows: int=1000) -> float: 
        sample_median=pl.read_parquet(self.path, n_rows=sample_n_rows)
        
        schema_string= [col for col in sample_median.columns if sample_median[col].dtype == pl.String]
        
        avg_string_len=sum([
            sample_median[col].str.len_bytes().median() for col in schema_string
        ]) / len(schema_string)
        
        if avg_string_len < 10: 
            return 1.6
        elif avg_string_len < 50: 
            return 1.8
        elif avg_string_len < 100: 
            return 2.0
        else: 
            return 2.3
    
    def parquet_algorithm_overhead(self, 
        default_factor: float=1.7, 
        sample_n_rows: int=1000) -> Dict[str, Any]: 
        factores= []
        
        for col in self.schema: 
            tipo= col.type
            #Entero 
            if pa.types.is_integer(tipo): 
                factores.append(1.3)
            #Flotante 
            if pa.types.is_floating(tipo): 
                factores.append(1.4)
            #String 
            if pa.types.is_string(tipo) or pa.types.is_large_string(tipo): 
                factores.append(self.string_overhead_estimator(sample_n_rows=sample_n_rows))
            #Boleano 
            if pa.types.is_boolean(tipo): 
                factores.append(2.0)
            #Timestamp o fecha
            if pa.types.is_timestamp(tipo) or pa.types.is_date(tipo):
                factores.append(1.5)
            #Lista de types
            if pa.types.is_list(tipo): 
                factores.append(2.2)
            #Otro 
            else: 
                factores.append(default_factor)
        
        return sum(factores) / len(factores)
    
    def uncompressed_data_size(self) -> int: 
        uncompressed_data_size = sum([
            self.metadata.row_group(rg).column(col).total_uncompressed_size
            for rg in range(self.metadata.num_row_groups)
            for col in range(self.metadata.num_columns)
        ])
        
        return uncompressed_data_size

class CsvOverheadEstimator: 
    def __init__(self, archivo: Path):
        self.archivo = archivo
    
    def csv_string_bytes_estimator(self, schema: pl.schema, frame: pl.DataFrame) -> float: 
        string_columns= [col for col in schema if frame[col].dtype == pl.String]
        
        longitudes= []
        
        for col in string_columns: 
            long_col=frame[col].str.len_bytes().drop_nulls()
            if len(long_col) > 0: 
                longitudes.append(long_col.median())
        
        return max(sum(longitudes), 32)
    
    def csv_bytes_per_column(self,
        default_factor: int=16, 
        sample_n_rows: int=1000) -> float: 
        frame= pl.read_csv(self.archivo, n_rows=sample_n_rows)
        schema= frame.schema
        
        bytes_per_column= 0
        
        for col in schema.values(): 
            #Entero
            if col == pl.Int8: 
                bytes_per_column+=1
            elif col == pl.Int16: 
                bytes_per_column+=2
            elif col == pl.Int32: 
                bytes_per_column+=4
            elif col == pl.Int64: 
                bytes_per_column+=8
            #Flotante 
            elif col == pl.Float32: 
                bytes_per_column+=4
            elif col == pl.Float64: 
                bytes_per_column+=8
            #Datetime 
            elif col == pl.Datetime: 
                bytes_per_column+=8
            #Time
            elif col == pl.Date: 
                bytes_per_column+=4
            elif col == pl.Boolean: 
                bytes_per_column+=1
            #string
            elif col == pl.String: 
                bytes_per_column+=self.csv_string_bytes_estimator(schema=schema, frame=frame)
            #Other
            else: 
                bytes_per_column+=default_factor
        
        return bytes_per_column
    
    def total_rows_csv(self) -> int: 
        try: 
            result=subprocess.run(
                ['wc', '-l', self.archivo], 
                capture_output=True, text=True
            )
            return int(result.stdout.split()[0] )-1
        except: 
            with open(self.archivo, 'r', encoding='utf-8') as file: 
                return sum(1 for _ in file) -1

class FileSizeEstimator: 
    @staticmethod
    def estimate_parquet_size( 
        parquet_overhead: pp.ParquetFile,
        default_factor: float=1.7, 
        sample_n_rows: int=1000, 
        os_margin: float=0.2) -> int: 
        #file overhead and unconmpressed
        uncompressed_data_size= parquet_overhead.uncompressed_data_size()
        overhead_estimated= parquet_overhead.parquet_algorithm_overhead(default_factor=default_factor, sample_n_rows=sample_n_rows)
        
        #resources available and estimated
        estimated_memory= (overhead_estimated*uncompressed_data_size) / (1024**3)
        memoria_disponible=psutil.virtual_memory().available/(1024**3)
        total_memory=psutil.virtual_memory().total/(1024**3)
        
        #margin of safety
        safety_memory= total_memory*os_margin
        usable_ram= memoria_disponible-safety_memory
        
        ratio= estimated_memory/usable_ram
        
        return {
            'ratio': round(ratio, 3),
            'memoria_base_gb': round(uncompressed_data_size/(1024**3), 3), 
            'overhead_estimado':round(overhead_estimated, 3), 
            'safety_memory':round(safety_memory, 3),
            'memoria_total_estimada_gb':round(estimated_memory, 3), 
            'memoria_disponible':round(memoria_disponible, 3), 
            'total_memory':round(total_memory, 3)
        }
    
    @staticmethod
    def estimate_csv_size( 
        csv_overhead_class,
        csv_overhead: float=1.8,
        default_factor: int=16, 
        sample_n_rows: int=1000, 
        os_margin: float=0.2): 
        #bytes and num rows
        num_rows= csv_overhead_class.total_rows_csv()
        bytes_per_column= csv_overhead_class.csv_bytes_per_column(default_factor=default_factor, sample_n_rows=sample_n_rows)
        
        #resources and estimated resources
        estimated_memory= (num_rows*csv_overhead*bytes_per_column) / (1024**3)
        memoria_disponible= psutil.virtual_memory().available/(1024**3)
        total_memory=psutil.virtual_memory().total/(1024**3)
        
        #margin of safety
        safety_memory= total_memory*os_margin
        usable_ram= memoria_disponible-safety_memory
        
        ratio= estimated_memory/usable_ram
        
        return {
            'ratio': round(ratio, 3),
            'total_rows':num_rows, 
            'bytes_por_columna':bytes_per_column,
            'safety_memory':round(safety_memory, 3),
            'memoria_total_estimada_gb':round(estimated_memory, 3), 
            'memoria_disponible':round(memoria_disponible, 3), 
            'total_memory':round(total_memory, 3)
        }

'''
    Pasar los parametros a un yaml para tener menos parametros
    En el pipeline no se pusieron los parametros de la clase FileSizeEstimator 
'''
class PipelineEstimatedSizeFiles: 
    def __init__(self, archivo: str):
        self.archivo= Path(archivo)
        self.estimator=FileSizeEstimator()
    
    def estimated_size_file(self) -> Dict[str, Any]: 
        if self.archivo.suffix == '.csv': 
            overhead_csv= CsvOverheadEstimator(archivo=self.archivo)
            resources_csv= self.estimator.estimate_csv_size(csv_overhead_class=overhead_csv)
            
            if resources_csv['ratio'] <= 0.65: 
                resources_csv['decision']= 'eager'
            elif resources_csv['ratio'] <= 2.0:
                resources_csv['decision']= 'lazy'
            else: 
                resources_csv['decision']= 'streaming'
            return resources_csv
        else: 
            overhead_parquet= ParquetOverheadEstimator(archivo=self.archivo)
            resources_parquet=self.estimator.estimate_parquet_size(parquet_overhead=overhead_parquet)
            
            if resources_parquet['ratio'] <= 0.65: 
                resources_parquet['decision']= 'eager'
            elif resources_parquet['ratio'] <= 2.0:
                resources_parquet['decision']= 'lazy'
            else: 
                resources_parquet['decision']= 'streaming'
            return resources_parquet