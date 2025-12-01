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
    def __init__(self, archivo: Path):
        self.path = archivo
        self.parquet_overhead = ParquetOverheadEstimator(archivo=self.path)
        self.csv_overhead= CsvOverheadEstimator(archivo=self.path)
    
    def estimate_parquet_size(self,  
        default_factor: float=1.7, 
        sample_n_rows: int=1000) -> int: 
        uncompressed_data_size= self.parquet_overhead.uncompressed_data_size()
        overhead_estimated= self.parquet_overhead.parquet_algorithm_overhead(default_factor=default_factor, sample_n_rows=sample_n_rows)
        
        estimated_memory= (overhead_estimated*uncompressed_data_size) / (1024**3)
        
        return {
            'memoria_base_gb':uncompressed_data_size/(1024**3), 
            'overhead_estimado':overhead_estimated, 
            'memoria_total_estimada_gb':estimated_memory, 
            'memoria_disponible':psutil.virtual_memory().available/(1024**3)
        }
    
    def estimate_csv_size(self, 
        csv_overhead: float=1.8,
        default_factor: int=16, 
        sample_n_rows: int=1000): 
        num_rows= self.csv_overhead.total_rows_csv()
        overhead_csv= csv_overhead
        bytes_per_column= self.csv_overhead.csv_bytes_per_column(default_factor=default_factor, sample_n_rows=sample_n_rows)
        
        estimated_memory= (num_rows*overhead_csv*bytes_per_column) / (1024**3)
        
        return {
            'memoria_base_gb':num_rows/(1024**3), 
            'overhead_estimado':overhead_csv, 
            'memoria_total_estimada_gb':estimated_memory, 
            'memoria_disponible':psutil.virtual_memory().available/(1024**3)
        }

# margin_os: float=0.2
class PipelineEstimatedSizeFiles: 
    def __init__(self, archivo: str):
        self.archivo= Path(archivo)
        self.estimator=FileSizeEstimator(archivo=self.archivo)
    
    def estimated_size_with_safety_margin(self, os_margin: float=0.2) -> Dict[str, Any]: 
        if self.archivo.suffix == '.csv': 
            pass
        elif self.archivo.suffix == '.parquet': 
            pass
        else: 
            raise ValueError(f'El archivo {self.archivo.name} debe ser un csv o parquet')