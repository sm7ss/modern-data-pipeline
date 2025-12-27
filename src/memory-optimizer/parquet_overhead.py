from pathlib import Path
import polars as pl 
from typing import Dict, Any
import pyarrow.parquet as pp
import pyarrow as pa

import logging 
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger = logging.getLogger(__name__)

class ParquetOverheadEstimator: 
    def __init__(self, archivo: Path, n_rows_sample: int=1000):
        self.path = archivo
        self.n_rows_sample= n_rows_sample
        
        self.parquet_file= pp.ParquetFile(self.path)
        self.metadata = self.parquet_file.metadata
        self.schema= self.metadata.schema.to_arrow_schema()
    
    def string_overhead(self) -> float: 
        sample_median=pl.read_parquet(self.path, n_rows=self.n_rows_sample)
        string_columns= [col for col in sample_median.columns if sample_median[col].dtype == pl.String]
        
        if not string_columns: 
            return 1.0
        
        avg_string_len=sum([
            sample_median[col].str.len_bytes().sum() for col in string_columns
        ]) / len(string_columns)
        
        avg_string_len= max(1.0, avg_string_len)
        
        base= 1.0+4.0 / avg_string_len
        if avg_string_len < 5: 
            return min(base+0.4, 2.2)
        if avg_string_len < 10: 
            return base + 0.2
        elif avg_string_len < 20: 
            return base + 0.1
        else: 
            return base + 0.05
    
    def overhead_parquet(self) -> float: 
        promedio_overhead= []
        
        for valor in self.schema: 
            dtype= valor.type
            if pa.types.is_int8(dtype):
                promedio_overhead.append(1.2)
            elif pa.types.is_int16(dtype): 
                promedio_overhead.append(1.25)
            elif pa.types.is_int32(dtype): 
                promedio_overhead.append(1.3)
            elif pa.types.is_int64(dtype): 
                promedio_overhead.append(1.35)
            elif pa.types.is_float32(dtype): 
                promedio_overhead.append(1.4)
            elif pa.types.is_float64(dtype): 
                promedio_overhead.append(1.45)
            elif pa.types.is_boolean(dtype): 
                promedio_overhead.append(2.0)
            elif pa.types.is_date(dtype): 
                promedio_overhead.append(1.5)
            elif pa.types.is_timestamp(dtype): 
                promedio_overhead.append(1.55)
            elif pa.types.is_dictionary(dtype): 
                promedio_overhead.append(1.1)
            elif pa.types.is_list(dtype): 
                promedio_overhead.append(2.2)
            elif pa.types.is_struct(dtype): 
                promedio_overhead.append(2.5)
            elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype): 
                promedio_overhead.append(self.string_overhead())
            else: 
                promedio_overhead.append(1.7)
        
        return round(sum(promedio_overhead)/len(promedio_overhead), 3)
    
    def parquet_algorithm_overhead(self) -> Dict[str, Any]: 
        factores= []
        overhead_default= self.overhead_parquet()
        string_parquet_overhead=self.string_overhead()
        
        for col in self.schema: 
            tipo= col.type
            #Entero 
            if pa.types.is_integer(tipo): 
                factores.append(1.3)
            #Flotante 
            elif pa.types.is_floating(tipo): 
                factores.append(1.4)
            #String 
            elif pa.types.is_string(tipo) or pa.types.is_large_string(tipo): 
                factores.append(string_parquet_overhead)
            #Boleano 
            elif pa.types.is_boolean(tipo): 
                factores.append(2.0)
            #Timestamp o fecha
            elif pa.types.is_timestamp(tipo) or pa.types.is_date(tipo):
                factores.append(1.5)
            #Lista de types
            elif pa.types.is_list(tipo): 
                factores.append(2.2)
            #Otro 
            else: 
                factores.append(overhead_default)
        
        return sum(factores) / len(factores)
    
    def uncompressed_data_size(self) -> int: 
        uncompressed_data_size = sum([
            self.metadata.row_group(rg).column(col).total_uncompressed_size
            for rg in range(self.metadata.num_row_groups)
            for col in range(self.metadata.num_columns)
        ])
        
        return uncompressed_data_size