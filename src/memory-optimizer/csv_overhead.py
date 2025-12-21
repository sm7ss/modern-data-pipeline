import subprocess
from pathlib import Path
import polars as pl 
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s-%(levelname)s-%(message)s')
logger= logging.getLogger(__name__)

class CsvOverhead: 
    def __init__(self, path: str, n_rows_sample: int=1000):
        self.path= Path(path)
        
        self.frame_sample= pl.read_csv(path, n_rows=n_rows_sample)
        self.str_columns= [col for col in self.frame_sample.columns if self.frame_sample[col].dtype == pl.String]
    
    def string_csv_overhead(self) -> float: 
        if not self.str_columns: 
            return 1.0
        
        avg_len= sum(
            self.frame_sample[col].str.len_bytes().median() for col in self.str_columns
        ) / len(self.str_columns)
        
        if avg_len <= 1: 
            return 2.0 
        elif avg_len <= 5: 
            return 1.8
        elif avg_len <= 10: 
            return 1.6 
        elif avg_len <= 20: 
            return 1.4
        elif avg_len <= 50: 
            return 1.2 
        else: 
            return 1.1
    
    def overhead_csv(self) -> float: 
        col_type_overhead=[]
        csv_overhead= self.string_csv_overhead()
        
        for col in self.frame_sample.columns:
            if self.frame_sample[col].dtype == pl.Int8: 
                col_type_overhead.append(1.4)        
            elif self.frame_sample[col].dtype == pl.Int16: 
                col_type_overhead.append(1.45)
            elif self.frame_sample[col].dtype == pl.Int32: 
                col_type_overhead.append(1.5)
            elif self.frame_sample[col].dtype == pl.Int64: 
                col_type_overhead.append(1.55)
            elif self.frame_sample[col].dtype == pl.Float32: 
                col_type_overhead.append(1.6)
            elif self.frame_sample[col].dtype == pl.Float64: 
                col_type_overhead.append(1.65)
            elif self.frame_sample[col].dtype == pl.Boolean: 
                col_type_overhead.append(2.5)
            elif self.frame_sample[col].dtype == pl.Date: 
                col_type_overhead.append(1.8)
            elif self.frame_sample[col].dtype == pl.Datetime: 
                col_type_overhead.append(1.85)
            elif self.frame_sample[col].dtype == pl.Categorical: 
                col_type_overhead.append(1.6)
            elif self.frame_sample[col].dtype == pl.List: 
                col_type_overhead.append(2.5)
            elif self.frame_sample[col].dtype == pl.Struct: 
                col_type_overhead.append(2.8)
            elif self.frame_sample[col].dtype == pl.String: 
                col_type_overhead.append(csv_overhead)
            else: 
                col_type_overhead.append(1.7)
        
        return sum(col_type_overhead)/len(col_type_overhead)

class CsvOverheadEstimator: 
    def __init__(self, archivo: Path, n_rows_sample: int=1000):
        self.archivo = archivo
        self.frame= pl.read_csv(self.archivo, n_rows=n_rows_sample)
    
    def string_csv_bytes(self) -> float: 
        string_columns= [col for col in self.frame.columns if self.frame[col].dtype == pl.String]
        
        longitudes= []
        
        for col in string_columns: 
            long_col=self.frame[col].str.len_bytes().drop_nulls()
            if len(long_col) > 0: 
                longitudes.append(long_col.median())
        
        return max(sum(longitudes), 32)
    
    '''
        Quitar esto, es muy repetitivo a pesar de ser un calculo anterior para quitar 
        parametros estaticos del bytes, pero quitar más adelante y remplazar por 
        más dinámismo.
    '''
    def estimated_bytes(self) -> int:
        bytes_estimados=[]
        string_bytes=self.string_csv_bytes()
        
        for col in self.frame.columns: 
            if self.frame[col].dtype == pl.Int8: 
                bytes_estimados.append(1)
            elif self.frame[col].dtype == pl.Int16: 
                bytes_estimados.append(2)
            elif self.frame[col].dtype == pl.Int32: 
                bytes_estimados.append(4)
            elif self.frame[col].dtype == pl.Int64: 
                bytes_estimados.append(8)
            elif self.frame[col].dtype == pl.Float32: 
                bytes_estimados.append(4)
            elif self.frame[col].dtype == pl.Float64: 
                bytes_estimados.append(8)
            elif self.frame[col].dtype == pl.Datetime: 
                bytes_estimados.append(8)
            elif self.frame[col].dtype == pl.Date: 
                bytes_estimados.append(4)
            elif self.frame[col].dtype == pl.Boolean: 
                bytes_estimados.append(1)
            elif self.frame[col].dtype == pl.String:
                bytes_estimados.append(string_bytes)  
            else:
                bytes_estimados.append(16)
        
        return (sum(bytes_estimados)/len(self.frame.columns), sum(bytes_estimados))
    
    def csv_bytes_per_column(self) -> float: 
        bytes_per_column= 0
        string_bytes=self.string_csv_bytes()
        avg_string=self.estimated_bytes()
        
        for col in self.frame.columns: 
            #Entero
            if self.frame[col].dtype == pl.Int8: 
                bytes_per_column+=1
            elif self.frame[col].dtype == pl.Int16: 
                bytes_per_column+=2
            elif self.frame[col].dtype == pl.Int32: 
                bytes_per_column+=4
            elif self.frame[col].dtype == pl.Int64: 
                bytes_per_column+=8
            #Flotante 
            elif self.frame[col].dtype == pl.Float32: 
                bytes_per_column+=4
            elif self.frame[col].dtype == pl.Float64: 
                bytes_per_column+=8
            #Datetime 
            elif self.frame[col].dtype == pl.Datetime: 
                bytes_per_column+=8
            #Time
            elif self.frame[col].dtype == pl.Date: 
                bytes_per_column+=4
            elif self.frame[col].dtype == pl.Boolean: 
                bytes_per_column+=1
            #string
            elif self.frame[col].dtype == pl.String: 
                bytes_per_column+=string_bytes
            #Other
            else: 
                bytes_per_column+=avg_string[0]
        
        return (bytes_per_column, avg_string[1])
    
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
