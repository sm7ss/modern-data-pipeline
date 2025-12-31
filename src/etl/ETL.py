import polars as pl 
import logging 
from typing import Union, Dict
from pydantic import BaseModel
#from prefect import task, flow

from ..strategies.Strategies import dtype_estrategia, rename_columns_estrategia

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(levelname)s')
logger = logging.getLogger(__name__)

class DataTypeCleaning: 
    @staticmethod
    def create_cast_expr(col: str, dtype: str) -> pl.Expr: 
        for tipo in dtype_estrategia: 
            if tipo[0] == dtype.lower(): 
                return pl.col(col).cast(tipo[1])
    
    @staticmethod
    def cast_datetime_date() -> pl.Expr: 
        return pl.selectors.by_dtype(pl.Date).cast(pl.Datetime)
    
    @staticmethod
    def cast_datetime_string(col:str) -> pl.Expr: 
        return pl.col(col).str.to_datetime()

class RenameColumnsCleaning:
    def __init__(self, frame: Union[pl.LazyFrame, pl.DataFrame]):
        self.frame= frame
    
    def estrategia(self, estrategia: rename_columns_estrategia) -> Dict: 
        if isinstance(self.frame, pl.DataFrame): 
            frame= self.frame.columns
        else: 
            frame= self.frame.collect_schema().names()
        
        match estrategia: 
            case rename_columns_estrategia.CAPITALIZE: 
                return {col: col.capitalize() for col in frame}
            case rename_columns_estrategia.LOWER: 
                return {col: col.lower() for col in frame}
            case rename_columns_estrategia.UPPER: 
                return {col: col.upper() for col in frame}
    
    def rename_columns_frame(self, diccionario: Dict[str, str]) -> Union[pl.LazyFrame, pl.DataFrame]: 
        return self.frame.rename(diccionario)

class PipelineETL: 
    def __init__(self, Frame: Union[pl.DataFrame, pl.LazyFrame], model: BaseModel):
        self.frame= Frame
        self.model= model
        
        self.dtype_transformer= DataTypeCleaning()
        self.rc= RenameColumnsCleaning(frame=self.frame)
        
        self.column_renaming= self.model.schema_config.column_naming
        self.date_format= self.model.schema_config.date_format #bool
        self.data_type= self.model.schema_config.data_type
    
    #@task
    def rename_columns_cleaner(self) -> Union[pl.LazyFrame, pl.DataFrame]: 
        diccionario= self.rc.estrategia(estrategia=self.column_renaming)
        return self.rc.rename_columns_frame(diccionario=diccionario)
    
    #@task
    def dtype_cleaning(self, frame: Union[pl.LazyFrame, pl.DataFrame]) -> Union[pl.LazyFrame, pl.DataFrame]: 
        expresiones_cast= []
        
        for col, tipo in self.data_type.items(): 
            expresiones_cast.append(self.dtype_transformer.cast_expr(col=col, dtype=tipo))
        
        return frame.with_columns(expresiones_cast)
    
    #@task
    def format_date_cleaning(self, frame: Union[pl.LazyFrame, pl.DataFrame]) -> Union[pl.LazyFrame, pl.DataFrame]: 
        formato_expr= []
        
        sample= frame.limit(10)
        if isinstance(sample, pl.DataFrame): 
            schema= sample.schema
        else: 
            schema= sample.collect_schema()
        
        for col, tipo in schema.items(): 
            try: 
                if tipo == pl.String: 
                    expr_str= self.dtype_transformer.cast_datetime_string(col=col)
                    sample.with_columns(expr_str)
                    formato_expr.append(expr_str)
            except: 
                continue
        
        formato_expr.append(self.dtype_transformer.cast_datetime_date())
        return frame.with_columns(formato_expr)
    
    #@flow(name='Pipeline ETL - rename and dtype transformation')
    def etl(self) -> pl.DataFrame:
        if self.column_renaming: 
            frame= self.rename_columns_cleaner()
        if self.data_type:
            frame= self.dtype_cleaning(frame=frame)
        if self.date_format: 
            frame= self.format_date_cleaning(frame=frame)
        return frame
