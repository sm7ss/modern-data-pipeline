from pydantic import BaseModel, Field, model_validator, field_validator
from pathlib import Path
import logging
from typing import Union, Dict
import polars as pl 

from ..strategies.strategies import date_estrategia, rename_columns_estrategia, dtype_estrategia
from ..etl.dtype_cleaning import DtypeExprList
from .yaml_classes_type_validation import DictValidation, FormatExistence, RenameColumnsValidation

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger = logging.getLogger(__name__)

class path_validation(BaseModel): 
    input_path: str
    
    @field_validator('input_path')
    def validate_path(cls, v): 
        path = Path(v)
        nombre = path.name
        terminacion = path.suffix
        
        if not path.exists(): 
            logger.info(f'El archivo {nombre} no existe')
            raise FileNotFoundError(f'El archivo {nombre} no existe')
        
        if terminacion not in ['.csv', '.parquet']:
            logger.info((f'El archivo {nombre} no debe tener una terminacion .csv o .parquet'))
            raise ValueError(f'El archivo {nombre} no debe tener una terminacion .csv o .parquet')
        return path

class cleaning_steps_validation(BaseModel): 
    data_type: Union[Dict[str, str], None]
    date_format: date_estrategia
    rename_columns: Union[str, None]

class validation_data_validation(BaseModel): 
    sample_size: float = Field(..., gt=0, le=1, )

class etl_validation(BaseModel): 
    cleaning_steps: cleaning_steps_validation
    validation_data: validation_data_validation

class validation_yaml(BaseModel): 
    path: path_validation
    etl: etl_validation
    
    @model_validator(mode='after')
    def etl_column_type_validation(self): 
        '''
            - Valida diccionario si existe 
            - Accede a la clase DataValidation para validar: 
                - Columnas existentes 
                - Tipos de datos disponibles 
                - Valida las conversiones antes de pasar al ETL 
        '''
        path= self.path.input_path
        df= pl.read_csv(path, n_rows=1000) if path.endswith('.csv') else pl.read_parquet(path, n_rows=1000)
        schema= df.schema
        
        dict_validation= DictValidation(data_type=self.etl.cleaning_steps.data_type)
        
        if self.etl.cleaning_steps.data_type: 
            dict_validation.exist_column(schema=schema)
            dict_validation.exist_type()
            dict_validation.dtype_transformer_validation(df=df)
        return self
    
    @model_validator(mode='after')
    def date_format_validation(self): 
        '''
            - Validamos formato en caso de existir 
            - Si hay un formato pero no hay columnas de fecha actuamos: 
                - No para el pipeline, pero aparecera en el logging
        '''
        path= self.path.input_path
        df= pl.read_csv(path, n_rows=1000) if path.endswith('.csv') else pl.read_parquet(path, n_rows=1000)
        schema= df.schema
        
        if self.etl.cleaning_steps.date_format: 
            lista_columnas_fecha= FormatExistence.date_column_exists(df=df, schema=schema, format=self.etl.cleaning_steps.date_format)
            if not lista_columnas_fecha: 
                logger.warning(f'No se encontraron columnas númericas, por lo que no habrá conversiones de tipo de fecha para ninguna columna')
        return self
    
    @model_validator(mode='after')
    def rename_columns_validation(self): 
        '''
            - Validamos la columna existente en caso de ser diccionario
            - Validamos la estrategía en caso de ser string
        '''
        path= self.path.input_path
        schema= pl.read_csv(path, n_rows=1000) if path.endswith('.csv') else pl.read_parquet(path, n_rows=1000).schema
        
        rename_column= self.etl.cleaning_steps.rename_columns
        
        if isinstance(rename_column, dict): 
            DictValidation(data_type=rename_column).exist_column(schema=schema)
        else: 
            RenameColumnsValidation.strategy_rename(strategy=rename_column)
        return self
