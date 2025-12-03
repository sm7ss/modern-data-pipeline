from pydantic import BaseModel, Field, model_validator, field_validator
from pathlib import Path
import logging
from typing import Union
import polars as pl 

from strategies import date_estrategia, rename_columns_estrategia, dtype_estrategia

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
    data_type: Union[dict, str, None]
    date_format: date_estrategia
    rename_columns: Union[dict, str, None]

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
        path= self.path.input_path
        schema= pl.read_csv(path, n_rows=1000) if path.endswith('.csv') else pl.read_parquet(path, n_rows=1000).schema
        
        type_disponible= [tipo.value for tipo in dtype_estrategia]
        date_type_disponible= [tipo.value for tipo in date_estrategia]
        
        for col, tipo in schema.items(): 
            if col not in schema: 
                logger.error(f'La columna {col} no se encuentra en el Schema del Frame\n')
                raise ValueError(f'La columna {col} no se encuentra en el Schema del Frame')
            elif tipo not in type_disponible:
                if tipo in date_type_disponible:
                    continue
                logger.error(f'El tipo de dato {tipo} no se ecuentra disponible en los tipos de datos\n')
                raise ValueError((f'El tipo de dato {tipo} no se ecuentra disponible en los tipos de datos'))
