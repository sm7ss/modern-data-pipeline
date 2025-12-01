from pydantic import BaseModel, Field, model_validator, field_validator
from pathlib import Path
import logging
from typing import Union

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
        tamaño = path.stat().st_size
        
        if not path.exists(): 
            logger.info(f'El archivo {nombre} no existe')
            raise FileNotFoundError(f'El archivo {nombre} no existe')
        
        if terminacion not in ['.csv', '.parquet']:
            logger.info((f'El archivo {nombre} no debe tener una terminacion .csv o .parquet'))
            raise ValueError(f'El archivo {nombre} no debe tener una terminacion .csv o .parquet')
        
        if tamaño 
        
        return path

class cleaning_steps_validation(BaseModel): 
    data_type: Union[dict, str, date_estrategia, None]
    rename_columns: Union[dict, str, dtype_estrategia, rename_columns_estrategia, None]

class validation_data_validation(BaseModel): 
    sample_size: float = Field(..., gt=0, le=1, )

class etl_validation(BaseModel): 
    cleaning_steps: cleaning_steps_validation
    validation_data: validation_data_validation
    
    @model_validator(mode='after')
    def etl_validator_params(self): 
        path = self.cleaning_steps.input_path
        tamaño = path.stat().st_size
        terminacion = path.suffix
        
        df_lazy = 
