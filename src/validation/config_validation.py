from pydantic import BaseModel, Field, model_validator, field_validator
from pathlib import Path
import logging
from typing import Union, Dict, Optional
import polars as pl 

from ..strategies.strategies import date_estrategia, rename_columns_estrategia, dtype_estrategia
from ..etl.dtype_cleaning import create_cast_expr

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
    date_format: Optional[date_estrategia]
    rename_columns: Union[Dict[str, str], rename_columns_estrategia, None]
    
    @field_validator('data_type')
    def tipo_validacion(cls, v): 
        tipos= [v_tipo.value[0] for v_tipo in dtype_estrategia]
        for valor in v.values(): 
            if valor not in tipos: 
                logger.error(f'El tipo de valor {valor} no está disponible en estrategias\n')
                raise ValueError(f'El tipo de valor {valor} no está disponible en estrategias')
        return v

class validation_data_validation(BaseModel): 
    sample_size: float = Field(..., gt=0, le=1, )

class etl_validation(BaseModel): 
    cleaning_steps: cleaning_steps_validation
    validation_data: validation_data_validation

class validation_yaml(BaseModel): 
    path: path_validation
    etl: etl_validation
    
    @model_validator(mode='after')
    def column_type_validation(self): 
        archivo= Path(self.path.input_path)
        frame= pl.read_csv(archivo, n_rows=1000) if archivo.suffix=='.csv' else pl.read_parquet(archivo, n_rows=1000)
        schema= frame.schema
        
        #Validar columnas y conversion de tipo de datos para data_type 
        data_type= self.etl.cleaning_steps.data_type
        if data_type:
            for col in data_type: 
                if col not in schema: 
                    logger.error(f'La columna {col} no se encuentra en el DataFrame del archivo\n')
                    raise ValueError(f'La columna {col} no se encuentra en el DataFrame del archivo\n')
            
            for col, tipo in data_type.items(): 
                try:
                    frame.with_columns(create_cast_expr(col=col, dtype=tipo))
                except Exception: 
                    logger.error(f'Ocurrio un error al querer tranformar la columna {col} a el tipo de dato {tipo}\n')
                    raise ValueError(f'currio un error al querer tranformar la columna {col} a el tipo de dato {tipo}')
        
        #Validar columnas para rename_columns
        rename_columns= self.etl.cleaning_steps.rename_columns
        if rename_columns:
            if isinstance(rename_columns, dict): 
                for col in rename_columns: 
                    if col not in schema: 
                        logger.error(f'La columna {col} no se encuentra en el DataFrame del archivo\n')
                        raise ValueError(f'La columna {col} no se encuentra en el DataFrame del archivo\n')
        return self
