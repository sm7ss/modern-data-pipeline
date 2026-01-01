from pydantic import BaseModel, field_validator, model_validator, Field
import polars as pl 
import logging
from pathlib import Path
from typing import Dict, Optional

from ..strategies.Strategies import rename_columns_estrategia, dtype_estrategia
from ..etl.ETL import DataTypeCleaning

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

class schema_config_validation(BaseModel): 
    column_naming: Optional[rename_columns_estrategia]
    date_format: Optional[bool]
    data_type: Optional[Dict[str, str]]
    decimal_precision: Optional[int]
    
    @field_validator('column_naming')
    def column_naming_validation(cls, v): 
        if not v: 
            return 'lower'
        else: 
            return v
    
    @field_validator('date_format')
    def date_format_validation(cls,v): 
        if not v: 
            return pl.Datetime
        else: 
            return v
    
    @field_validator('data_type')
    def data_type_validation(cls, v): 
        if not v: 
            return v
        
        tipos= [v_tipo.value[0] for v_tipo in dtype_estrategia]
        for valor in v.values(): 
            if valor not in tipos: 
                logger.error(f'El tipo de valor {valor} no está disponible en estrategias\n')
                raise ValueError(f'El tipo de valor {valor} no está disponible en estrategias')
        return v
    
    @field_validator('decimal_precision')
    def decimal_precision_validation(cls, v): 
        if not v: 
            return 2 
        else: 
            return v

class validation_data_validation(BaseModel): 
    sample_size: float = Field(gt=0.0, le=1.0)

class validation_yaml(BaseModel): 
    path: path_validation
    schema_config: schema_config_validation
    validation_data: validation_data_validation
    
    @model_validator(mode='after')
    def column_type_validation(self): 
        archivo= Path(self.path.input_path)
        frame= pl.read_csv(archivo, n_rows=1000) if archivo.suffix=='.csv' else pl.read_parquet(archivo, n_rows=1000)
        schema= frame.schema
        
        #Validar columnas y conversion de tipo de datos para data_type 
        data_type= self.schema_config.data_type
        if data_type: 
            for col in data_type: 
                if col not in schema: 
                    logger.error(f'La columna {col} no se encuentra en el DataFrame del archivo\n')
                    raise ValueError(f'La columna {col} no se encuentra en el DataFrame del archivo\n')
            
            for col, tipo in data_type.items(): 
                try:
                    frame.with_columns(DataTypeCleaning().cast_expr(col=col, dtype=tipo))
                except Exception: 
                    logger.error(f'Ocurrio un error al querer tranformar la columna {col} a el tipo de dato {tipo}\n')
                    raise ValueError(f'currio un error al querer tranformar la columna {col} a el tipo de dato {tipo}')
        return self
