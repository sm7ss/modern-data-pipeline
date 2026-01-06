from pydantic import BaseModel, field_validator, model_validator, Field
import polars as pl 
import logging
from pathlib import Path
from typing import Dict, Optional, Literal

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

class os_configuration_validation(BaseModel): 
    os_margin: float= Field(ge=0.1, le=0.5)
    n_rows_sample: int
    
    @field_validator('n_rows_sample')
    def n_rows_rample_validation(cls, v): 
        if v > 1000: 
            logger.warning(f'Se modifico el valor de "n_rows_sample" de {v} a 1000 puesto que el limite maximo para "n_rows_sample" son 1000 filas')
            v= 1000
        elif v < 100: 
            logger.warning(f'Se modifico el valor de "n_rows_sample" de {v} a 100 puesto que el limite minimo para "n_rows_sample" son 100 filas')
            v=100
        else: 
            return v
        return v

class database_validation(BaseModel): 
    table_name: str
    if_table_exists: Optional[Literal['append', 'replace', 'fail']]
    
    @field_validator('if_table_exists')
    def if_table_exists_validation(cls, v): 
        if not v: 
            v= 'fail'
            logger.warning('Como no se puso un valor pre-definido para si la tabla existe, se asigno "fail" y en caso de existir la tabla se dará un error. El costo de la operación esta en que se ejecutará el pipeline y fallara al tratar de meterse los datos procesados a la base de datos.')
        return v
    
    @field_validator('table_name')
    def table_name_validation(cls, v): 
        if len(v)==0: 
            logger.warning(f'El nuevo_nombre de la tabla es el nombre del archivo; puesto que no se puso ningun nombre en la configuracion')
            return 'new_table'
        
        len_v= len(v)
        v= v.strip()
        len_v_later= len(v)
        if not(len_v == len_v_later): 
            logger.warning('Se quitaron los espacios de inicio y fin de la palabra')
        
        if '-' in v: 
            v= v.replace('-', '_')
            logger.warning('Se remplazaron los caracteres - por _')
        
        if ' ' in v: 
            v= v.replace(' ', '')
            logger.warning('Se quitaron los espacios en blanco entre las palabras')
        
        for l in v: 
            try:
                if int(l): 
                    logger.warning(f'"{v}" no puede tener numeros en incio de la palabra')
                    nuevo_nombre= v.split(l)[1]
                    if len(nuevo_nombre)==0: 
                        logger.warning(f'El nuevo_nombre de la tabla es el nombre del archivo; puesto que el nombre de la tabla eran unicamente numeros')
                        return 'new_table'
                    v= nuevo_nombre
            except Exception: 
                logger.info(f'Se obtuvo exitosamente el nuevo nombre de la tabla.\nEl nuevo nombre de la tabla es: "{v}"')
                return v

class validation_data_validation(BaseModel): 
    sample_size: float = Field(gt=0.0, le=1.0)

class validation_yaml(BaseModel): 
    path: path_validation
    schema_config: schema_config_validation
    validation_data: validation_data_validation
    os_configuration: os_configuration_validation
    database: database_validation
    
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
    
    @model_validator(mode='after')
    def table_name(self): 
        archivo= self.path.input_path.stem
        
        if self.database.table_name=='new_table': 
            self.database.table_name= archivo
        return self

