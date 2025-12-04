import logging
from typing import Dict, List
import polars as pl 

from ..strategies.strategies import date_estrategia, rename_columns_estrategia, dtype_estrategia
from ..etl.dtype_cleaning import DtypeExprList

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger = logging.getLogger(__name__)

class DictValidation: 
    def __init__(self, data_type: Dict[str, str]):
        self.datatype= data_type
    
    def exist_column(self, schema: pl.Schema) -> None: 
        #Validamos que la columna exista
        for col in self.datatype.keys(): 
            if col not in schema: 
                logger.error(f'La columna {col} no se encuentra en el Schema del Frame\n')
                raise ValueError(f'La columna {col} no se encuentra en el Schema del Frame')
    
    @staticmethod
    def exist_type(self) -> None: 
        type_disponible= [tipo.value for tipo in dtype_estrategia]
        #Tipo de conversion existente
        for tipo in self.data_type.values(): 
            if tipo not in type_disponible: 
                logger.error(f'El tipo de dato {tipo} no se ecuentra disponible en los tipos de estrategias\n')
                raise ValueError(f'El tipo de dato {tipo} no se ecuentra disponible en los tipos de estrategias')
    
    @staticmethod
    def dtype_transformer_validation(self, df: pl.DataFrame) -> None: 
        enteros= ['Int8', 'Int16', 'Int32', 'Int64']
        flotantes= ['Float32', 'Float64']
        
        #Validar que la tranformación sea posible
        for col, tipo in self.data_type.items(): 
            try: 
                if tipo in enteros: 
                    df.with_columns(
                        DtypeExprList.int_expr(col_int=col, dtype_int=tipo)
                    )
                elif tipo in flotantes: 
                    df.with_columns(
                        DtypeExprList.float_expr(col_float=col, dtype_float=tipo)
                    )
                else: 
                    df.with_columns(
                        DtypeExprList.string_expr(col_string=col, dtype_string=tipo)
                    )
            except Exception as e: 
                logger.error(f'La conversion de la columna {col} al tipo de dato {tipo} no es posible:\n{e}\n')
                raise ValueError(f'La conversion de la columna {col} al tipo de dato {tipo} no es posible')

class FormatExistence: 
    @staticmethod
    def date_column_exists(df: pl.DataFrame, schema: pl.Schema, format: str) -> List[str]: 
        lista_columnas_date= []
        
        for col in schema: 
            if (df[col].dtype == pl.Date) or (df[col].dtype == pl.Datetime): 
                match format: 
                    case date_estrategia.AÑO: 
                        lista_columnas_date.append(col)
                    case date_estrategia.MES_EN_NUMERO: 
                        lista_columnas_date.append(col)
                    case date_estrategia.DIA_DEL_MES: 
                        lista_columnas_date.append(col)
                    case date_estrategia.EUROPEO: 
                        lista_columnas_date.append(col)
                    case date_estrategia.AMERICANO:
                        lista_columnas_date.append(col)
                    case date_estrategia.FECHA: 
                        lista_columnas_date.append(col)
            else: 
                continue
        
        return lista_columnas_date

class RenameColumnsValidation: 
    @staticmethod
    def strategy_rename(strategy: str) -> None: 
        match strategy: 
            case rename_columns_estrategia.LOWER: 
                pass
            case rename_columns_estrategia.UPPER: 
                pass
            case rename_columns_estrategia.CAPITALIZE: 
                pass
            case _: 
                logger.error(f'La estrategia {strategy} para renombrar columans no es valiada\n')
                raise ValueError(f'La estrategia {strategy} para renombrar columans no es valiada')
