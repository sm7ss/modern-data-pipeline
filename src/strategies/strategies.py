from enum import Enum

class date_estrategia(str, Enum):
    AÑO_4_DIGITOS = '%Y'
    MES_CON_NUMEROS = '%m'
    DIA_DEL_MES = '%d'
    EUROPEO = '%d/%m/%Y %H:%M'
    AMERICANO = '%m-%d-%Y %I:%M %p'
    FECHA = '%Y-%m-%d'

class rename_columns_estrategia(str, Enum): 
    LOWER = 'lower'
    UPPER = 'upper'
    CAPITALIZE = 'capitalize'

class dtype_estrategia(str, Enum): 
    UTF8 = 'utf8'
    INT64 = 'int64'
    INT32 = 'int32'
    INT8 = 'int8'
    FLOAT64 = 'float64'
    FLOAT32= 'Float32'