from enum import Enum
import polars as pl

class rename_columns_estrategia(str, Enum): 
    LOWER = 'lower'
    UPPER = 'upper'
    CAPITALIZE = 'capitalize'

class dtype_estrategia(tuple, Enum): 
    UTF8 = ('utf8', pl.Utf8)
    INT64 = ('int64', pl.Int64)
    INT32 = ('int32', pl.Int32)
    INT8 = ('int8', pl.Int8)
    FLOAT64 = ('float64', pl.Float64)
    FLOAT32= ('float32', pl.Float32)