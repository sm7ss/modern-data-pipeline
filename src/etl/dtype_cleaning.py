import polars as pl 
import logging 
from ..strategies.strategies import dtype_estrategia

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(levelname)s')
logger = logging.getLogger(__name__)

class DtypeExprList: 
    @staticmethod
    def int_expr(col_int: str, dtype_int: str) -> pl.Expr: 
        match dtype_int: 
            case dtype_estrategia.INT8:
                return pl.col(col_int).cast(pl.Int8)
            case dtype_estrategia.INT32: 
                return pl.col(col_int).cast(pl.Int32)
            case dtype_estrategia.INT64: 
                return pl.col(col_int).cast(pl.Int64)
    
    @staticmethod
    def float_expr(col_float: str, dtype_float: str) -> pl.Expr: 
        match dtype_float: 
            case dtype_estrategia.FLOAT32: 
                return pl.col(col_float).cast(pl.Float32)
            case dtype_estrategia.FLOAT64: 
                return pl.col(col_float).cast(pl.Float64)
    
    @staticmethod
    def string_expr(col_string: str, dtype_string: str) -> pl.expr: 
        match dtype_string: 
            case dtype_estrategia.UTF8: 
                return pl.col(col_string).cast(pl.Utf8)
