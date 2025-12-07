import polars as pl 
import logging 

from ..strategies.strategies import dtype_estrategia

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(levelname)s')
logger = logging.getLogger(__name__)

def create_cast_expr(col: str, dtype: str) -> pl.Expr: 
    for tipo in dtype_estrategia: 
        if tipo[0] == dtype.lower(): 
            return pl.col(col).cast(tipo[1])
