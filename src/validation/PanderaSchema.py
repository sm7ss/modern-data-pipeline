import pandera.polars as pa
import pickle
import polars as pl
from pydantic import BaseModel
from typing import Dict, Any, Tuple
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class PanderaSchema: 
    def __init__(self, model: BaseModel, archivo: str, file_overhead: Dict[str, Any]):
        self.percent= model.validation_data.sample_size
        self.archivo= Path(archivo)
        self.file_overhead= file_overhead
    
    def _get_schema_lazy_streaming(self, decision: str, file_name: str, filas: int) -> Tuple[pl.DataFrame, pl.Schema]: 
        porcentaje= self.percent*100
        total_rows_processing= filas*self.percent
        
        logger.warning(f'La opcion {decision} es una opcion que no es recomendable cargar completa, por lo que se obtendra un {porcentaje}% del total de filas del archivo. {total_rows_processing}/{filas}')
        
        if self.archivo.suffix=='.csv': 
            frame= pl.read_csv(self.archivo, n_rows=total_rows_processing)
            schema= frame.schema
            logger.info(f'Se obtuvo de manera correcta el frame y schema del archivo {file_name}')
        else: 
            frame= pl.read_parquet(self.archivo, n_rows=total_rows_processing)
            schema= frame.schema
            logger.info(f'Se obtuvo de manera correcta el frame y schema del archivo {file_name}')
        
        return (frame, schema)
    
    def _get_frame_schema(self) -> Tuple[pl.DataFrame, pl.Schema]: 
        decision= self.file_overhead['decision']
        file_name= self.archivo.name
        
        if decision == 'eager': 
            if self.archivo.suffix=='.csv':
                frame= pl.read_csv(self.archivo)
                schema= frame.schema
                logger.info(f'Se obtuvo de manera correcta el frame y schema del archivo {file_name} con la decision {decision}')
            else: 
                frame= pl.read_parquet(self.archivo)
                schema= frame.schema
                logger.info(f'Se obtuvo de manera correcta el frame y schema del archivo {file_name} con la decision {decision}')
            return (frame, schema)
        elif decision == 'lazy': 
            return self._get_schema_lazy_streaming(decision=decision, file_name=file_name)
        else: 
            return self._get_schema_lazy_streaming(decision=decision, file_name=file_name)
    
    def _get_first_schema_validation(self) -> None: 
        schema= self._get_frame_schema()[1]
        pandera_columna={}
        
        for col, tipo in schema.items():
            pandera_columna[col]=pa.Column(
                tipo
            )
        
        schema_validation= pa.DataFrameSchema(
            columns=pandera_columna, 
            strict=True, 
            coerce=True
        )
        
        with open('primer_ingesta_schema.pkl', 'wb') as file: 
            pickle.dump(schema_validation, file)
        logger.info('Se guardo la primera ingesta de datos para el schema exitosamente.')
    
    def validation_schema(self) -> None: 
        archivo_primera_ingesta= Path('primer_ingesta_schema.pkl')
        
        if not archivo_primera_ingesta.exists(): 
            self._get_first_schema_validation()
        else: 
            with open(archivo_primera_ingesta, 'rb') as f: 
                schema_primera_ingesta= pickle.load(f)
            logger.info(f'Se leyó correctamente el archivo {archivo_primera_ingesta.name}')
            
            frame= self._get_frame_schema()[0]
            
            try: 
                schema_primera_ingesta.validate(frame)
                logger.info(f'Validacion exitosa. Los datos siguen el schema original')
            except pa.errors.SchemaError as e: 
                logger.error(f'Los datos de ingesta han cambiado. Error detectado \n{e}')
