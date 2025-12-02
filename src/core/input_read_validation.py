import yaml
from pathlib import Path
import logging

from ..validation.input_yaml_validation import input_path_config_validation

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class ReadYamlFile: 
    def __init__(self, archivo: str):
        self.archivo= Path(archivo)
        
        if not self.archivo.exists(): 
            raise FileNotFoundError(f'El archivo {self.archivo.name} no existe')
        if self.archivo.suffix not in ['.yml', '.yaml']: 
            raise ValueError(f'El archivo {self.archivo.name} debe ser un archivo yaml')
    
    def read_yaml(self): 
        try: 
            with open(self.archivo, 'r') as file: 
                letcura= yaml.safe_load(file)
                logger.info(f'Se leyo correctamente el archivo {self.archivo.name}')
            validacion= input_path_config_validation(**letcura)
            logger.info(f'Se validó correctamente el archivo {self.archivo.name}')
            return validacion
        except yaml.YAMLError: 
            logger.error(f'El archivo {self.archivo.name} está corrupto')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio un error al querer leer el arhivo {self.archivo.name}:\n{e}')
