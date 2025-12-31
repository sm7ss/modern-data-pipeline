import yaml
import tomli
from pathlib import Path
import logging
from pydantic import BaseModel

from .ConfigValidation import validation_yaml

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class ReadSchemaValidation: 
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
            validacion= validation_yaml(**letcura)
            logger.info(f'Se validó correctamente el archivo {self.archivo.name}')
            return validacion
        except yaml.YAMLError: 
            logger.error(f'El archivo {self.archivo.name} está corrupto')
            raise 
        except Exception as e: 
            logger.error(f'Ocurrio un error al querer leer el arhivo {self.archivo.name}:\n{e}')
    
    def read_toml(self) -> BaseModel: 
        try: 
            with open(self.archivo, 'r') as file: 
                lectura= tomli.load(file)
                logger.info(f'Se leyó correctamente el archivo {self.archivo.name}')
            validacion= validation_yaml(**lectura)
            logger.info(f'Se validó correctamente el schema del toml para el archivo {self.archivo.name}')
            return validacion
        except tomli.TOMLDecodeError: 
            logger.error(f'El archivo {self.archivo.name} está corrupto')
            raise
        except Exception as e: 
            logger.error(f'Ocurrió un error al querer validar el archivo {self.archivo.name}: \n{e}\n')
            raise
    
    def read_file(self) -> BaseModel: 
        archivo_terminacion= self.archivo.suffix
        if archivo_terminacion in ['.yaml', '.yml']: 
            return self.read_yaml()
        else: 
            return self.read_toml()
