import logging 
import psutil
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s-%(asctime)s-%(message)s')
logger= logging.getLogger(__name__)

class CSVOptimalBatchSizeRows: 
    def __init__(self, path: Path, sample_bytes: float, total_rows: int, safe_margin: float):
        self.archivo= path
        self.sample_bytes= sample_bytes
        self.safe_margin= safe_margin
        self.total_rows= total_rows
        self.size_file= path.stat().st_size
        
        self.available_ram= psutil.virtual_memory().available
        self.num_cpu= psutil.cpu_count(logical=False)
    
    def estimate_max_chunk_radio(self) -> float: 
        ratio= self.size_file / self.available_ram
        
        if ratio > 5: 
            return 0.15 
        elif ratio > 2: 
            return 0.25
        elif ratio > 0.5: 
            return 0.4
        else: 
            return 0.7
    
    def estimate_min_chunks(self) -> int: 
        return max(5, self.num_cpu*2)
    
    def calculate_optimal_chunk_size(self) -> int: 
        ram_for_use= self.available_ram * self.safe_margin
        max_chunk_ratio= self.estimate_max_chunk_radio()
        min_chunks= self.estimate_min_chunks()
        
        ram_limit= int(ram_for_use * max_chunk_ratio)
        partition_limit= self.size_file // min_chunks
        
        chunk_size= min(ram_limit, partition_limit)
        chunk_size= max(chunk_size, 1024**2)
        chunk_size= min(chunk_size, self.size_file)
        
        return chunk_size
    
    def estimate_bytes_per_row(self) -> float: 
        avg_bytes_per_row= self.sample_bytes / self.total_rows
        
        return avg_bytes_per_row
    
    def batch_size_rows(self) -> int: 
        chunk_size= self.calculate_optimal_chunk_size()
        bytes_per_row= self.estimate_bytes_per_row()
        
        batch_size_rows= max(10000, int(chunk_size/bytes_per_row))
        batch_size_rows= min(batch_size_rows, 1000000)
        batch_size_rows=max(batch_size_rows, 10000)
        return batch_size_rows
