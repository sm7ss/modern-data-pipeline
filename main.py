import time
import tracemalloc
import psutil

from src.etl.EngineDecision import EngineDecision

process = psutil.Process()
mem_before = process.memory_info().rss
cpu_before = process.cpu_percent(interval=None)
tracemalloc.start()
start_time = time.perf_counter()

EngineDecision().orquestador_pipeline() 

elapsed_time = time.perf_counter() - start_time
mem_after = process.memory_info().rss
mem_used = mem_after - mem_before
cpu_after = process.cpu_percent(interval=None)
current, peak = tracemalloc.get_traced_memory()
tracemalloc.stop()
io_counters = process.io_counters()

print('tiempo_segundos', elapsed_time)
print('memoria_rss_bytes', mem_used)
print('memoria_rss_mb', mem_used / (1024**2))
print('cpu_percent', cpu_after - cpu_before)
print('tracemalloc_current_mb', current / (1024**2))
print('tracemalloc_peak_mb', peak / (1024**2))



