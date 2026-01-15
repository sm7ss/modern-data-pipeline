# 🏭 Modern Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Polars](https://img.shields.io/badge/Polars-Fast__DataFrames-red.svg)](https://pola.rs/)
[![Pydantic](https://img.shields.io/badge/Pydantic-Data__Validation-yellow.svg)](https://docs.pydantic.dev/)
[![Docker](https://img.shields.io/badge/Docker-Containerization-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

Un sistema de data pipeline end-to-end que procesa inteligentemente desde 1 millón hasta 244 millones de filas con recursos limitados, tomando decisiones automáticas basadas en memoria disponible, validando schemas en tiempo real, e ingiriendo datos a bases de datos de producción. 🚀

## 🎯 El Desafío: Big Data en Hardware Limitado

Problema: ¿Cómo procesas 244 millones de filas con solo 15GB de RAM total y solo de 8 a 9 GB de RAM disponible, mantienes consistencia de schemas, soportas múltiples formatos de archivo, y todo ejecutándose 100% local?

Solución: Este pipeline inteligente que:

- Auto-decide estrategias de carga (eager/lazy/streaming)
- Optimiza memoria en tiempo real basado en hardware
- Valida schemas para prevenir data drift
- Procesa eficientemente 1M a 244M filas
- Reduce tiempo en 30% y memoria en 45%

## 🏆 Logros Clave (Métricas Reales)

### **Dataset de 244M filas (15GB RAM disponible):**

| Estrategia	       | Tiempo Total |	Memoria Peak | CPU Usage |	Mejora                           |
|----------------------|--------------|--------------|-----------|-----------------------------------|
| **Sin optimizar**	   | 15:00 min	  | 951 MB       | 40%	     | Baseline                          |
| **Batch 2M filas**   | 12:46 min	  | 160 MB	     | 12%	     | 15% más rápido                    |
| **Paralelizado**	   | 12:20 min	  | 951 MB	     | 15%	     | 17% más rápido                    |
| **Optimizado Final** | 11:28 min	  | 521 MB	     | 14%	     | 30% más rápido, 45% menos memoria |

### **Resultados por Tamaño de Dataset:**

- 1M filas (CSV): 3.68s → 2.09s (43% más rápido)
- 1M filas (Parquet): 19.22s → 12.80s (33% más rápido)
- 244M filas (Parquet): 15:00min → 11:28min (30% más rápido)

## ✨ Características Principales

### 🧠 **Inteligencia de Carga Automática**

- Memory-aware decision engine: Analiza RAM disponible vs datos necesarios
- Tres estrategias automáticas: eager (RAM), lazy (LazyFrame), streaming (chunks)
- Overhead calculation: Preciso para CSV y Parquet basado en tipos de datos reales
- Hardware adaptation: Se ajusta dinámicamente a tus recursos

### 🛡️ **Validación y Consistencia**

- Schema validation con Pandera: Garantiza consistencia entre ejecuciones
- Pydantic para configs: Valida YAML/TOML con tipos estrictos
- Data type enforcement: Casting seguro con manejo de errores
- Persistent schema tracking: Detecta drifts automáticamente

### ⚡ **Performance Optimizado**

- PyArrow zero-copy: Reduce overhead de memoria
- Batch size inteligente: Ajusta dinámicamente basado en presión de memoria
- Streaming engine: Para datasets que no caben en RAM
- Efficient Postgres ingestion: COPY command con batches optimizados

### 🔄 **Pipeline End-to-End**

- Configuración dual: YAML o TOML
- ETL personalizable: Renaming, type casting, date parsing
- Database integration: PostgreSQL + DuckDB para queries rápidas
- Docker-ready: Infraestructura reproducible

## 🏗️ Arquitectura del Sistema

### **Flujo de Datos:**

1. 📄 Config YAML/TOML → Pydantic validation
2. 🔍 File analysis → Memory estimation
3. 🧠 Decision engine → eager/lazy/streaming
4. ⚙️ ETL processing → Type casting, renaming
5. 🛡️ Schema validation → Pandera check
6. 🗄️ Database ingestion → PostgreSQL
7. 📊 Query layer → DuckDB for analysis

## 🚀 Instalación Rápida

```bash 
# Clonar el repositorio
git clone https://github.com/sm7ss/production-data-pipeline.git
cd production-data-pipeline

# Instalar dependencias
pip install -r requirements.txt

# Iniciar PostgreSQL con Docker
docker-compose up -d
```

### **Dependencias Principales:**

```text 
polars>=0.19.0
pydantic>=2.0.0
pandera>=0.18.0
pyarrow>=14.0.0
psutil>=5.9.0
duckdb>=0.9.0
psycopg2-binary>=2.9.0
docker>=6.0.0
```

## 💻 Uso en 3 Pasos

### **1. Configuración (config.yml):**

```yaml 
path:
  input_path: 'datos.parquet'

schema_config:
  column_naming: 'lower'
  date_format: true
  data_type:
    age: 'int64'
    salary: 'float64'

validation_data:
  sample_size: 0.01

os_configuration:
  os_margin: 0.3
  n_rows_sample: 1000

database:
  table_name: 'employees'
  if_table_exists: 'replace'
```

### **2. Ejecución:**

```python 
from src.etl.EngineDecision import EngineDecision

# Pipeline automático
pipeline = EngineDecision()
results = pipeline.orquestador_pipeline()

print(f"✅ Procesado: {results['total_rows']} filas")
print(f"⏱️  Tiempo: {results['execution_time']:.2f}s")
print(f"💾 Memoria usada: {results['memory_used_mb']:.2f} MB")
```

### **3. Análisis con DuckDB:**

```python 
from src.database.DuckDBQueries import DuckDBPostgresConnector

# Query rápida sobre PostgreSQL
df = DuckDBPostgresConnector.query("""
  SELECT department, AVG(salary) as avg_salary
  FROM pg_main.employees
  GROUP BY department
  ORDER BY avg_salary DESC
""")
```

## 🔧 Configuración Avanzada

### **Optimización de Memoria:**

```yaml 
os_configuration:
  os_margin: 0.3        # 30% margen de seguridad
  n_rows_sample: 5000   # Muestreo para estimación
```

### **ETL Personalizado:**

```yaml 
schema_config:
  column_naming: 'upper'  # lower, upper, capitalize
  date_format: true       # Auto-detecta strings de fecha
  data_type: {'user_id': 'int64', 'created_at': 'datetime', 'price': 'float64'} # Casting manual
  decimal_precision: 3    # Precisión decimal
```

### **Estrategias de Database:**

```yaml 
database:
  table_name: 'analytics_table'
  if_table_exists: 'append'  # fail, append, replace
```

## 📊 Casos de Uso Empresariales

### **1. ETL Diario de Grandes Volúmenes**

```python 
# Procesamiento nocturno de 100M+ filas
pipeline = EngineDecision()
pipeline.orquestador_pipeline()  # Auto-optimiza para recursos nocturnos
```

### **2. Data Quality Monitoring**

```python 
# Validación automática de schema changes
from src.validation.PanderaSchema import PanderaSchema

validator = PanderaSchema(config="config.yml", data="new_data.parquet")
try:
    validator.validation_schema()  # ✅ Schema consistente
except:
    alert_team("Schema changed!")  # 🚨 Data drift detectado
```

### **3. Resource-Constrained Environments**

- Ejecución en servidores con RAM limitada
- El sistema auto-ajusta a 4GB, 8GB, 16GB de RAM
- Cambia automáticamente entre eager/lazy/streaming

### **4. Development vs Production**

```yaml 
# Development (muestra pequeña)
validation_data:
  sample_size: 0.01  # 1% para desarrollo rápido

# Production (validación completa)  
validation_data:
  sample_size: 0.10  # 10% para producción segura
```

## 🎯 Decision Engine: Cómo Funciona

### **Algoritmo de Decisión:**

```python 
def decide_strategy(file_size, available_ram, os_margin=0.3):
    safety_ram = available_ram * (1 - os_margin)
    estimated_needed = estimate_memory_need(file_size)
    
    ratio = estimated_needed / safety_ram
    
    if ratio <= 0.65:      # 65% de RAM disponible
        return "eager"     # ✅ Carga completa
    elif ratio <= 2.0:     # Hasta 200% 
        return "lazy"      # ⚡ LazyFrame
    else:                  # Más del 200%
        return "streaming" # 🚀 Procesamiento por chunks
```

### **Estimación Precisa:**

- CSV: Basado en tipos de datos y longitud de strings
- Parquet: Usa metadata real de PyArrow (tamaño descomprimido)
- Considera: Integers, floats, strings, dates, nested types

## 🔬 Métricas de Performance Detalladas

### **Hardware de Prueba:**

- RAM: 15.5GB total, 8-9GB disponible típicamente
- CPU: 12 cores físicos
- Storage: SSD 250GB + HDD 1TB
- 100% local: Sin cloud

### **Dataset 1: CSV (1M filas, 5 columnas)**

**Baseline**:    3.68s, 347MB, 97.5% CPU
**Optimized**:   2.09s, 346MB, 80.2% CPU
**Improvement**: 43% más rápido, misma memoria

### **Dataset 2: Parquet (1M filas, 26 columnas)**

**Baseline**:    19.22s, 1302MB, 55.4% CPU  
**Optimized**:   12.80s, 1306MB, 47.7% CPU
**Improvement**: 33% más rápido, CPU más eficiente

### **Dataset 3: Parquet (244M filas, 5 columnas)**

**Baseline**:    15:00min, 951MB, 40% CPU
**Optimized**:   11:28min, 521MB, 14% CPU
**Improvement**: 30% más rápido, 45% menos memoria

## 🛠️ Tech Stack Detallado

### **Procesamiento Principal:**

- Polars: DataFrames ultrarrápidos en Rust
- PyArrow: Acceso a metadatos sin copia, serialización eficiente
- Pydantic: Validación de configuración con seguridad de tipos

### **Validación y Calidad:**

- Pandera: Validación de esquemas, cumplimiento de contratos de datos
- Validadores personalizados: Existencia de archivos, compatibilidad de tipos, reglas de nomenclatura

### **Infraestructura y Monitoreo:**

- Docker: Entornos reproducibles, contenedor de PostgreSQL
- psutil: Monitoreo de memoria, detección de hardware
- tracemalloc: Seguimiento detallado de memoria
- Métricas personalizadas: Tiempo de ejecución, uso de memoria, conteo de filas

### **Capa de Base de Datos:**

- PostgreSQL: Almacenamiento de datos de nivel productivo
- DuckDB: Consultas analíticas, agregaciones rápidas
- psycopg2: Inserciones por lotes eficientes con comando COPY

### **Configuración y Flexibilidad:**

- YAML/TOML: Soporte para formato de configuración dual
- Estrategias Enum: Opciones de configuración con seguridad de tipos
- Variables de entorno: Gestión segura de credenciales

## 🤝 Contribuciones y Feedback

Si tienes sugerencias para:

- Mejoras en la arquitectura
- Optimizaciones de performance
- Mejores prácticas de ingeniería de datos
- Ideas para nuevas features

¡Tu feedback es super bienvenido! 💫

### **Guía de contribución:**

1. Fork el proyecto
2. Crea feature branch (git checkout -b feature/nueva-db)
3. Commit cambios (git commit -m 'feat: add ClickHouse support')
4. Push a la rama (git push origin feature/nueva-db)
5. Abre Pull Request
