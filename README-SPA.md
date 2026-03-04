# 🏭 Modern Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Polars](https://img.shields.io/badge/Polars-Fast__DataFrames-red.svg)](https://pola.rs/)
[![Pydantic](https://img.shields.io/badge/Pydantic-Data__Validation-yellow.svg)](https://docs.pydantic.dev/)
[![Docker](https://img.shields.io/badge/Docker-Containerization-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

Un sistema de datos end-to-end que procesa de forma inteligente entre 1 millón y 244 millones de filas con recursos limitados, tomando decisiones automáticas basadas en la memoria disponible, validando esquemas en tiempo real e ingestando datos en bases de datos de producción. 🚀

## 🎯 El reto: Big Data en hardware limitado

**Problema:** ¿Cómo se procesan 244 millones de filas con solo 15GB de RAM total y solo 8-9GB de RAM disponible, se mantiene la coherencia del esquema, se admiten múltiples formatos de archivo y todo se ejecuta al 100% de forma local?

**Solución:** Esta canalización inteligente que:
- Decide automáticamente las estrategias de carga (eager/lazy/streaming).
- Optimiza la memoria en tiempo real en función del hardware.
- Valida los esquemas para evitar la deriva de datos.
- Procesa de manera eficiente entre 1 y 244 millones de filas.
- Reduce el tiempo en un 30% y la memoria en un 45%.

## 🏆 Logros clave (métricas reales)

### **Conjunto de datos de 244 millones de filas (15 GB de RAM disponibles):**

| Estrategia           | Tiempo total |    Memoria máxima | Uso de la CPU |    Mejora                           |
|--------------------- -|--------------|--------------|-----------|-----------------------------------|
| **Sin optimizar**       | 15:00 min	  | 951MB       | 40%         | Referencia                          |
| **Lote de 2 millones de filas**   | 12:46 min      | 160MB         | 12%         | 15% más rápido                  |
| **Paralelizado**       | 12:20 min      | 951MB	     | 15%         | 17% más rápido                    |
| **Optimizado final** | 11:28 min      | 521MB         | 14%         | 30% más rápido, 45% menos de memoria |

### **Resultados por tamaño del conjunto de datos:**

- 1 millón de filas (CSV): 3,68 s → 2,09 s (43 % más rápido)
- 1 millón de filas (Parquet): 19,22 s → 12,80 s (33 % más rápido)
- 244 millones de filas (Parquet): 15:00 min → 11:28 min (30 % más rápido)

## ✨ Características principales

### 🧠 **Inteligencia de carga automática**

- Motor de decisión consciente de la memoria: analiza la RAM disponible frente a los datos necesarios
- Tres estrategias automáticas: eager (RAM), lazy (LazyFrame) y streaming (chunks)
- Cálculo de la sobrecarga: preciso para CSV y Parquet basado en tipos de datos reales
- Adaptación al hardware: se ajusta dinámicamente a tus recursos

### 🛡️ **Validación y coherencia**

- Validación de esquemas con Pandera: garantiza la coherencia entre ejecuciones.
- Pydantic para configuraciones: valida YAML/TOML con tipos estrictos.
- Aplicación de tipos de datos: conversión segura con gestión de errores.
- Seguimiento persistente de esquemas: detecta automáticamente las desviaciones.

### ⚡ **Rendimiento optimizado**

- PyArrow sin copia: reduce la sobrecarga de memoria.
- Tamaño de lote inteligente: se ajusta dinámicamente en función de la presión de la memoria.
- Motor de streaming: para conjuntos de datos que no caben en la RAM.
- Ingestión eficiente de Postgres: comando COPY con lotes optimizados.

### 🔄 **Pipeline de extremo a extremo**

- Configuración dual: YAML o TOML
- ETL personalizable: renombrado, conversión de tipos, análisis de fechas
- Integración de bases de datos: PostgreSQL + DuckDB para consultas rápidas
- Listo para Docker: infraestructura reproducible

## 🏗️ Arquitectura del sistema

### **Flujo de datos:**

1. 📄 Configuración YAML/TOML → Validación Pydantic
2. 🔍 Análisis de archivos → Estimación de memoria
3. 🧠 Motor de decisión → eager/lazy/streaming
4. ⚙️ Procesamiento ETL → Conversión de tipos, renombramiento
5. 🛡️ Validación de esquemas → Comprobación Pandera
6. 🗄️ Ingestión de bases de datos → PostgreSQL
7. 📊 Capa de consultas → DuckDB para análisis

## 🚀 Instalación rápida

```bash 
# Clonar el repositorio
git clone https://github.com/sm7ss/production-data-pipeline.git
cd production-data-pipeline

# Instalar dependencias
pip install -r requirements.txt

# Iniciar PostgreSQL con Docker
docker-compose up -d
```

### **Dependencias principales:**

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

## 💻 Uso en 3 pasos

### **1. Configuración (config.yml):**

```yaml 
path:
  input_path: "datos.parquet"

schema_config:
  column_naming: "lower"
  date_format: true
  data_type: {"age": "int64", "salary": "float64"}

validation_data:
  sample_size: 0.01

os_configuration:
  os_margin: 0.3
  n_rows_sample: 1000

database:
  table_name: "employees"
  if_table_exists: "replace"
```

### **2. Ejecución:**

```python 
from src.etl.EngineDecision import EngineDecision

# Canalización automática
pipeline = EngineDecision()
results = pipeline.orquestador_pipeline()

print(f"✅ Procesadas: {results["total_rows"]} filas")
print(f"⏱️  Tiempo: {results["execution_time"]:.2f}s")
print(f"💾 Memoria utilizada: {results["memory_used_mb"]:.2f} MB")
```

### **3. Análisis con DuckDB:**

```python 
from src.database.DuckDBQueries import DuckDBPostgresConnector

# Consulta rápida sobre PostgreSQL
df = DuckDBPostgresConnector.query("""
  SELECT department, AVG(salary) as avg_salary
  FROM pg_main.employees
  GROUP BY department
  ORDER BY avg_salary DESC
  """)
```

## 🔧 Configuración avanzada

### **Optimización de la memoria:**

```yaml 
os_configuration:
  os_margin: 0.3        # Margen de seguridad del 30 %
  n_rows_sample: 5000   # Muestreo para estimación
```

### **ETL personalizado:**

```yaml 
schema_config:
  column_naming: "upper"  # minúsculas, mayúsculas, mayúscula inicial
  date_format: true       # Detecta automáticamente las cadenas de fecha
  data_type: {"user_id": "int64", "created_at": "datetime", "price": "float64"} # Conversión manual
  decimal_precision: 3    # Precisión decimal
```

### **Estrategias de base de datos:**

```yaml 
database:
  table_name: "analytics_table"
  if_table_exists: "append"  # fallar, añadir, reemplazar
```

## 📊 Casos de uso empresarial

### **1. ETL diario de gran volumen**

```python 
# Procesamiento nocturno de más de 100 millones de filas.
pipeline = EngineDecision()
pipeline.orquestador_pipeline()  # Se optimiza automáticamente para los recursos nocturnos.
```

### **2. Supervisión de la calidad de los datos**

```python 
# Validación automática de cambios en el esquema
from src.validation.PanderaSchema import PanderaSchema

validator = PanderaSchema(config"config.yml", data="new_data.parquet")
try:
    validator.validation_schema()  # ✅ Esquema coherente
except:
    alert_team("¡Esquema cambiado!")  # 🚨 Se ha detectado una desviación de datos
```

### **3. Entornos con recursos limitados**

- Ejecución en servidores con RAM limitada
- El sistema se ajusta automáticamente a 4 GB, 8 GB o 16 GB de RAM
- Cambia automáticamente entre eager/lazy/streaming

### **4. Desarrollo frente a producción**

```yaml 
# Desarrollo (muestra pequeña)
validation_data:
  sample_size: 0.01  # 1% para un desarrollo rápido

# Producción (validación completa)  
validation_data:
  sample_size: 0.10  # 10% para una producción segura
```

## 🎯 Motor de decisión: cómo funciona

### **Algoritmo de decisión:**

```python 
def decide_strategy(file_size, available_ram, os_margin=0.3):
    safety_ram = available_ram * (1 - os_margin)
    estimated_needed = estimate_memory_need(file_size)
    
    ratio = estimated_needed / safety_ram
    
    if ratio <= 0.65:      # 65 % de la RAM disponible
        return "eager"     # ✅ Carga completa
    elif ratio <= 2.0:     # Hasta el 200 % 
        return "lazy"      # ⚡ LazyFrame
    else:                  # Más del 200 %
        return "streaming" # 🚀 Procesamiento por fragmentos
```

### **Estimación precisa:**

- CSV: basado en tipos de datos y longitudes de cadena
- Parquet: utiliza metadatos PyArrow reales (tamaño sin comprimir)
- Considera: enteros, flotantes, cadenas, fechas, tipos anidados

## 🔬 Métricas detalladas de rendimiento

### **Hardware de prueba:**

- RAM: 15,5GB en total, 8-9GB disponibles normalmente
- CPU: 12 núcleos físicos
- Almacenamiento: SSD de 250GB + HDD de 1 TB
- 100% local: sin nube

### **Conjunto de datos 1: CSV (1 millón de filas, 5 columnas)**

**Referencia**: 3,68s, 347MB, 97,5% de CPU
**Optimizado**: 2,09s, 346MB, 80,2% de CPU
**Mejora**: 43% más rápido, misma memoria

### **Conjunto de datos 2: Parquet (1 millón de filas, 26 columnas)**

**Referencia**:    19,22s, 1302MB, 55,4% de CPU  
**Optimizado**:   12,80s, 1306MB, 47,7% de CPU
**Mejora**: un 33% más rápido, CPU más eficiente

### **Conjunto de datos 3: Parquet (244 millones de filas, 5 columnas)**

**Referencia**:    15:00 min, 951MB, 40% de CPU
**Optimizado**:   11:28 min, 521MB, 14% de CPU
**Mejora**: un 30% más rápido, un 45% menos de memoria

## 🛠️ Pila tecnológica detallada

### **Procesamiento principal:**

- Polars: DataFrames ultrarrápidos en Rust
- PyArrow: Acceso a metadatos sin copia, serialización eficiente
- Pydantic: Validación de la configuración con seguridad de tipos

### **Validación y calidad:**

- Pandera: Validación de esquemas, cumplimiento de contratos de datos
- Validadores personalizados: existencia de archivos, compatibilidad de tipos, reglas de nomenclatura

### **Infraestructura y supervisión:**

- Docker: entornos reproducibles, contenedor PostgreSQL
- psutil: supervisión de la memoria, detección de hardware
- tracemalloc: seguimiento detallado de la memoria
- Métricas personalizadas: tiempo de ejecución, uso de la memoria, recuento de filas

### **Capa de base de datos:**

- PostgreSQL: almacenamiento de datos a nivel de producción
- DuckDB: consultas analíticas, agregaciones rápidas
- psycopg2: inserciones por lotes eficientes con el comando COPY

### **Configuración y flexibilidad:**

- YAML/TOML: compatibilidad con dos formatos de configuración
- Estrategias Enum: opciones de configuración seguras en cuanto a tipos
- Variables de entorno: gestión segura de credenciales

## 🤝 Contribuciones y comentarios

Si tienes sugerencias sobre:

- Mejoras en la arquitectura.
- Optimizaciones del rendimiento.
- Mejores prácticas de ingeniería de datos.
- Ideas para nuevas funciones.

¡Tus comentarios son bienvenidos! 💫

### **Guía de contribución:**

1. Bifurca el proyecto.
2. Crea una rama de características (git checkout -b feature/new-db).
3. Confirma los cambios (git commit -m “feat: add ClickHouse support”).
4. Envía a la rama (git push origin feature/new-db).
5. Abre una solicitud de extracción.

