# 🏭 Modern Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Polars](https://img.shields.io/badge/Polars-Fast__DataFrames-red.svg)](https://pola.rs/)
[![Pydantic](https://img.shields.io/badge/Pydantic-Data__Validation-yellow.svg)](https://docs.pydantic.dev/)
[![Docker](https://img.shields.io/badge/Docker-Containerization-blue.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

An end-to-end data pipeline system that intelligently processes from 1 million to 244 million rows with limited resources, making automatic decisions based on available memory, validating schemas in real-time, and ingesting data into production databases. 🚀
 
## 🎯 The Challenge: Big Data on Limited Hardware

**Problem:** How do you process 244 million rows with only 15GB total RAM and only 8-9 GB of available RAM, maintain schema consistency, support multiple file formats, all running 100% locally?

**Solution:** This intelligent pipeline that:
- Auto-decides loading strategies (eager/lazy/streaming)
- Optimizes memory in real-time based on hardware
- Validates schemas to prevent data drift
- Efficiently processes 1M to 244M rows
- Reduces time by 30% and memory by 45%

## 🏆 Key Achievements (Real Metrics)

### **244M Row Dataset (15GB RAM available):**

| Strategy	       | Total Time |	Peak Memory | CPU Usage |	Improvement                           |
|----------------------|--------------|--------------|-----------|-----------------------------------|
| **Unoptimized**	   | 15:00 min	  | 951 MB       | 40%	     | Baseline                          |
| **2M Row Batch**   | 12:46 min	  | 160 MB	     | 12%	     | 15% faster                  |
| **Parallelized**	   | 12:20 min	  | 951 MB	     | 15%	     | 17% faster                    |
| **Final Optimized** | 11:28 min	  | 521 MB	     | 14%	     | 30% faster, 45% less memory |

### **Results by Dataset Size:**

- 1M rows (CSV): 3.68s → 2.09s (43% faster)
- 1M rows (Parquet): 19.22s → 12.80s (33% faster)
- 244M rows (Parquet): 15:00min → 11:28min (30% faster)

## ✨ Key Features

### 🧠 **Automatic Load Intelligence**

- Memory-aware decision engine: Analyzes available RAM vs needed data
- Three automatic strategies: eager (RAM), lazy (LazyFrame), streaming (chunks)
- Overhead calculation: Accurate for CSV and Parquet based on real data types
- Hardware adaptation: Dynamically adjusts to your resources

### 🛡️ **Validation and Consistency**

- Schema validation with Pandera: Ensures consistency between executions
- Pydantic for configs: Validates YAML/TOML with strict types
- Data type enforcement: Safe casting with error handling
- Persistent schema tracking: Automatically detects drifts

### ⚡ **Optimized Performance**

- PyArrow zero-copy: Reduces memory overhead
- Intelligent batch size: Dynamically adjusts based on memory pressure
- Streaming engine: For datasets that don't fit in RAM
- Efficient Postgres ingestion: COPY command with optimized batches

### 🔄 **End-to-End Pipeline**

- Dual configuration: YAML or TOML
- Customizable ETL: Renaming, type casting, date parsing
- Database integration: PostgreSQL + DuckDB for fast queries
- Docker-ready: Reproducible infrastructure

## 🏗️ System Architecture

### **Data Flow:**

1. 📄 Config YAML/TOML → Pydantic validation
2. 🔍 File analysis → Memory estimation
3. 🧠 Decision engine → eager/lazy/streaming
4. ⚙️ ETL processing → Type casting, renaming
5. 🛡️ Schema validation → Pandera check
6. 🗄️ Database ingestion → PostgreSQL
7. 📊 Query layer → DuckDB for analysis

## 🚀 Quick Installation

```bash 
# Clone the repository
git clone https://github.com/sm7ss/production-data-pipeline.git
cd production-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL with Docker
docker-compose up -d
```

### **Main Dependencies:**

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

## 💻 Usage in 3 Steps

### **1. Configuration (config.yml):**

```yaml 
path:
  input_path: 'datos.parquet'

schema_config:
  column_naming: 'lower'
  date_format: true
  data_type: {'age': 'int64', 'salary': 'float64'}

validation_data:
  sample_size: 0.01

os_configuration:
  os_margin: 0.3
  n_rows_sample: 1000

database:
  table_name: 'employees'
  if_table_exists: 'replace'
```

### **2. Execution:**

```python 
from src.etl.EngineDecision import EngineDecision

# Automatic pipeline
pipeline = EngineDecision()
results = pipeline.orquestador_pipeline()

print(f"✅ Processed: {results['total_rows']} rows")
print(f"⏱️  Time: {results['execution_time']:.2f}s")
print(f"💾 Memory used: {results['memory_used_mb']:.2f} MB")
```

### **3. Analysis with DuckDB:**

```python 
from src.database.DuckDBQueries import DuckDBPostgresConnector

# Fast query over PostgreSQL
df = DuckDBPostgresConnector.query("""
  SELECT department, AVG(salary) as avg_salary
  FROM pg_main.employees
  GROUP BY department
  ORDER BY avg_salary DESC
""")
```

## 🔧 Advanced Configuration

### **Memory Optimization:**

```yaml 
os_configuration:
  os_margin: 0.3        # 30% safety margin
  n_rows_sample: 5000   # Sampling for estimation
```

### **Custom ETL:**

```yaml 
schema_config:
  column_naming: 'upper'  # lower, upper, capitalize
  date_format: true       # Auto-detects date strings
  data_type: {'user_id': 'int64', 'created_at': 'datetime', 'price': 'float64'} # Manual casting
  decimal_precision: 3    # Decimal precision
```

### **Database Strategies:**

```yaml 
database:
  table_name: 'analytics_table'
  if_table_exists: 'append'  # fail, append, replace
```

## 📊 Business Use Cases

### **1. Daily High-Volume ETL**

```python 
# Nightly processing of 100M+ rows
pipeline = EngineDecision()
pipeline.orquestador_pipeline()  # Auto-optimizes for nightly resources
```

### **2. Data Quality Monitoring**

```python 
# Automatic validation of schema changes
from src.validation.PanderaSchema import PanderaSchema

validator = PanderaSchema(config="config.yml", data="new_data.parquet")
try:
    validator.validation_schema()  # ✅ Consistent schema
except:
    alert_team("Schema changed!")  # 🚨 Data drift detected
```

### **3. Resource-Constrained Environments**

- Execution on servers with limited RAM
- System auto-adjusts to 4GB, 8GB, 16GB RAM
- Automatically switches between eager/lazy/streaming

### **4. Development vs Production**

```yaml 
# Development (small sample)
validation_data:
  sample_size: 0.01  # 1% for fast development

# Production (complete validation)  
validation_data:
  sample_size: 0.10  # 10% for safe production
```

## 🎯 Decision Engine: How It Works

### **Decision Algorithm:**

```python 
def decide_strategy(file_size, available_ram, os_margin=0.3):
    safety_ram = available_ram * (1 - os_margin)
    estimated_needed = estimate_memory_need(file_size)
    
    ratio = estimated_needed / safety_ram
    
    if ratio <= 0.65:      # 65% of available RAM
        return "eager"     # ✅ Full load
    elif ratio <= 2.0:     # Up to 200% 
        return "lazy"      # ⚡ LazyFrame
    else:                  # More than 200%
        return "streaming" # 🚀 Chunk processing
```

### **Accurate Estimation:**

- CSV: Based on data types and string lengths
- Parquet: Uses real PyArrow metadata (uncompressed size)
- Considers: Integers, floats, strings, dates, nested types

## 🔬 Detailed Performance Metrics

### **Test Hardware:**

- RAM: 15.5GB total, 8-9GB typically available
- CPU: 12 physical cores
- Storage: 250GB SSD + 1TB HDD
- 100% local: No cloud

### **Dataset 1: CSV (1M rows, 5 columns)**

**Baseline**:    3.68s, 347MB, 97.5% CPU
**Optimized**:   2.09s, 346MB, 80.2% CPU
**Improvement**: 43% faster, same memory

### **Dataset 2: Parquet (1M rows, 26 columns)**

**Baseline**:    19.22s, 1302MB, 55.4% CPU  
**Optimized**:   12.80s, 1306MB, 47.7% CPU
**Improvement**: 33% faster, more efficient CPU

### **Dataset 3: Parquet (244M rows, 5 columns)**

**Baseline**:    15:00min, 951MB, 40% CPU
**Optimized**:   11:28min, 521MB, 14% CPU
**Improvement**: 30% faster, 45% less memory

## 🛠️ Detailed Tech Stack

### **Main Processing:**

- Polars: Ultra-fast DataFrames in Rust
- PyArrow: Zero-copy metadata access, efficient serialization
- Pydantic: Configuration validation with type safety

### **Validation and Quality:**

- Pandera: Schema validation, data contract compliance
- Custom validators: File existence, type compatibility, naming rules

### **Infrastructure and Monitoring:**

- Docker: Reproducible environments, PostgreSQL container
- psutil: Memory monitoring, hardware detection
- tracemalloc: Detailed memory tracking
- Custom metrics: Execution time, memory usage, row count

### **Database Layer:**

- PostgreSQL: Production-level data storage
- DuckDB: Analytical queries, fast aggregations
- psycopg2: Efficient batch insertions with COPY command

### **Configuration and Flexibility:**

- YAML/TOML: Dual configuration format support
- Enum strategies: Type-safe configuration options
- Environment variables: Secure credential management

## 🤝 Contributions and Feedback

If you have suggestions for:

- Architecture improvements
- Performance optimizations
- Data engineering best practices
- Ideas for new features

Your feedback is very welcome! 💫

### **Contribution Guide:**

1. Fork the project
2. Create feature branch (git checkout -b feature/new-db)
3. Commit changes (git commit -m 'feat: add ClickHouse support')
4. Push to branch (git push origin feature/new-db)
5. Open Pull Request
