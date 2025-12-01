# 🔄 Pipeline ETL - Polars + PostgreSQL + DuckDB

[![Estado](https://img.shields.io/badge/Estado-🚧_Desarrollo_Activo-orange)]()
[![Stack](https://img.shields.io/badge/Stack-Python_Data_Engineering-blue)]()
[![ETL](https://img.shields.io/badge/ETL-Polars_|_PostgreSQL_|_DuckDB-green)]()

> **💡 Proyecto de Ingeniería de Datos** - Un pipeline ETL modular que explora las mejores herramientas para cada etapa del proceso de datos.

## 🎯 Objetivo del Proyecto

Implementar un **pipeline ETL robusto** utilizando herramientas modernas donde cada una brilla en su especialidad:

- **🔄 Polars**: Transformaciones ultra-rápidas de datos
- **🐘 PostgreSQL**: Almacenamiento persistente y estructurado  
- **🦆 DuckDB**: Análisis analítico rápido y eficiente
- **🐳 Docker**: Contenedorización de la base de datos

## 🛠️ Stack Tecnológico Actual

### **Lenguaje Principal**

![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)

### **Procesamiento de Datos**

![Polars](https://img.shields.io/badge/Polars-CD7929?logo=rust&logoColor=white)
![PyArrow](https://img.shields.io/badge/PyArrow-0C0D0D?logo=apachearrow&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black)

### **Base de Datos & Infraestructura**

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)

### **Calidad & Validación**

![Pydantic](https://img.shields.io/badge/Pydantic-E92063?logo=pydantic&logoColor=white)
![Pandera](https://img.shields.io/badge/Pandera-3EB049?logo=python&logoColor=white)

### **Utilidades**

![YAML](https://img.shields.io/badge/YAML-CB171E?logo=yaml&logoColor=white)
![Logging](https://img.shields.io/badge/Logging-000000?logo=python&logoColor=white)
![psutil](https://img.shields.io/badge/psutil-3A75BD?logo=python&logoColor=white)

## 📈 Estado Actual del Desarrollo

### **✅ Implementando**

- [ ] Configuración YAML con validación Pydantic
- [ ] Gestión de recursos del sistema con psutil
- [ ] Lectura eficiente de archivos CSV/Parquet
- [ ] Contenedor Docker para PostgreSQL

### **🚧 En Progreso**

- [ ] Sistema de logging estructurado
- [ ] Normalización robusta de formatos de fecha
- [ ] Validación de calidad de datos con Pandera
- [ ] Conexión y escritura a PostgreSQL
- [ ] Integración con DuckDB para análisis

### **📅 Próximos Objetivos**

- [ ] Transformaciones básicas con Polars
- [ ] Pipeline ETL end-to-end funcionando
- [ ] Optimización de memoria con PyArrow
- [ ] Análisis demostrativo con DuckDB

## 🚀 Instalación y Uso

```bash
# Clonar el proyecto
git clone https://github.com/sm7ss/modern-data-pipeline.git
cd modern-data-pipeline

# Instalar dependencias (ejemplo)
pip install -r requirements.txt

# Ejecutar contenedor de PostgreSQL
docker-compose up -d

# Ejecutar pipeline
python main.py
```

## 🎯 Casos de Uso Explorados

- 🔄 Transformaciones ETL con sintaxis moderna de Polars
- 📊 Análisis con la velocidad de DuckDB
- 🐳 Infraestructura reproducible con Docker
- ✅ Validación de datos con Pydantic + Pandera
- 📈 Metadatos y performance con PyArrow

## 🤝 Contribuciones y Feedback

Si tienes sugerencias para:

- Mejoras en la arquitectura
- Optimizaciones de performance
- Mejores prácticas de ingeniería de datos
- Ideas para nuevas features

¡Tu feedback es super bienvenido! 💫

## 👩‍💻 Sobre la Desarrolladora

💭 **¿Por qué este stack?** Cada herramienta fue elegida por su especialidad: Polars para velocidad, PostgreSQL para persistencia, DuckDB para análisis.
