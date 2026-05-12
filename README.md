# E-commerce Data Warehouse & Orchestration Stack

Este proyecto representa la construcción de una infraestructura moderna de datos local, diseñada para escalar y orquestar procesos de ETL/ELT. Es un proyecto de portafolio que demuestra habilidades en **Ingeniería de Datos**, **DevOps con Docker** y **Automatización con Python**.

## 🚀 Resumen del Proyecto

El objetivo es crear un entorno robusto que permita la ingesta de datos brutos (RAW), su orquestación mediante flujos de trabajo y la gestión de un Data Warehouse local.

## 🛠️ Pilares del Proyecto
*   **Infraestructura:** Un entorno completo con Docker (Postgres + Airflow + pgAdmin).
*   **Ingesta Robusta:** Script de Python que maneja errores, detecta cambios de esquema, evita duplicados y deja rastro de auditoría.
*   **Arquitectura de Datos (Medallion):** Pipeline de dbt con capas de Staging, Intermediate y Marts.
*   **Gobernanza:** Tests automáticos de calidad, integridad referencial y frescura de datos.
*   **Orquestación:** Un flujo automatizado en Airflow que vigila cada paso por ti.

---

### 🏗️ 1. Infraestructura con Docker (El Corazón del Sistema)
Se decidió utilizar **Docker Compose** para desplegar una arquitectura de tres capas:
*   **PostgreSQL (Data Warehouse):** Nuestra base de datos central. Se configuró para persistir los datos en volúmenes locales, asegurando que la información no se pierda al reiniciar los contenedores.
*   **Apache Airflow (Orquestador):** La herramienta estándar de la industria para programar y monitorear pipelines. Incluye un `Scheduler` y un `Webserver`.
*   **pgAdmin:** Una interfaz gráfica para explorar y validar los datos de manera rápida sin necesidad de usar la línea de comandos.

**Motivo:** Docker garantiza que cualquier persona pueda replicar este entorno exacto en segundos, eliminando el clásico problema de "en mi máquina funciona".

---

### 🐍 2. Entorno de Desarrollo Local
Se configuró un entorno virtual de Python (`venv`) fuera de Docker para ejecutar scripts de ingesta rápidos.
*   **Librerías clave:** `pandas` para el procesamiento, `SQLAlchemy` para la comunicación con la BD y `psycopg2` como driver de PostgreSQL.

---

### 📥 3. Capa de Ingesta (Scripts de Python)
Se desarrolló un script de ingesta inteligente (`scripts/ingest.py`) que sigue mejores prácticas de Data Engineering:

*   **Esquema RAW:** Los datos nunca se mezclan con el esquema por defecto. Se cargan en un schema dedicado llamado `raw` para mantener la trazabilidad.
*   **Metadata Crítica:** Cada fila insertada incluye:
    *   `_ingested_at`: Timestamp exacto de la carga.
    *   `_source_file`: Nombre del archivo de origen.
    *   `_batch_id`: Un hash único (MD5) basado en el contenido del archivo para identificar cargas específicas.

**¿Qué control te da esto?**

| Necesidad | Solución con las 3 columnas |
| :--- | :--- |
| **Auditoría** | `_batch_id` te permite rastrear CADA ingesta |
| **Rollback** | `DELETE WHERE _batch_id = 'xxx'` |
| **Debugging** | `_source_file` te dice qué archivo falló |
| **Re-procesamiento** | Filtra por `_batch_id` y regenera capas |
| **Evolución temporal** | `_ingested_at` muestra la historia completa |
| **Deduplicación inteligente** | `PARTITION BY negocio ORDER BY _ingested_at DESC` |

*   **Tabla de Auditoría:** Se creó una tabla `ingestion_audit` que registra el éxito, los fallos, el tamaño del archivo y la cantidad de filas duplicadas en cada ejecución.

---

### 💎 4. Capa de Transformación (dbt - Data Build Tool)
Se implementó **dbt** para transformar los datos crudos en información de negocio siguiendo la **Arquitectura Medallion**:

*   **Staging (Silver - Individual):** Modelos 1:1 con las fuentes `raw` donde se renombran columnas, se estandarizan tipos de datos y se realiza limpieza básica (como la deduplicación de reseñas).
*   **Intermediate (Silver - Transformation):** Capa donde se realizan uniones críticas (Joins) y agregaciones (ej. totales por pedido, traducción de categorías al inglés).
*   **Marts (Gold - Business):** Tablas finales optimizadas para el análisis.
    *   `fct_orders`: Tabla de hechos con métricas financieras y estados de envío.
    *   `dim_customers` y `dim_products`: Dimensiones maestras.

#### 🛡️ Calidad y Gobernanza de Datos
*   **Tests de Esquema:** Se validan claves únicas y campos no nulos (`unique`, `not_null`).
*   **Integridad Referencial:** Tests de `relationships` para asegurar que no existen pedidos sin clientes válidos.
*   **Source Freshness:** Alertas automáticas si los datos de origen tienen más de 24 horas de antigüedad.
*   **Linaje Automatizado:** Generación de diagramas de dependencia para auditoría técnica.

---

### 🛠️ Pasos Realizados

1.  **Definición de Servicios:** Creación del `docker-compose.yml` con políticas de salud (*healthchecks*) para asegurar que Airflow no inicie hasta que la BD esté lista.
2.  **Configuración de Airflow:** Inicialización de la base de datos de metadatos y creación del usuario administrador de forma automática.
3.  **Desarrollo del Pipeline de Ingesta:** Programación del script `ingest.py` con validaciones de tipos de datos y limpieza de nombres de columnas (snake_case).
4.  **Implementación de dbt:** Configuración del proyecto `ecommerce_pipeline`, creación de modelos en 3 capas y validación mediante tests automatizados.
5.  **Orquestación con Airflow:** Creación de un DAG que automatiza la ingesta, la transformación y los tests de calidad de forma secuencial.

---

## 📈 Próximos Pasos
*   [ ] Construir un dashboard básico sobre el Data Warehouse (Marts).
*   [ ] Optimizar la materialización (Tablas Incrementales) para grandes volúmenes de datos.
