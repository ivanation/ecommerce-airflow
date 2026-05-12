import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from pathlib import Path
from datetime import datetime
import hashlib
import json
import warnings

# Configuración de base de datos local
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = '5432'
DB_NAME = 'airflow'

engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / 'data' / 'raw'

# ============================================================
# NUEVAS FUNCIONES PARA MANEJO DE ESQUEMAS
# ============================================================

def create_schemas():
    """Crea esquemas necesarios: raw, audit, quarantine"""
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS quarantine;"))
    print("✅ Esquemas verificados/creados")

def create_audit_tables():
    """Crea tablas de control de cambios de esquema"""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit.schema_registry (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(100),
                schema_hash VARCHAR(8),
                columns JSONB,
                detected_at TIMESTAMP DEFAULT NOW(),
                source_file VARCHAR(255),
                is_active BOOLEAN DEFAULT TRUE
            );
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS audit.ingestion_log (
                id SERIAL PRIMARY KEY,
                table_name VARCHAR(100),
                batch_id VARCHAR(8),
                source_file VARCHAR(255),
                rows_ingested INT,
                rows_rejected INT,
                status VARCHAR(20),
                message TEXT,
                ingested_at TIMESTAMP DEFAULT NOW()
            );
        """))
        # Aseguramos que la columna source_file existe (Evolución manual)
        conn.execute(text("ALTER TABLE audit.ingestion_log ADD COLUMN IF NOT EXISTS source_file VARCHAR(255);"))
    print("✅ Tablas de auditoría listas y actualizadas")

def is_file_processed(filename):
    """Verifica si un archivo ya fue procesado exitosamente"""
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM audit.ingestion_log 
            WHERE source_file = :filename AND status = 'SUCCESS'
        """), {'filename': filename}).scalar()
    return result > 0

def get_table_columns(schema, table_name):
    """Obtiene columnas existentes en una tabla (si existe)"""
    inspector = inspect(engine)
    if inspector.has_table(table_name, schema=schema):
        return [col['name'] for col in inspector.get_columns(table_name, schema=schema)]
    return None

def get_schema_hash(df):
    """Calcula hash único del esquema del DataFrame"""
    schema_info = {
        'columns': list(df.columns),
        'dtypes': {col: str(df[col].dtype) for col in df.columns}
    }
    return hashlib.md5(json.dumps(schema_info, sort_keys=True).encode()).hexdigest()[:8]

def register_schema(table_name, schema_hash, df, source_file):
    """Registra el esquema en la tabla de auditoría"""
    columns_info = {col: str(df[col].dtype) for col in df.columns}
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO audit.schema_registry (table_name, schema_hash, columns, source_file)
            VALUES (:table_name, :schema_hash, :columns, :source_file)
        """), {
            'table_name': table_name,
            'schema_hash': schema_hash,
            'columns': json.dumps(columns_info),
            'source_file': source_file
        })

def evolve_schema_if_needed(df, schema, table_name):
    """
    Detecta columnas nuevas o faltantes y evoluciona la tabla automáticamente.
    Retorna (df_adaptado, cambios_detectados)
    """
    existing_cols = get_table_columns(schema, table_name)
    if not existing_cols:
        # Tabla nueva, no hay evolución necesaria
        return df, {'added': [], 'removed': [], 'type_changes': []}
    
    current_cols = set(df.columns)
    existing_set = set(existing_cols)
    
    # Ignorar columnas de auditoría en la comparación para no intentar añadirlas si ya existen
    audit_cols = {'_ingested_at', '_source_file', '_batch_id'}
    existing_set = existing_set - audit_cols
    current_cols = current_cols - audit_cols
    
    added = current_cols - existing_set
    removed = existing_set - current_cols
    
    changes = {'added': list(added), 'removed': list(removed), 'type_changes': []}
    
    # 1. Añadir columnas nuevas a la tabla
    with engine.begin() as conn:
        for col in added:
            # Inferir tipo SQL básico
            sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else 'text'
            if pd.api.types.is_integer_dtype(df[col]):
                sql_type = "INTEGER"
            elif pd.api.types.is_float_dtype(df[col]):
                sql_type = "FLOAT"
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "TEXT"
            try:
                conn.execute(text(f"ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS {col} {sql_type}"))
                print(f"   ➕ Columna añadida: {col} ({sql_type})")
            except Exception as e:
                print(f"   ⚠️ No se pudo añadir {col}: {e}")
    
    # 2. Para columnas removidas: no eliminamos datos históricos, solo advertimos
    if removed:
        print(f"   ⚠️ Columnas ya no presentes en el nuevo archivo: {removed}")
        print(f"      Se insertarán como NULL en esas columnas.")
    
    # 3. Adaptar DataFrame: añadir columnas faltantes con NULL
    for col in removed:
        df[col] = None  # para que la inserción no falle
    
    # 4. Reordenar columnas para que coincidan con la tabla existente (opcional pero limpio)
    df = df[existing_cols + list(added)]  # mantiene orden
    
    return df, changes

def save_to_quarantine(df, csv_file, table_name, reason):
    """Guarda datos problemáticos en cuarentena para revisión manual"""
    quarantine_path = BASE_DIR / 'quarantine' / f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(quarantine_path, index=False)
    print(f"   🛑 Datos enviados a cuarentena: {quarantine_path}")
    # También guardar metadatos del problema
    with open(quarantine_path.with_suffix('.meta.txt'), 'w') as f:
        f.write(f"Table: {table_name}\nReason: {reason}\nSource: {csv_file.name}\nDate: {datetime.now()}")
    return str(quarantine_path)

def validate_and_clean(df):
    """Tu función original mejorada (sin cambiar nombres de columnas a minúsculas si quieres preservar original)"""
    # Eliminar filas vacías
    df = df.dropna(how='all')
    
    # estandarizar nombres de columnas (minúsculas, sin espacios)
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    
    # Inferir fechas solo en columnas que parezcan fechas por su nombre (optimización heurística)
    date_keywords = ['date', 'time', '_at', 'timestamp']
    for col in df.columns:
        if df[col].dtype == 'object':
            if any(keyword in col.lower() for keyword in date_keywords):
                try:
                    df[col] = pd.to_datetime(df[col], format='mixed')
                except (ValueError, TypeError):
                    pass
    return df

def ingest_data():
    # Validar directorio
    if not RAW_DIR.exists():
        print(f"❌ Directorio no existe: {RAW_DIR}")
        return
    
    csv_files = list(RAW_DIR.glob('*.csv'))
    if not csv_files:
        print(f"⚠️ No hay CSVs en {RAW_DIR}")
        return
    
    # Preparar esquemas y tablas de control
    create_schemas()
    create_audit_tables()
    
    for csv_file in csv_files:
        if is_file_processed(csv_file.name):
            print(f"⏩ Saltando: {csv_file.name} (Ya procesado anteriormente)")
            continue

        table_name = csv_file.stem.lower()
        # Si el nombre tiene fecha (ej: orders_20231027), limpiamos para que la tabla sea solo 'orders'
        if '_' in table_name and any(char.isdigit() for char in table_name):
            table_name = table_name.split('_')[0]
            
        print(f"\n📄 Procesando: {csv_file.name} → raw.{table_name}")
        
        try:
            # 1. Leer CSV (Optimizado con PyArrow)
            df = pd.read_csv(csv_file, engine='pyarrow')
            original_rows = len(df)
            
            # 2. Limpiar datos
            df = validate_and_clean(df)
            
            # 3. Añadir columnas de control (¡ANTES de evolucionar el esquema!)
            df['_ingested_at'] = datetime.now()
            df['_source_file'] = csv_file.name
            df['_batch_id'] = hashlib.md5(csv_file.name.encode()).hexdigest()[:8]
            
            # 4. Calcular hash del esquema (ahora con auditoría)
            schema_hash = get_schema_hash(df)
            
            # 5. Registrar esquema (si es nuevo)
            register_schema(table_name, schema_hash, df, csv_file.name)
            
            # 6. Evolucionar esquema de la tabla si es necesario
            df, changes = evolve_schema_if_needed(df, 'raw', table_name)
            
            # 7. Si hay cambios drásticos, opcionalmente guardar copia en cuarentena
            if changes['removed']:
                # Podrías decidir si esto es crítico o no
                warnings.warn(f"Columnas removidas en {csv_file.name}: {changes['removed']}")
                # Opcional: guardar una copia en cuarentena
                save_to_quarantine(df, csv_file, table_name, f"columnas_removidas:{changes['removed']}")
            
            # 8. Cargar a PostgreSQL (APPEND con Paginación)
            df.to_sql(
                name=table_name,
                con=engine,
                schema='raw',
                if_exists='append',
                index=False,
                method='multi',      # mejora rendimiento
                chunksize=10000      # evita desbordamientos de memoria RAM
            )
            
            # 9. Log de éxito
            log_ingestion(table_name, schema_hash, csv_file.name, original_rows, len(df), 'SUCCESS', '')
            print(f"   ✅ Cargadas {len(df)} filas (nuevas columnas: {changes['added']})")
            
        except Exception as e:
            error_msg = str(e)
            print(f"   ❌ Error: {error_msg}")
            log_ingestion(table_name, None, csv_file.name, 0, 0, 'FAILED', error_msg)
            # Guardar el CSV problemático completo en cuarentena
            try:
                df_problem = pd.read_csv(csv_file)
                save_to_quarantine(df_problem, csv_file, table_name, f"error:{error_msg[:100]}")
            except:
                pass

def log_ingestion(table_name, batch_id, source_file, rows_in, rows_out, status, message):
    """Registra cada intento de ingesta"""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO audit.ingestion_log 
            (table_name, batch_id, source_file, rows_ingested, rows_rejected, status, message)
            VALUES (:table_name, :batch_id, :source_file, :rows_ingested, :rows_rejected, :status, :message)
        """), {
            'table_name': table_name,
            'batch_id': batch_id,
            'source_file': source_file,
            'rows_ingested': rows_in,
            'rows_rejected': rows_in - rows_out,
            'status': status,
            'message': message
        })

if __name__ == '__main__':
    print("🚀 Iniciando ingesta robusta con manejo de cambios de esquema...")
    ingest_data()
    print("🏁 Proceso completado.")