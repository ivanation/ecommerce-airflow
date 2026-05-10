import os
import hashlib
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from pathlib import Path

# Configuración de base de datos local (según docker-compose)
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'airflow'

# String de conexión SQLAlchemy
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Directorio de los CSVs (relativo a la ubicación de este script)
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / 'data' / 'raw'

def create_schema_if_not_exists():
    """Crea el schema 'raw' en la base de datos si no existe."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()
    print("✅ Schema 'raw' verificado/creado exitosamente.")

def validate_and_clean(df):
    """
    Función básica para limpiar y validar tipos de datos.
    Aplica conversiones genéricas o limpieza de nulos.
    """
    # 1. Eliminar filas donde todos los valores sean nulos
    df = df.dropna(how='all')
    
    # 2. Convertir nombres de columnas a minúsculas y sin espacios (buena práctica para BD)
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    
    # 3. Intentar inferir fechas en columnas de tipo objeto/string
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                # Convierte a datetime solo si el formato es consistente, ignora si falla
                df[col] = pd.to_datetime(df[col], format='mixed')
            except (ValueError, TypeError):
                pass
                
    return df

def ingest_data():
    # Validar que el directorio exista
    if not RAW_DIR.exists() or not RAW_DIR.is_dir():
        print(f"❌ El directorio de datos no existe: {RAW_DIR}")
        return

    csv_files = list(RAW_DIR.glob('*.csv'))
    if not csv_files:
        print(f"⚠️ No se encontraron archivos CSV en {RAW_DIR}")
        return

    # Preparar el schema
    create_schema_if_not_exists()

    # Procesar cada CSV encontrado
    for csv_file in csv_files:
        table_name = csv_file.stem.lower() # Nombre de tabla = nombre del archivo
        print(f"\nProcesando archivo: {csv_file.name} -> Tabla: raw.{table_name}")
        
        try:
            file_size_mb = csv_file.stat().st_size / (1024 * 1024)
            file_hash = hashlib.md5(csv_file.read_bytes()).hexdigest()
            
            # Leer el CSV
            df = pd.read_csv(csv_file)
            rows_total = len(df)
            rows_duplicates = len(df) - len(df.drop_duplicates())
            
            # Validación y limpieza
            df = validate_and_clean(df)
            
            # AÑADIR METADATA (CRÍTICO)
            df['_ingested_at'] = datetime.now()
            df['_source_file'] = csv_file.name
            df['_batch_id'] = file_hash[:8]
            
            # Guardar en RAW (CON duplicados)
            df.to_sql(
                name=table_name,
                con=engine,
                schema='raw',
                if_exists='append',
                index=False,
                method='multi'  # Mejor rendimiento
            )
            print(f"✅ Se cargaron exitosamente {len(df)} filas en raw.{table_name}.")
            
            # Almacenas el contexto completo de cada ingesta
            audit_table = {
                '_batch_id': file_hash[:8],
                'source_file': csv_file.name,
                'ingested_at': datetime.now(),
                'rows_total': rows_total,
                'rows_duplicates': rows_duplicates,
                'file_size_mb': file_size_mb,
                'status': 'completed'
            }
            pd.DataFrame([audit_table]).to_sql(
                name='ingestion_audit',
                con=engine,
                schema='raw',
                if_exists='append',
                index=False
            )
            
        except Exception as e:
            print(f"❌ Error al procesar {csv_file.name}: {e}")
            audit_table = {
                '_batch_id': file_hash[:8] if 'file_hash' in locals() else None,
                'source_file': csv_file.name,
                'ingested_at': datetime.now(),
                'rows_total': rows_total if 'rows_total' in locals() else None,
                'rows_duplicates': rows_duplicates if 'rows_duplicates' in locals() else None,
                'file_size_mb': file_size_mb if 'file_size_mb' in locals() else None,
                'status': f'failed: {str(e)[:100]}'
            }
            try:
                pd.DataFrame([audit_table]).to_sql(
                    name='ingestion_audit',
                    con=engine,
                    schema='raw',
                    if_exists='append',
                    index=False
                )
            except Exception as audit_e:
                print(f"⚠️ No se pudo guardar auditoría de error: {audit_e}")

if __name__ == '__main__':
    print("🚀 Iniciando proceso de ingesta de CSVs a PostgreSQL...")
    ingest_data()
    print("🏁 Proceso de ingesta finalizado.")
