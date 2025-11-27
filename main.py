# -*- coding: utf-8 -*-
"""
Created on Wed Sep 10 17:19:13 2025

@author: Diego
"""

from fastapi import FastAPI, HTTPException
import requests
from google.cloud import bigquery
import json
from datetime import datetime
import pandas as pd
import os

# Configuración
API_LOCAL_URL = os.getenv("API_LOCAL_URL")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
TOKEN = os.getenv("TOKEN_CR")
TOKEN_INDUSTRY = os.getenv("TOKEN_CR_INDUSTRY")

def delete_range_in_bigquery(client: bigquery.Client, table_id: str, start_date, end_date) -> int:
    """
    Elimina en BigQuery todas las filas cuyo campo `dia` (cast a DATE)
    esté entre start_date y end_date (inclusive).
    """
    if start_date is None or end_date is None:
        print("⚠️ Rango de fechas no válido. Se omite la eliminación.")
        return 0

    sql = f"""
    DELETE FROM `{table_id}`
    WHERE DATE(dia) BETWEEN @start_date AND @end_date
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    print(f"🧹 Eliminando en destino: DATE(dia) BETWEEN {start_date} AND {end_date}")
    job = client.query(sql, job_config=job_config)
    job.result()  # espera a que termine

    deleted = job.num_dml_affected_rows or 0
    print(f"✅ Filas eliminadas: {deleted}")
    return deleted

def _transform_to_dataframe(data_json, empresa_label: str) -> pd.DataFrame | None:
    if not data_json:
        return None
    # Convertir a DataFrame
    data = pd.DataFrame(data_json)

    # Fechas con formato dd-mm-YYYY HH:MM:SS
    date_columns = ['Her', 'FlogAsi','Hsr','Entrada', 'Salida','Dia']
    print("Transformando columnas a formato datetime (DD-MM-YYYY HH:MM:SS)")
    for col in date_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], format='%d-%m-%Y %H:%M:%S', errors='coerce')

    # Fechas con formato YYYY-MM-DD HH:MM:SS
    datetime_columns=['FechaMarcaEntrada','FechaMarcaSalida']
    print("Transformando columnas a formato datetime (YYYY-MM-DD HH:MM:SS)")
    for col in datetime_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    # Normalizar nombres de columnas
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')
    data.columns = data.columns.str.replace('.', '')
    data.columns = data.columns.str.replace('%', '')
    data.columns = data.columns.str.replace('-', '_')
    data.columns = data.columns.str.replace('(', '')
    data.columns = data.columns.str.replace(')', '')
    data.columns = data.columns.str.replace('á', 'a')
    data.columns = data.columns.str.replace('é', 'e')
    data.columns = data.columns.str.replace('í', 'i')
    data.columns = data.columns.str.replace('ó', 'o')
    data.columns = data.columns.str.replace('ú', 'u')
    data.columns = data.columns.str.replace('ñ', 'n')
    data.columns = data.columns.str.replace('°', '')

    # Números con miles y coma decimal
    number_columns=['hrtotrol','hrextpacrol','hrextpacasi','hr_tot_asi','hrextremasi','valortvf']
    print("Transformando columnas a formato float")
    for col in number_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(
                data[col].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False).str.strip(),
                errors="coerce"
            )

    # Agregar etiqueta de empresa
    data["empresa"] = empresa_label

    print(f"✅ Datos transformados ({empresa_label}): {len(data)} registros")
    return data

def _fetch_from_api(token: str) -> list:
    headers = {"method": "report", "token": token}
    print(f"API URL: {API_LOCAL_URL}")
    print(f"Headers: {{'method': 'report', 'token': '***'}}")
    try:
        print("🔄 Iniciando llamada a ControlRoll...")
        response = requests.get(API_LOCAL_URL, headers=headers, timeout=3600)
        print("✅ Llamada completada")
    except requests.exceptions.Timeout:
        error_msg = "Timeout: La API externa tardó más de 1 hora en responder"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=504, detail=error_msg)
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Error de conexión con la API externa: {str(e)}"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=502, detail=error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = f"Error en la petición HTTP: {str(e)}"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=502, detail=error_msg)
    except Exception as e:
        error_msg = f"Error inesperado: {type(e).__name__}: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_msg)
    
    print(f"Status code: {response.status_code}")
    response.raise_for_status()
    data_text = response.text
    print(f"Longitud de respuesta: {len(data_text)}")
    print(f"Primeros 200 caracteres: {data_text[:200]}")

    data_json = json.loads(data_text)
    print(f"Datos obtenidos: {len(data_json)} registros")
    print(f"Primer registro: {data_json[0] if data_json else 'No hay datos'}")
    return data_json

def _fetch_and_process_for_token(token: str | None, empresa_label: str) -> pd.DataFrame | None:
    if not token:
        print(f"⚠️ Token no definido para {empresa_label}. Se omite esta fuente.")
        return None
    data_json = _fetch_from_api(token)
    if not data_json:
        print(f"⚠️ Fuente sin datos para {empresa_label}.")
        return None
    return _transform_to_dataframe(data_json, empresa_label)

def fetch_and_process_data():
    """Obtiene y procesa datos desde dos fuentes y las combina antes de cargar."""
    print("=== OBTENIENDO Y PROCESANDO DATOS (2 FUENTES) ===")

    # Fuente Security (token original)
    df_security = _fetch_and_process_for_token(TOKEN, "Security")
    # Fuente Industry (nuevo token)
    df_industry = _fetch_and_process_for_token(TOKEN_INDUSTRY, "Industry")

    if df_security is None and df_industry is None:
        print("No hay datos para procesar de ninguna fuente")
        return None

    frames = [df for df in [df_security, df_industry] if df is not None]
    combined = pd.concat(frames, ignore_index=True)
    print(f"✅ Datos combinados total: {len(combined)} registros")
    return combined

def load_to_bigquery(df_bridge):
    """Función para cargar datos procesados a BigQuery (reemplazo por rango)."""
    if df_bridge is None:
        return {
            "success": True,
            "message": "No hay datos para cargar",
            "records_processed": 0
        }

    print("=== CARGANDO DATOS A BIGQUERY ===")

    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        # ===================== NUEVO: obtener min/max de 'dia' =====================
        if 'dia' not in df_bridge.columns:
            raise HTTPException(status_code=400, detail="La columna 'dia' no existe en los datos a cargar.")

        # Asegura tipo datetime y toma solo la fecha (para BQ como DATE)
        if not pd.api.types.is_datetime64_any_dtype(df_bridge['dia']):
            df_bridge['dia'] = pd.to_datetime(df_bridge['dia'], errors='coerce')

        if df_bridge['dia'].isna().all():
            raise HTTPException(status_code=400, detail="La columna 'dia' no contiene fechas válidas.")

        start_date = df_bridge['dia'].dt.date.min()
        end_date   = df_bridge['dia'].dt.date.max()

        print(f"🗓️ Rango detectado en origen: {start_date} → {end_date}")

        # Borra en destino el rango a sustituir (incluye extremos)
        deleted_rows = delete_range_in_bigquery(client, table_id, start_date, end_date)
        # ==========================================================================

        # Configuración de carga (append porque ya realizamos el “reemplazo”)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="dia"  # campo de partición
            ),
        )

        print(f"🔄 Cargando {len(df_bridge)} registros a BigQuery: {table_id}")
        job = client.load_table_from_dataframe(df_bridge, table_id, job_config=job_config)
        job.result()

        print(f"✅ Data cargada exitosamente. {len(df_bridge)} registros cargados a BigQuery.")

        return {
            "success": True,
            "message": (
                f"Data procesada y cargada exitosamente. "
                f"Se eliminaron {deleted_rows} filas previas en el rango {start_date} a {end_date}."
            ),
            "records_processed": len(df_bridge),
            "deleted_rows": int(deleted_rows),
            "range_start": str(start_date),
            "range_end": str(end_date)
        }

    except HTTPException:
        # Relevantar tal cual si ya es HTTPException
        raise
    except Exception as e:
        error_msg = f"Error al cargar datos en BigQuery: {type(e).__name__}: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_msg)

def sync_to_bigquery():
    """Función principal para sincronizar datos con BigQuery"""
    print("=== INICIANDO SINCRONIZACIÓN COMPLETA ===")
    
    # Paso 1: Obtener y procesar datos
    df_bridge = fetch_and_process_data()
    
    # Paso 2: Cargar a BigQuery
    result = load_to_bigquery(df_bridge)
    
    return result

# Crear la aplicación FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"message": "Servicio de sincronización de rotación activo"}

@app.get("/health")
def health_check():
    """Endpoint de salud para verificar el estado del servicio"""
    return {
        "status": "healthy",
        "message": "Servicio funcionando correctamente",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/fetch_data")
def fetch_data():
    """
    Endpoint para obtener y procesar datos de la API externa (sin cargar a BigQuery)
    """
    try:
        df_bridge = fetch_and_process_data()
        if df_bridge is None:
            return {
                "success": True,
                "message": "No hay datos para procesar",
                "records_processed": 0
            }
        
        return {
            "success": True,
            "message": "Datos obtenidos y procesados exitosamente",
            "records_processed": len(df_bridge),
            "columns": list(df_bridge.columns),
            "sample_data": df_bridge.head(3).to_dict('records') if len(df_bridge) > 0 else []
        }
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al obtener y procesar datos"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/load_data")
def load_data():
    """
    Endpoint para cargar datos procesados a BigQuery
    """
    try:
        # Primero obtener los datos
        df_bridge = fetch_and_process_data()
        
        # Luego cargarlos a BigQuery
        result = load_to_bigquery(df_bridge)
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al cargar datos a BigQuery"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/industry_load")
def industry_load():
    """
    Endpoint para cargar solo datos de Industry a BigQuery
    """
    try:
        # Obtener y procesar únicamente Industry
        df_bridge = _fetch_and_process_for_token(TOKEN_INDUSTRY, "Industry")
        # Cargar a BigQuery (maneja df_bridge None internamente)
        result = load_to_bigquery(df_bridge)
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al cargar datos de Industry a BigQuery"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/rotacion_sync")
def rotacion_sync():
    """
    Endpoint para sincronizar datos de rotación (proceso completo)
    """
    try:
        result = sync_to_bigquery()
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al procesar la sincronización"
        }
        raise HTTPException(status_code=500, detail=error_response)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))









