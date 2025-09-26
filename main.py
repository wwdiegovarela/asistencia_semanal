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

def fetch_and_process_data():
    """Función para obtener y procesar datos de la API externa"""
    print("=== OBTENIENDO Y PROCESANDO DATOS ===")
    
    # Preparar parámetros para la API local
    headers = {
        "method": "report",
        "token": TOKEN
    }
    print(f"API URL: {API_LOCAL_URL}")
    print(f"Headers: {headers}")
    
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
    
    if not data_json:
        print("No hay datos para procesar")
        return None

    # Convertir a DataFrame
    data_text = response.text
    print(f"Longitud de respuesta: {len(data_text)}")
    print(f"Primeros 200 caracteres: {data_text[:200]}")

    data_json = json.loads(data_text)

    # Convertir a DataFrame
    data = pd.DataFrame(data_json)
    date_columns = ['Her', 'FlogAsi','Hsr','Entrada', 'Salida','Dia']
    print("Transformando columnas a formato datetime")
    for col in date_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], format='%d-%m-%Y %H:%M:%S')
            
            
    datetime_columns=['FechaMarcaEntrada','FechaMarcaSalida']
    print("Transformando columnas a formato datetime")
    for col in datetime_columns:
        if col in data.columns:
           data[col] = pd.to_datetime(data[col], format='%Y-%m-%d %H:%M:%S')



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

    number_columns=['hrtotrol','hrextpacrol','hrextpacasi','hr_tot_asi','hrextremasi','valortvf']
    print("Transformando columnas a formato float")
    for col in number_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(
                data[col].astype(str)                      # asegura string
                         .str.replace(".", "", regex=False)  # quita miles
                         .str.replace(",", ".", regex=False) # coma -> punto
                         .str.strip(),                        # quita espacios
                errors="coerce"
            )

    
    # Procesar datos de rotación

    print(f"✅ Datos procesados exitosamente: {len(data)} registros")
    return data

def load_to_bigquery(df_bridge):
    """Función para cargar datos procesados a BigQuery"""
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
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            type_=bigquery.TimePartitioningType.DAY,  # partición diaria
        field="dia"
        )
        
        print(f"🔄 Cargando {len(df_bridge)} registros a BigQuery: {table_id}")
        job = client.load_table_from_dataframe(df_bridge, table_id, job_config=job_config)
        job.result()
        
        print(f"✅ Data cargada exitosamente. {len(df_bridge)} registros cargados a BigQuery.")
        
        return {
            "success": True,
            "message": "Data procesada y cargada exitosamente",
            "records_processed": len(df_bridge)
        }
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






