# -*- coding: utf-8 -*-
"""
Created on Tue Sep  2 16:36:25 2025

@author: Diego
"""

import requests
import json
import pandas as pd
from google.cloud import bigquery
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Lista de turnos a excluir
turnos_no = [
    '+4x4   ', '+5x2   ', '+6x1   ', '+6x2   ', '+7x7   ',
    'F      ', 'LME    ', 'LMPP   ', 'LMU    ', 'OS1    ',
    'OS2    ', 'OS3    ', 'P1     ', 'P2     ', 'PL3    ',
    'PL4    ', 'PL7    ', 'PNH    ', 'V1     ', 'IND3   ',
    'IND2   ', 'PMA    '
]

def obtener_datos_controlroll(token):
    """Obtiene datos de la API de ControlRoll"""
    try:
        url = "https://cl.controlroll.com/ww01/ServiceUrl.aspx"
        
        headers = {
            "method": "report",
            "token": token
        }
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data_json = json.loads(response.text)
        return pd.DataFrame(data_json)
        
    except Exception as e:
        logger.error(f"Error obteniendo datos: {e}")
        raise

def procesar_dataframe(df):
    """Procesa y limpia el DataFrame"""
    try:
        # Filtrar turnos no deseados
        df2 = df.loc[~df.Turno.isin(turnos_no)]
        
        # Limpiar columna HrTotRol
        df2['HrTotRol'] = df2['HrTotRol'].str.replace(',', '.')
        df2['HrTotRol'] = df2['HrTotRol'].astype(float)
        
        # Convertir columnas de fecha
        df2['Her'] = pd.to_datetime(df2['Her'], format='%d-%m-%Y %H:%M:%S')
        df2['Hsr'] = pd.to_datetime(df2['Hsr'], format='%d-%m-%Y %H:%M:%S')
        df2['FlogAsi'] = pd.to_datetime(df2['FlogAsi'], format='%d-%m-%Y %H:%M:%S')
        
        logger.info(f"DataFrame procesado: {len(df2)} filas")
        return df2
        
    except Exception as e:
        logger.error(f"Error procesando DataFrame: {e}")
        raise

def cargar_a_bigquery(df, project_id, dataset_id, table_id):
    """Carga DataFrame a BigQuery"""
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        
        job = client.load_table_from_dataframe(
            df, 
            table_ref, 
            job_config=job_config
        )
        
        job.result()
        
        logger.info(f"DataFrame cargado exitosamente a {table_ref}")
        logger.info(f"Filas cargadas: {len(df)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error al cargar a BigQuery: {e}")
        return False

def main():
    """Función principal"""
    
    # Configuración desde variables de entorno
    token = os.getenv('CONTROLROLL_TOKEN', 'xMAOXmuzF8MeHi+qZLbgkd1Dg8WuzzHXi/eDMGNgO+8=')
    PROJECT_ID = "worldwide-470917"
    DATASET_ID = "worldwide_rrhh"
    TABLE_ID = "Cobertura_WW"
    
    if not all([PROJECT_ID, DATASET_ID]):
        raise ValueError("PROJECT_ID y DATASET_ID son requeridos")
    
    try:
        logger.info("Iniciando proceso...")
        
        # 1. Obtener datos de la API
        logger.info("Obteniendo datos de ControlRoll...")
        df = obtener_datos_controlroll(token)
        logger.info(f"Datos obtenidos: {len(df)} filas")
        
        # 2. Procesar DataFrame
        logger.info("Procesando datos...")
        df2 = procesar_dataframe(df)
        
        # 3. Cargar a BigQuery
        logger.info("Cargando a BigQuery...")
        exito = cargar_a_bigquery(df2, PROJECT_ID, DATASET_ID, TABLE_ID)
        
        if exito:
            logger.info("¡Proceso completado exitosamente!")
            return {"status": "success", "rows_processed": len(df2)}
        else:
            logger.error("Error en la carga a BigQuery")
            return {"status": "error", "message": "Falló la carga a BigQuery"}
            
    except Exception as e:
        logger.error(f"Error en el proceso: {e}")
        raise

# Para Cloud Run, necesitamos una función HTTP
def controlroll_endpoint(request):
    """Endpoint HTTP para Cloud Run"""
    try:
        result = main()
        return result, 200
    except Exception as e:
        logger.error(f"Error en endpoint: {e}")
        return {"error": str(e)}, 500

if __name__ == "__main__":
    # Para ejecución local
    result = main()
    print(result)