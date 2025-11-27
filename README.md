## Servicio FastAPI - Sincronización de Rotación a BigQuery

Servicio en FastAPI que sincroniza datos de rotación de empleados desde una API externa (ControlRoll) hacia BigQuery. Realiza reemplazo por rango de fechas usando la columna `dia` y luego carga en modo append con particionado diario.

### Archivos incluidos

- `main.py` - Aplicación FastAPI principal
- `requirements.txt` - Dependencias de Python
- `Dockerfile` - Imagen Docker para despliegue (puerto 8080)

### Variables de entorno requeridas

- `API_LOCAL_URL` - URL de la API de ControlRoll
- `PROJECT_ID` - ID del proyecto de GCP
- `DATASET_ID` - ID del dataset de BigQuery
- `TABLE_ID` - ID de la tabla de BigQuery
- `TOKEN_CR` - Token de autenticación para la API (empresa Security)
- `TOKEN_CR_INDUSTRY` - Segundo token para la API (empresa Industry)

### Ejecución local

```bash
pip install -r requirements.txt

# PowerShell (Windows)
$env:API_LOCAL_URL="https://tu-api"
$env:PROJECT_ID="tu-proyecto"
$env:DATASET_ID="tu_dataset"
$env:TABLE_ID="tu_tabla"
$env:TOKEN_CR="tu_token"
$env:TOKEN_CR_INDUSTRY="tu_token_industry"

python main.py
# o
uvicorn main:app --host 0.0.0.0 --port 8080
```

### Docker

```bash
docker build -t asistencia-semanal .
docker run -p 8080:8080 `
  -e API_LOCAL_URL="https://tu-api" `
  -e PROJECT_ID="tu-proyecto" `
  -e DATASET_ID="tu_dataset" `
  -e TABLE_ID="tu_tabla" `
  -e TOKEN_CR="tu_token" `
  -e TOKEN_CR_INDUSTRY="tu_token_industry" `
  asistencia-semanal
```

### Despliegue en Cloud Run (gcloud CLI)

```bash
gcloud builds submit --tag gcr.io/TU_PROYECTO/asistencia-semanal
gcloud run deploy asistencia-semanal \
  --image gcr.io/TU_PROYECTO/asistencia-semanal \
  --platform managed \
  --region us-east1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars API_LOCAL_URL="https://tu-api",PROJECT_ID="TU_PROYECTO",DATASET_ID="tu_dataset",TABLE_ID="tu_tabla",TOKEN_CR="tu_token"
```

La URL final será similar a:

```
https://asistencia-semanal-xxxxxxxx-uc.a.run.app
```

### Endpoints

- `GET /` - Estado básico del servicio
- `GET /health` - Healthcheck con timestamp
- `POST /fetch_data` - Obtiene y transforma datos (no carga a BQ)
- `POST /load_data` - Obtiene y carga a BigQuery (reemplazo por rango + append)
- `POST /industry_load` - Obtiene solo Industry y carga a BigQuery (sin borrado, append-only)
- `POST /rotacion_sync` - Proceso completo: fetch -> load

Ejemplos:

```bash
curl -X GET  https://TU_URL/health
curl -X POST https://TU_URL/fetch_data
curl -X POST https://TU_URL/load_data
curl -X POST https://TU_URL/industry_load
curl -X POST https://TU_URL/rotacion_sync
```

### Permisos requeridos (IAM)

- `roles/bigquery.dataEditor` - Escribir datos en BigQuery
- `roles/bigquery.jobUser` - Ejecutar trabajos de BigQuery

### Monitoreo

- Cloud Run
- Cloud Logging
- Cloud Monitoring

### Notas sobre los datos

La app normaliza nombres de columnas (minúsculas, sin acentos/espacios) y convierte tipos de fecha/número relevantes antes de cargar a BigQuery. Asegúrate de que la columna `dia` exista y contenga fechas válidas para el reemplazo por rango.
