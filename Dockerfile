# Usar imagen base de Python
FROM python:3.9-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar requirements e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código de la aplicación
COPY . .

# Exponer puerto (Cloud Run usa 8080 por defecto)
EXPOSE 8080

# Comando para ejecutar la aplicación
CMD ["python", "main.py"]