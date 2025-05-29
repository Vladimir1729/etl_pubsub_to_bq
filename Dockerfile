# Usa una imagen base oficial de Python
#FROM python:3.10-slim
FROM python:3.12-slim-bookworm

# Establece el directorio de trabajo
WORKDIR /app

# Copia los archivos de tu aplicación
COPY requirements.txt ./
COPY main.py ./

# Instala las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto que usará Flask
EXPOSE 8080

# Comando para ejecutar la app
CMD ["python", "main.py"]
#FROM python:3.12-slim-bookworm


#WORKDIR /app
#COPY requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt

#COPY . .
#CMD ["python", "main.py"]
