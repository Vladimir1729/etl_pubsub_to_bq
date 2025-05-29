import base64
import json
from flask import Flask, request
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime

app = Flask(__name__)
client = bigquery.Client()

# Variables de configuración de BigQuery
project_id = 'tae-pagaqui-pro'
dataset_id = 'Pagaqui_historico'
table_name = "prueba_suv"
table_id = f'{project_id}.{dataset_id}.{table_name}'

# Esquema de la tabla
table_schema = [
    bigquery.SchemaField('ID', 'INT64'),
    bigquery.SchemaField('DATE', 'STRING'),
    bigquery.SchemaField('FK_user', 'INT64'),
    bigquery.SchemaField('FK_sku', 'INT64'),
    bigquery.SchemaField('amount', 'FLOAT64')
]

def ensure_table_exists():
    try:
        client.get_table(table_id)
    except NotFound:
        print(f'Tabla {table_id} no encontrada. Creando tabla...')
        table = bigquery.Table(table_id, schema=table_schema)
        table = client.create_table(table)
        print(f'Tabla {table_id} creada exitosamente.')

@app.route("/", methods=['GET', 'POST'])
def ingest():
    if request.method == 'GET':
        return "Servicio Flask activo y esperando mensajes POST desde Pub/Sub", 200
    
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        return "Bad Request", 400

    try:
        # Decodificar y parsear el mensaje
        data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
        print("📦 Payload recibido (string):", data)

        payload = json.loads(data)
        print("📦 Payload JSON:", payload)

        # Convertir timestamp a string con formato de fecha legible
        date_str = datetime.utcfromtimestamp(payload['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

        # Preparar la fila para insertar
        row = {
            'ID': payload['ID'],
            'DATE': date_str,
            'FK_user': payload['FK_users'],
            'FK_sku': payload['FK_sku'],
            'amount': payload['amount']
        }
        '''
        row = {
            'ID': payload['ID'],
            'DATE': str(payload['date']),
            'FK_user': payload['FK_users'],
            'FK_sku': payload['FK_sku'],
            'amount': payload['amount']
        }
        '''

        # Verificamos si la tabla existe
        ensure_table_exists()

        # Insertar fila
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            print("Error inserting to BigQuery:", errors)
            return "BQ Error", 500

        return "OK", 200

    except Exception as e:
        print("Unexpected error:", e)
        return "Internal Server Error", 500

# 🔽 Esto es lo que permite a Cloud Run arrancar correctamente el servicio
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
