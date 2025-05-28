import base64
import json
from flask import Flask, request
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

app = Flask(__name__)
client = bigquery.Client()


#Variables de configuracion de bigquery
project_id = 'tae-pagaqui-pro'
dataset_id = 'Pagaqui_historico'
table_name = "prueba_sub"
table_id = f'{project_id}.{dataset_id}.{table_name}'

#Esquema de la tabla
table_schema = [
    bigquery.SchemaField('ID', 'INT'),
    bigquery.SchemaField('DATE', 'STRING'),
    bigquery.SchemaField('FK_user', 'INT'),
    bigquery.SchemaField('FK_sku', 'INT'),
    bigquery.SchemaField('amount', 'FLOAT')
]


def ensure_table_exists():
    try:
        client.get_table(table_id)
    except NotFound:
        print(f'Tabla {table_id} no encontrada. Creando tabla...')
        table = bigquery.Table(table_id, schema = table_schema)
        table = client.create_table(table)
        print(f'Tabla {table_id} creada exitosamente.')



@app.route("/", methods = ['POST'])
def ingest():
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        return "Bad Request", 400
    
    try:
        #Decodificar y parsear el mensaje
        data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
        playload = json.loads(data)


        #Preparar la fila para insertar
        row = {
            'id' : playload['ID'],
            'date': str(playload['date']),
            'FK_users' : playload['FK_users'],
            'FK_sku': playload['FK_sku'],
            'amount': playload['amount']
        }

        #Verificamos si la tabla existe
        ensure_table_exists()

        #Insertar fila
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            print("Error inserting to Bigquery": errors)
            return "BQ Error", 500
        
        return "OK", 200
    
    except Exception as e:
        print("Unexpected error: ", e)
        return "Internal Server Error", 500