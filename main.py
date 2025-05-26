import base64
import json
from flask import Flask, request
from google.cloud import bigquery


app = Flask(__name__)
client = bigquery.Client()


@app.route("/", methods = ['POST'])
def ingest():
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        return "Bad Request", 400

    data = base64.b64decode(envelope["message"]["data"]).decode("utf-8")
    payload = json.loads(data)

    row = {
        'id': payload['ID'],
        'date': payload['date'],
        'FK_users': payload['FK_users'],
        'FK_sku': payload['FK_sku'],
        'amount': payload['amount']
    }


    table_id = ""
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        print("Error inserting to BigQuery", errors)
        return "BQ Error", 500
    
    return "OK", 200