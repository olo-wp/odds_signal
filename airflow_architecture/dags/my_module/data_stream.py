import json
from kafka import KafkaProducer

def stream_data(df):
    df_json = df.to_json(orient='records')
    producer = KafkaProducer(bootstrap_servers='broker:29092', max_block_ms=3000,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('airflow-data-stream',value=df_json)
    producer.flush()
