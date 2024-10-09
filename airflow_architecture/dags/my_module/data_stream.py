import json
from kafka import KafkaProducer

def stream_data(df):
    producer = KafkaProducer(bootstrap_servers='broker:29092',
                             max_block_ms=3000,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for row in df.iterrows():
        row_dict = row.to_json()
        producer.send('airflow-data-stream',value=row_dict)

    producer.flush()
    producer.close()
