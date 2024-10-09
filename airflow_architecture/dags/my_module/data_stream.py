import json
from kafka import KafkaProducer
import pandas as pd

def serialize_record(record):
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):
            record[key] = value.timestamp()
    return record
def stream_data(df):
    producer = KafkaProducer(bootstrap_servers='broker:29092',
                             max_block_ms=3000,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    records = df.to_dict(orient='records')
    for record in records:
        for_record = serialize_record(record)
        producer.send('airflow-data-stream',value=for_record)

    producer.flush()
    producer.close()
