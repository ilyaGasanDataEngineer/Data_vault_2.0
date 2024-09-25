import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
import json
import socket
import psycopg2
import time
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    'owner': 'ilyagasan',
    'start_date': datetime(2024, 9, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def produce_to_kafka():
    def request_to_api():
        try:
            url = 'https://jsonplaceholder.typicode.com/posts/'
            response = requests.get(url)
            response.raise_for_status()
            result = response.json()
            return result
        except Exception as e:
            print(f'Cannot get data from REST API: {e}')
            return []

    config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
    }

    producer = Producer(config)
    try:
        for element in request_to_api():
            producer.produce(
                topic='jsonplaceholder_api',
                key=str(element['id']).encode('utf-8'),
                value=json.dumps(element)
            )
            print(f'Sent data with id: {element["id"]}')
        producer.flush()
    except Exception as e:
        print(f'Exception occurred while producing to Kafka: {e}')

def insert_data_to_stg(cursor, conn, buffer):
    try:
        cursor.executemany("""
            INSERT INTO stg_api_data (user_id, id, title, body, record_source)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, buffer)
        conn.commit()
        print(f"Inserted {len(buffer)} records into stg_api_data.")
    except Exception as e:
        print(f"Error during data insertion: {e}")
        conn.rollback()

def consume_from_kafka():
    conn = psycopg2.connect(
        dbname='tz_gazprom',
        host='localhost',
        port=5432,
        user='postgres',
        password='29892989'
    )
    cursor = conn.cursor()

    consumer = Consumer({
        'group.id': 'gazprom-consumer',
        'bootstrap.servers': 'localhost:9092',
        'enable.auto.commit': False,
        'client.id': socket.gethostname(),
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['jsonplaceholder_api'])

    buffer = []
    buffer_size = 100
    run = True
    empty_msg_count = 0
    empty_msg_limit = 10

    try:
        while run:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                empty_msg_count += 1
                if empty_msg_count >= empty_msg_limit:
                    run = False
                continue

            empty_msg_count = 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    run = False
                else:
                    raise KafkaException(msg.error())
            else:
                record = json.loads(msg.value().decode('utf-8'))
                buffer.append((
                    record['userId'],
                    record['id'],
                    record['title'],
                    record['body'],
                    'Kafka'
                ))
                print(f"Processed message id: {record['id']}")

                if len(buffer) >= buffer_size:
                    insert_data_to_stg(cursor, conn, buffer)
                    buffer = []

        if buffer:
            insert_data_to_stg(cursor, conn, buffer)

        conn.commit()

    except Exception as e:
        print(f"Error occurred: {e}")
        conn.rollback()
    finally:
        consumer.close()
        cursor.close()
        conn.close()

def transform_stg_to_dds():
    conn = psycopg2.connect(
        dbname='tz_gazprom',
        host='localhost',
        port=5432,
        user='postgres',
        password='29892989'
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT user_id
        FROM stg_api_data
        WHERE user_id IS NOT NULL;
    """)
    users = cursor.fetchall()

    cursor.executemany("""
        INSERT INTO dds_hub_user (user_id, load_date, record_source)
        VALUES (%s, NOW(), 'STG')
        ON CONFLICT (user_id) DO NOTHING;
    """, users)

    cursor.execute("""
        SELECT DISTINCT id
        FROM stg_api_data
        WHERE id IS NOT NULL;
    """)
    posts = cursor.fetchall()

    cursor.executemany("""
        INSERT INTO dds_hub_post (post_id, load_date, record_source)
        VALUES (%s, NOW(), 'STG')
        ON CONFLICT (post_id) DO NOTHING;
    """, posts)

    cursor.execute("""
        SELECT DISTINCT user_id, id
        FROM stg_api_data
        WHERE user_id IS NOT NULL AND id IS NOT NULL;
    """)
    user_post_links = cursor.fetchall()

    cursor.executemany("""
        INSERT INTO dds_link_user_post (user_id, post_id, load_date, record_source)
        VALUES (%s, %s, NOW(), 'STG')
        ON CONFLICT (user_id, post_id) DO NOTHING;
    """, user_post_links)

    cursor.execute("""
        SELECT id, title, body
        FROM stg_api_data
        WHERE id IS NOT NULL;
    """)
    post_details = cursor.fetchall()

    cursor.executemany("""
        INSERT INTO dds_sat_post (post_id, title, body, load_date, record_source)
        VALUES (%s, %s, %s, NOW(), 'STG')
        ON CONFLICT (post_id, load_date) DO NOTHING;
    """, post_details)

    conn.commit()
    cursor.close()
    conn.close()

with DAG(dag_id='gazprom_pipeline', default_args=DEFAULT_ARGS, schedule_interval='@daily', catchup=False) as dag:
    send_data_to_kafka_task = PythonOperator(
        task_id='send_data_to_kafka',
        python_callable=produce_to_kafka
    )

    consume_data_from_kafka_task = PythonOperator(
        task_id='consume_data_from_kafka',
        python_callable=consume_from_kafka
    )

    transform_stg_to_dds_task = PythonOperator(
        task_id='transform_stg_to_dds',
        python_callable=transform_stg_to_dds
    )

    send_data_to_kafka_task >> consume_data_from_kafka_task >> transform_stg_to_dds_task
