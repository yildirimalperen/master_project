import pika
import psycopg2
import json
def get_db_connection():
    return psycopg2.connect( dbname='postgres', user='postgres', password='', host='127.0.0.1', port='5433')

def setup_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS finance_records_new ( id SERIAL PRIMARY KEY, source TEXT NOT NULL, data JSONB NOT NULL );
    """)
    conn.commit()
    conn.close()

def callback(ch, method, properties, body):
    conn = get_db_connection()
    cursor = conn.cursor()

    data = json.loads(body.decode('utf-8'))  # Decode byte type to string and convert to JSON
    source = 'Alpha Vantage'  # Source can be dynamically set based on the queue or data type

    print(f"Received data from {source}: {data}")
    cursor.execute(
        "INSERT INTO finance_records_new (source, data) VALUES (%s, %s)",
        (source, json.dumps(data))  # Convert the JSON back to a string to store in JSONB column
    )
    conn.commit()
    cursor.close()
    conn.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    setup_database()  # Ensure the database and table are setup

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='alpha_queue', durable=True)
    
    channel.basic_consume(queue='alpha_queue', on_message_callback=callback, auto_ack=False)

    print('Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print('Interrupted')
        channel.stop_consuming()
        connection.close()

if __name__ == '__main__':
    main()
