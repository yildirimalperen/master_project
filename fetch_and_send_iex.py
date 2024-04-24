import requests
import pika
import time
import json

def fetch_data():
    API_URL = "https://cloud.iexapis.com/stable/stock/market/batch"
    API_KEY = "your_iex_cloud_api_key"
    SYMBOLS = "AAPL,FB,GOOGL"

    data = {
        "symbols": SYMBOLS,
        "types": "quote",
        "token": API_KEY
    }

    response = requests.get(API_URL, params=data)
    return response.json()

def send_to_rabbitmq(data):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='iex_queue', durable=True)
    
    message = json.dumps(data)
    channel.basic_publish(
        exchange='',
        routing_key='iex_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,
        ))

    print(f"Sent data to RabbitMQ on 'iex_queue': {message}")
    connection.close()

def main():
    while True:
        data = fetch_data()
        send_to_rabbitmq(data)
        time.sleep(300)

if __name__ == '__main__':
    main()
