import requests
import pika
import time
import json

def fetch_data(symbols):
    API_URL = "https://www.alphavantage.co/query"
    API_KEY = "TQVSJ9YAEURFN7I1"  # Alpha Vantage API Key
    data = {}

    for symbol in symbols:
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "5min",
            "apikey": API_KEY
        }
        
        response = requests.get(API_URL, params=params)
        if response.status_code == 200:
            json_response = response.json()
            data[symbol] = json_response.get('Time Series (5min)', {})
            print(f"Data fetched for {symbol}: {list(data[symbol].items())[0]}")  # Print the most recent data
        else:
            print(f"Failed to fetch data for {symbol}. HTTP Status Code: {response.status_code}")
    
    return data

def send_to_rabbitmq(data):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='alpha_queue', durable=True)
    
    message = json.dumps(data)
    channel.basic_publish(
        exchange='',
        routing_key='alpha_queue',
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        ))

    print(f"Sent data to RabbitMQ on 'alpha_queue': {message}")
    connection.close()

def main():
    symbols = ["IBM", "TSLA", "AAPL", "MSFT"]  # List of stock symbols to fetch

    while True:
        data = fetch_data(symbols)
        send_to_rabbitmq(data)
        time.sleep(30)  # Fetch data every 5 minutes

if __name__ == '__main__':
    main()
