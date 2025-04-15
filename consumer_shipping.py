# consumer_shipping.py
import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME



def ship_order(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]
    STUDENT_NAME = data["student_name"]

    print(f"[{STUDENT_NAME}] Shipping: Order {order_id} shipped. Events published.")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Publish order-shipped event
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="order-shipped", body=json.dumps(data))

    connection.close()

def start_shipping_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="shipping_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="shipping_queue", routing_key="order-fulfilled")

    channel.basic_consume(queue="shipping_queue", on_message_callback=ship_order, auto_ack=True)

    print("Waiting for shipping messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_shipping_consumer()
