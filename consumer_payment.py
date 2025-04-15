# consumer_payment.py
import pika
import json
import random
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def process_payment(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]
    student_name = data["student_name"]

    payment_success = random.choice([True, False])

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    if payment_success:
        channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="payment-applied", body=json.dumps(data))
        channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="payment-success", body=json.dumps(data))
        print(f"[{student_name}] Payment applied for Order {order_id}, events sent.")
    else:
        channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="payment-denied", body=json.dumps(data))
        print(f"[{student_name}] Payment denied for Order {order_id}, notification sent.")

    connection.close()

def start_payment_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="payment_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="payment_queue", routing_key="order-created")

    channel.basic_consume(queue="payment_queue", on_message_callback=process_payment, auto_ack=True)

    print("Waiting for payment messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_payment_consumer()
