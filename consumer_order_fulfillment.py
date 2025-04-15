# consumer_order_fulfillment.py
import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME


def fulfill_order(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]
    STUDENT_NAME=data["student_name"]

    print(f"[{STUDENT_NAME}] Fulfillment: Order {order_id} fulfilled. Events published.")

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Publish order-fulfilled event
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="order-fulfilled", body=json.dumps(data))

    connection.close()

def start_fulfillment_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="fulfillment_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="fulfillment_queue", routing_key="payment-applied")

    channel.basic_consume(queue="fulfillment_queue", on_message_callback=fulfill_order, auto_ack=True)

    print("Waiting for fulfillment messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_fulfillment_consumer()
