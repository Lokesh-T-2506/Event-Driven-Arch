# consumer_notification.py
import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def send_notification(ch, method, properties, body):
    data = json.loads(body)
    order_id = data["order_id"]
    student_name = data["student_name"]
    
    if method.routing_key == "order-created":
        message = f"[{student_name}] Notification: Order {order_id} has been placed successfully."
    elif method.routing_key == "payment-success":
        message = f"[{student_name}] Notification: Payment for Order {order_id} was successful."
    elif method.routing_key == "payment-denied":
        message = f"[{student_name}] Notification: Payment for Order {order_id} was denied."
    elif method.routing_key == "order-fulfilled":
        message = f"[{student_name}] Notification: Order {order_id} has been fulfilled."
    elif method.routing_key == "order-shipped":
        message = f"[{student_name}] Notification: Order {order_id} has been shipped."
    else:
        message = "Unknown notification event received."

    print(message)

def start_notification_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue="notification_queue")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key="order-created")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key="payment-success")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key="payment-denied")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key="order-fulfilled")
    channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key="order-shipped")

    channel.basic_consume(queue="notification_queue", on_message_callback=send_notification, auto_ack=True)

    print("Waiting for notification messages...")
    channel.start_consuming()

if __name__ == "__main__":
    start_notification_consumer()
