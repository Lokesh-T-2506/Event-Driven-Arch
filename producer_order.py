import pika
import json
import random
import uuid
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def generate_random_order():
    order_id = str(uuid.uuid4())[:8] 
    user_id = f"u{random.randint(100, 999)}" 
    book_id = f"b{random.randint(1000, 9999)}"
    student_name = "Lokesh Talluri" 

    return {
        "order_id": order_id,
        "user_id": user_id,
        "book_id": book_id,
        "student_name": student_name
    }

def place_order():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct')

    order = generate_random_order()

    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="order-created", body=json.dumps(order))
    print(f"[{order['student_name']}] Producer: Order placed: {order}")

    connection.close()

if __name__ == "__main__":
    place_order()
