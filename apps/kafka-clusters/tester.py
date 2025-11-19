import time
from kafka import KafkaProducer, KafkaConsumer

# Kafka bootstrap server (your port-forwarded host)
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'my-topic'

# 1️⃣ Create producer
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

# 2️⃣ Get input from user
message = input("Enter a message to send to Kafka: ")

# 3️⃣ Send message
producer.send(TOPIC, message.encode('utf-8'))
producer.flush()
print(f"Sent: {message}")

# 4️⃣ Wait 5 seconds
time.sleep(5)

# 5️⃣ Create consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000  # stop after 5 seconds if no messages
)

print("Consuming messages from topic:")
for msg in consumer:
    print(msg.value.decode('utf-8'))

consumer.close()
producer.close()