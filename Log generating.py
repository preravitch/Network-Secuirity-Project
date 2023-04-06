import json
import random
import time
from kafka import KafkaProducer

# Replace with your Kafka server's address
kafka_server = 'your.kafka.server:9092'
# Replace with your Kafka topic
kafka_topic = 'your-kafka-topic'

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Define a function to generate simulated log data
def generate_log_data():
    src_ips = ['192.168.1.2', '192.168.1.3', '192.168.1.4']
    dest_ips = ['192.168.1.10', '192.168.1.11', '192.168.1.12']
    src_ports = [1111, 2222, 3333]
    dest_ports = [80, 443, 8080]
    protocols = ['TCP', 'UDP']
    requests = ['GET /index.html', 'POST /login', 'GET /download']
    attack_types = ['Network Sniffing', 'Unsecured Credential', 'Credential Leakage', 'None']

    log_data = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'src_ip': random.choice(src_ips),
        'dest_ip': random.choice(dest_ips),
        'src_port': random.choice(src_ports),
        'dest_port': random.choice(dest_ports),
        'protocol': random.choice(protocols),
        'request': random.choice(requests),
        'attack_type': random.choice(attack_types),
    }

    return log_data

# Generate and send simulated log data to Kafka
while True:
    log_data = generate_log_data()
    producer.send(kafka_topic, log_data)
    print(f'Sent log data: {log_data}')
    time.sleep(random.uniform(0.5, 2))  # Adjust the sleep time to control the rate of log generation
