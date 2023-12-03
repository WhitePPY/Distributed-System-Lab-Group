from kafka import KafkaProducer
import time

def file_to_kafka(topic, file_path):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    try:
        with open(file_path, 'r') as file:
            for line in file:
                cleaned_line = line.rstrip()
                producer.send(topic, cleaned_line.encode('utf-8'))
    finally:
        producer.close()

if __name__ == '__main__':
    topic_name = 'testTopic'
    file_path = 'ACGT.txt'
    file_to_kafka(topic_name, file_path)
