from kafka import KafkaProducer
import time
from fasta_reader import read_fasta
import json
import configparser 

config = configparser.ConfigParser()
config.read('config.ini')

topic = config['TOPIC']['query_topic']
File1 = config['PRODUCER']['query_data_file1']  
File2 = config['PRODUCER']['query_data_file2']

bootstrap_servers = config['DEFAULT']['bootstrap_servers']

def file_to_kafka(topic):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    
    send_list = []

    for item1 in read_fasta(File1):
        for item2 in read_fasta(File2):
            send_list.append({'query_key':item1.defline, 'query_value':item1.sequence, 'target_key':item2.defline, 'target_value':item2.sequence})


    try:
        for i in send_list:

            #producer.send(topic, line.encode('utf-8'))
            json_string = json.dumps(i)
            producer.send(topic, json_string.encode('utf-8'))
            # time.sleep(1)  # 模拟延迟
    finally:
        producer.close()

if __name__ == '__main__':
        file_to_kafka(topic)