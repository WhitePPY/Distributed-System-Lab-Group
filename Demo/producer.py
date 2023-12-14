from kafka import KafkaProducer
import time
from fasta_reader import read_fasta
import json

def file_to_kafka(topic, file_path):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    File1 = 'input.fna'
    File2 = './GCF_001742465.1_ASM174246v1_genomic.fna'
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
    topic_name = 'testTopic'
    file_path = 'ACGT.txt'
    file_to_kafka(topic_name, file_path)
