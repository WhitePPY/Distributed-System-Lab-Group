from kafka import KafkaProducer
import time
from fasta_reader import read_fasta
import json
import sys

def file_to_kafka(topic):
    server_ip = '10.141.0.' + sys.argv[1][5:] + ':9092'
    producer = KafkaProducer(bootstrap_servers=server_ip)
    File1 = '/home/dsys2313/slurm_test/input.fna'
    File2 = '/home/dsys2313/slurm_test/GCF_001742465.1_ASM174246v1_genomic.fna'
    send_list = []

    for item1 in read_fasta(File1):
        for item2 in read_fasta(File2):
            send_list.append({'query_key':item1.defline, 'query_value':item1.sequence, 'target_key':item2.defline, 'target_value':item2.sequence})


    try:
        for i in send_list:
            print(i)

            #producer.send(topic, line.encode('utf-8'))
            json_string = json.dumps(i)
            producer.send(topic, json_string.encode('utf-8'))
            # time.sleep(1)  # 模拟延迟
    finally:
        producer.close()

if __name__ == '__main__':
    topic_name = 'Topic1'
    file_to_kafka(topic_name)
