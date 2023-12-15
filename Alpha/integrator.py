from kafka import KafkaConsumer
import json
from fasta_reader import read_fasta
import sys

def process_message(message):
    # 解析消息
    data = json.loads(message)
    formatted_message = (
        f"SeqA: {data['query_key']}\n"
        f"SeqB: {data['target_key']}\n"
        f"Score: {data['score']}\n"
        f"Alignment Seq: {data['alignment']}\n"
        f"Cost Time: {data['cost_time']}\n\n"
    )
    return formatted_message

def main():
    
    File1 = 'input.fna'
    File2 = 'GCF_001742465.1_ASM174246v1_genomic.fna'
    server_ip = '10.141.0.' + sys.argv[1][5:] + ':9092'
    key_num1  = 0
    for item1 in read_fasta(File1):
        for item2 in read_fasta(File2):
            key_num1+=1
    
    topic = 'Topic2'
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=server_ip,
        auto_offset_reset='earliest',
        group_id='group2'
    )
    key_num2 = 0

    try:
        with open('output.txt', 'w') as file:
            for message in consumer:
                key_num2+=1
                message_value = message.value.decode('utf-8')
                formatted_message = process_message(message_value)
                file.write(formatted_message)
                file.flush()  # 确保每条消息都立即写入文件
                # if key_num1 == key_num2:
                #     break
    except KeyboardInterrupt:
        print("Terminated")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
