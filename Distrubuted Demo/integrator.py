from kafka import KafkaConsumer 
from kafka import KafkaProducer
import json
from fasta_reader import read_fasta
import time
import configparser 

config = configparser.ConfigParser()
config.read('config.ini')


result_file = config['INTEGRATOR']['result_file']  

bootstrap_servers = config['DEFAULT']['bootstrap_servers']  
topic = config['TOPIC']['result_topic']
timeout = float(config['INTEGRATOR']['timeout'])

File1 = config['PRODUCER']['query_data_file1']  
File2 = config['PRODUCER']['query_data_file2']

def process_message(message):
    # read from encoded msg
    data = json.loads(message)
    formatted_message = (
        f"SeqA: {data['query_key']}\n"
        f"SeqB: {data['target_key']}\n"
        f"Score: {data['score']}\n"
        f"Alignment Seq: {data['alignment']}\n"
        f"Cost Time: {data['cost_time']}\n\n"
    )
    key_pair = str(data['query_key']) + '_' + str(data['target_key'])
    return formatted_message, key_pair

def main():
    
    query_list = {}
    for item1 in read_fasta(File1):
        for item2 in read_fasta(File2):
            query_list[str(item1.defline) + '_' + str(item2.defline)] = [item1.value, item2.value]
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        group_id='group2'
    )
    answer_list = []
    last_updated_time = time.time()
    try:
        with open(result_file, 'w') as file:
            for message in consumer:
                message_value = message.value.decode('utf-8')
                formatted_message, key_pair = process_message(message_value)
                answer_list.append(key_pair)
                last_updated_time = time.time()
                file.write(formatted_message)
                file.flush()  # ensure recording each msg
                if len(query_list) == len(answer_list):
                    break
                else:
                    if time.time() - last_updated_time > 10:
                        break
                    else:
                        continue
    except KeyboardInterrupt:
        print("Terminated")
    finally:
        consumer.close()
        
    missing_msgs = []
    for msg in answer_list:
        if msg not in query_list:
            missing_msgs.append(msg)

    while len(missing_msgs) > 0:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        send_list = []
        for item in missing_msgs:
            send_list.append({'query_key':item.split('_')[0], 'query_value':query_list[item][0], 'target_key':item.split('_')[1], 'target_value':query_list[item][1]})
        try:
            for i in send_list:
                json_string = json.dumps(i)
                producer.send('Topic1', json_string.encode('utf-8'))
        finally:
            producer.close()

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id='group2'
        )
        
        last_updated_time = time.time()
        try:
            with open(result_file, 'w') as file:
                for message in consumer:
                    message_value = message.value.decode('utf-8')
                    formatted_message, key_pair = process_message(message_value)
                    missing_msgs.remove(key_pair)
                    last_updated_time = time.time()
                    file.write(formatted_message)
                    file.flush()  # ensure recording each msg
                    if len(missing_msgs) == 0:
                        break
                    else:
                        if time.time() - last_updated_time > 10:
                            break
                        else:
                            continue
        except KeyboardInterrupt:
            print("Terminated")
        finally:
            consumer.close()

    
if __name__ == "__main__":
    main()
