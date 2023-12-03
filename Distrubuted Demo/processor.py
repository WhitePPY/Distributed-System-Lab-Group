from pyspark.sql import SparkSession
import numpy as np
import time
from kafka import KafkaConsumer
import json
from kafka import KafkaProducer

def print_alignment(align1, align2):
    print(align1)
    print(align2)

def smith_waterman(A, B, match=2, mismatch=-1, gap=1):
    n, m = len(A), len(B)
    H = np.zeros((n+1, m+1), dtype=int)

    max_score = 0
    max_i, max_j = 0, 0

    for i in range(1, n+1):
        for j in range(1, m+1):
            score_diagonal = H[i-1, j-1] + (match if A[i-1] == B[j-1] else mismatch)
            score_up = H[i, j-1] + gap
            score_left = H[i-1, j] + gap
            H[i, j] = max(0, score_diagonal, score_up, score_left)

            if H[i, j] > max_score:
                max_score = H[i, j]
                max_i, max_j = i, j

    alignA, alignB = "", ""
    i, j = max_i, max_j

    while i > 0 and j > 0 and H[i, j] > 0:
        if H[i, j] == H[i-1, j-1] + (match if A[i-1] == B[j-1] else mismatch):
            alignA = A[i-1] + alignA
            alignB = B[j-1] + alignB
            i -= 1
            j -= 1
        elif H[i, j] == H[i, j-1] + gap:
            alignA = '-' + alignA
            alignB = B[j-1] + alignB
            j -= 1
        elif H[i, j] == H[i-1, j] + gap:
            alignA = A[i-1] + alignA
            alignB = '-' + alignB
            i -= 1

    return alignA, alignB, max_score

def main():
    topic1 = 'Topic1'
    topic2 = 'Topic2'
    spark = SparkSession.builder.appName("Smith-Waterman").getOrCreate()
    consumer = KafkaConsumer(
        topic1,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='group1'
    )

    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    try:
        for message in consumer:
            query_dict = json.loads(message.value.decode('utf-8'))

            start_time = time.time()
            alignA, alignB, max_score = smith_waterman(query_dict['query_value'], query_dict['target_value'])
            end_time = time.time()

            print("Alignment score:", max_score)
            final_alignment = alignA + alignB
            print("Run Time:", (end_time - start_time) * 1e6, "μs")

            # time.sleep(0.001)

            # send things to topic2
            result_dict = {'alignment': final_alignment, 'score': str(max_score), 'cost_time': str(end_time - start_time), 'query_key': query_dict['query_key'], 'target_key': query_dict['target_key']}
            producer.send(topic2, json.dumps(result_dict).encode('utf-8'))

    except KeyboardInterrupt:
        print("Terminated")
    finally:
        consumer.close()
        spark.stop()
        producer.close()

if __name__ == "__main__":
    main()