from pyspark.sql import SparkSession
import numpy as np
import time
from kafka import KafkaConsumer
import json
from kafka import KafkaProducer
import heapq

def print_alignment(align1, align2):
    print(align1)
    print(align2)

def smith_waterm_topk(A, B, k = 1, match=2, mismatch=-1, gap=1):
    n, m = len(A), len(B)
    H = np.zeros((n+1, m+1), dtype=int)

    topk_list = []

    # start_time = time.time()
    for i in range(1, n+1):
        for j in range(1, m+1):
            score_diagonal = H[i-1, j-1] + (match if A[i-1] == B[j-1] else mismatch)
            score_up = H[i, j-1] + gap
            score_left = H[i-1, j] + gap
            H[i, j] = max(0, score_diagonal, score_up, score_left)

            if H[i, j] > 0:
                if len(topk_list) < k:
                    heapq.heappush(topk_list, (H[i, j], i, j))
                else:
                    heapq.heappushpop(topk_list, (H[i, j], i, j))

    topk_list.sort(key=lambda x: x[0], reverse=True)
    # end_time = time.time()
    # print("Run Time:", (end_time - start_time) * 1000, "ms")

    aligned_list = []
    for result in topk_list:
        score, i, j = result
        alignA = ""
        alignB = ""
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
        aligned_list.append((score, alignA, alignB))
    return aligned_list

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
            #! 在什么时候允许用户输入 k 比较合适
            align_list = smith_waterm_topk(query_dict['query_value'], query_dict['target_value'])
            end_time = time.time()

            print("Run Time:", (end_time - start_time) * 1e6, "μs")
            for item in align_list:
                score, alignA, alignB = item
                print("Alignment score:", score)
                final_alignment = alignA + alignB
                #! 如果是k个的话 cost_time 应该是一致的
                result_dict = {'alignment': final_alignment, 'score': str(score), 'cost_time': str(end_time - start_time), 'query_key': query_dict['query_key'], 'target_key': query_dict['target_key']}
                producer.send(topic2, json.dumps(result_dict).encode('utf-8'))

    except KeyboardInterrupt:
        print("Terminated")
    finally:
        consumer.close()
        spark.stop()
        producer.close()

if __name__ == "__main__":
    main()