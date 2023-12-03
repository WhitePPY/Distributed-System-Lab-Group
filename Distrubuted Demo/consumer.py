from pyspark.sql import SparkSession
import numpy as np
import time
from kafka import KafkaConsumer

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
    spark = SparkSession.builder.appName("Smith-Waterman").getOrCreate()

    # Fixed sequence
    seq1 = "GCTGCATGATATTGAAAAAATATCACCAAATAAAAAACGCCTTAGTAAGTATTTTTCAGCTTTTCATTCTGACTGCAACGGGCAATATGTCTCTGTGTGGATTAAAAAAGAGTGTCTGATAGCAGCTTCTGAACTGGTTACCTGCCGTGAGTAAATTAAAATTTTATTGACTTAGGTCACTAAATACTTTAACCAATATAGGCATAGCGCACAGACAGATAAAAATTACAGAGTACACAACATCCATGAAACGCATTAGCACCACCATTACCACCACCATCACCATTACCACAGGTAACGGTGCGGGCTGACGACGTACAGGAAACACAGAAAAAAGCCCGCTAC"

    topic_name = 'testTopic'
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='my-group'
    )

    try:
        for message in consumer:
            seq2 = message.value.decode('utf-8')
            
            start_time = time.time()
            alignA, alignB, max_score = smith_waterman(seq1, seq2)
            end_time = time.time()

            print("Alignment score:", max_score)
            print_alignment(alignA, alignB)
            print("Run Time:", (end_time - start_time) * 1e6, "μs")
            
            # time.sleep(0.001)
    except KeyboardInterrupt:
        print("Terminated")
    finally:
        consumer.close()
        spark.stop()

if __name__ == "__main__":
    main()
