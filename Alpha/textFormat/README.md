#### this dir is for users to convinently transform their data into the format our framework is using

usage:  python .\data_transform.py (regex expression) (input_filename) (output_filename)



example : python .\data_transform.py ':(.*?)(?:\n|$)' .\data_input.txt output.fna

input:
```
seq1:ACTACTTATTCATTATC
seq2:ACTACTTATTCATTATT
seq3:ACTACTTATTCATTATA
seq4:ACTACTTATTCATTATG
```
output:
```
>.\data_input.txt_0001
ACTACTTATTCATTATC
>.\data_input.txt_0002
ACTACTTATTCATTATT
>.\data_input.txt_0003
ACTACTTATTCATTATA
>.\data_input.txt_0004
ACTACTTATTCATTATG
```
