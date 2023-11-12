1. two grade fuctions
- $s(a,b)$ - Similarity score of the elements that constituted the two sequences
- $W_{k}$ - The penalty of a gap that has length k

2. Construct a scoring matrix H and initialize its first row and first column.
   
4. Fill the scoring matrix using the equation below.
$$
H_{i,j} = \max
\left\{
\begin{matrix}
H_{i-1,j-1} + s(a_i, b_j) \\
\max_{k \geq 1}\{H_{i-k,j} - W_k\} \\
\max_{l \geq 1}\{H_{i,j-l} - W_l\} \\
0 \\
\end{matrix}
\right.
$$
4. Traceback. Starting at the highest score in the scoring matrix H and ending at a matrix cell that has a score of 0, traceback based on the source of each score recursively to generate the best local alignment.
