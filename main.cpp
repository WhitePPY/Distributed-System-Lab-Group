#include <iostream>
#include <vector>
#include <algorithm>
#include <string>

// Initialize the scoring parameters
const int match = 1;
const int mismatch = -1;
const int gap = -1;

// Function to print the alignment
void print_alignment(const std::string& align1, const std::string& align2) {
    std::cout << align1 << std::endl;
    std::cout << align2 << std::endl;
}

// The Smith-Waterman algorithm function
void smith_waterman(const std::string& A, const std::string& B) {
    int n = A.length();
    int m = B.length();

    // Initialize the score matrix
    std::vector<std::vector<int>> H(n + 1, std::vector<int>(m + 1, 0));
    std::vector<std::vector<int>> backtrack(n + 1, std::vector<int>(m + 1, 0));

    int max_score = 0;
    int max_i = 0;
    int max_j = 0;

    // Fill the score matrix and keep track of the backtracking
    for (int i = 1; i <= n; ++i) {
        for (int j = 1; j <= m; ++j) {
            int score_diagonal = H[i - 1][j - 1] + (A[i - 1] == B[j - 1] ? match : mismatch);
            int score_up = H[i][j - 1] + gap;
            int score_left = H[i - 1][j] + gap;
            H[i][j] = std::max({ 0, score_diagonal, score_up, score_left });

            // Keep track of the maximum score and its position
            if (H[i][j] > max_score) {
                max_score = H[i][j];
                max_i = i;
                max_j = j;
            }

            // Backtracking information
            if (H[i][j] == 0)
                backtrack[i][j] = 0; // 0 means the alignment ends here
            else if (H[i][j] == score_diagonal)
                backtrack[i][j] = 1; // 1 means it comes from diagonal
            else if (H[i][j] == score_up)
                backtrack[i][j] = 2; // 2 means it comes from up
            else if (H[i][j] == score_left)
                backtrack[i][j] = 3; // 3 means it comes from left
        }
    }

    // Start backtracking from the max score position
    std::string alignA, alignB;
    int i = max_i;
    int j = max_j;

    while (i > 0 && j > 0 && H[i][j] > 0) {
        if (backtrack[i][j] == 1) {
            alignA = A[i - 1] + alignA;
            alignB = B[j - 1] + alignB;
            --i; --j;
        }
        else if (backtrack[i][j] == 2) {
            alignA = '-' + alignA;
            alignB = B[j - 1] + alignB;
            --j;
        }
        else if (backtrack[i][j] == 3) {
            alignA = A[i - 1] + alignA;
            alignB = '-' + alignB;
            --i;
        }
    }

    // Print the alignment
    std::cout << "Alignment score: " << max_score << std::endl;
    print_alignment(alignA, alignB);
}

int main() {
    std::string seq1, seq2;
    while (true)
    {
        // Get the sequences from the user
        std::cout << "Enter first sequence: ";
        std::cin >> seq1;
        std::cout << "Enter second sequence: ";
        std::cin >> seq2;

        // Perform the Smith-Waterman algorithm
        smith_waterman(seq1, seq2);
    }

    return 0;
}
