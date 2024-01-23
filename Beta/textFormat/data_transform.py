import re
import sys

def extract_info(regex, input_file, output_file):
    index = 0
    try:
        # read input file
        with open(input_file, 'r') as file:
            content = file.read()

        #  use regex to extract information
        matches = re.findall(regex, content)

        print(matches)

        # output to file
        with open(output_file, 'w') as output_file:
            for match in matches:
                index += 1
                output_file.write('>' + input_file + '_000'+ str(index) + '\n' + match + '\n')

        print(f"data has been extracted from '{input_file}'，and has been written to '{output_file}'.")

    except FileNotFoundError:
        print(f"can't find file: {input_file}")
    except Exception as e:
        print(f"error: {e}")

if __name__ == "__main__":
    # check command line arguments
    if len(sys.argv) != 4:
        print("Usage: python script.py <regex expression> <input_filename> <output_filename>")
        sys.exit(1)

    # extract command line arguments
    regex = sys.argv[1]
    input_file = sys.argv[2]
    output_file = sys.argv[3]

    # use regex to extract information
    extract_info(regex, input_file, output_file)
