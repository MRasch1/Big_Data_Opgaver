from pyspark import SparkContext, SparkConf
import string
import os

# Initialize SparkContext
conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)

def process_line(line):
    # Convert the text to lowercase
    line = line.lower()

    # Replace punctuation with spaces
    for punct in string.punctuation:
        line = line.replace(punct, ' ')

    # Split the line into words
    words = line.split()
    return words

# Input file path
input_filepath = 'Spark_WordCount/AChristmasCarol_CharlesDickens_English.txt'  # Replace with your input file path

# Read input file into an RDD
lines_rdd = sc.textFile(input_filepath)

# Perform word count
word_count_rdd = lines_rdd.flatMap(process_line) \
                          .map(lambda word: (word, 1)) \
                          .reduceByKey(lambda a, b: a + b)

# Collect the RDD to the driver
word_count_list = word_count_rdd.collect()

# Output file path
output_filepath = 'Spark_WordCount/output_spark.txt'

# Write word count results to the output file
with open(output_filepath, 'w') as f:
    for word, count in word_count_list:
        f.write(f'{word}: {count}\n')

# Stop SparkContext
sc.stop()

print(f'Word count saved to {output_filepath}')
