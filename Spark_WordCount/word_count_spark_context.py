from pyspark import SparkContext, SparkConf
import string

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

# Coalesce to a single partition
word_count_rdd = word_count_rdd.coalesce(1)

# Save the word count results to a text file
output_filepath = 'Spark_WordCount\output_spark'
word_count_rdd.saveAsTextFile(output_filepath)

# Stop SparkContext
sc.stop()

print(f'Word count saved to {output_filepath}')
