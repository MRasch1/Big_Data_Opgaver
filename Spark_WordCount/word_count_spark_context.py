from pyspark.sql import SparkSession
import string

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Word Count") \
    .getOrCreate()

def process_line(line):
    # Convert the text to lowercase
    line = line.lower()

    # Replace punctuation with spaces
    for punct in string.punctuation:
        line = line.replace(punct, ' ')

    # Split the line into words
    words = line.split()
    return words

def count_words(input_filepath, output_filepath):
    # Read the text file into an RDD
    text_rdd = spark.sparkContext.textFile(input_filepath)
    
    # Process each line to remove punctuation and split into words
    words_rdd = text_rdd.flatMap(process_line)
    
    # Map each word to a tuple (word, 1)
    word_tuples_rdd = words_rdd.map(lambda word: (word, 1))
    
    # Reduce by key to count the occurrences of each word
    word_count_rdd = word_tuples_rdd.reduceByKey(lambda a, b: a + b)
    
    # Sort the results by word frequency in descending order
    sorted_word_count_rdd = word_count_rdd.sortBy(lambda x: x[1], ascending=False)
    
    # Save the results to a text file
    sorted_word_count_rdd.saveAsTextFile(output_filepath)

# Input and output file paths
input_filename = 'Spark_WordCount/AChristmasCarol_CharlesDickens_English.txt'  # Replace with your input file path
output_filename = 'Spark_WordCount/output_spark.txt'  # Replace with your output file path (directory)

# Perform the word count
count_words(input_filename, output_filename)

# Stop the Spark session
spark.stop()

print(f'Word count saved to {output_filename}')
