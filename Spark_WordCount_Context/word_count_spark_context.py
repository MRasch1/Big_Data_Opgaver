from pyspark import SparkContext

from collections import Counter

import string




def count_words(text):

    """Count the number of words in the given text."""

    words = text.split()

    return len(words)




def count_word_frequencies(text):

    """Count the frequencies of each word in the given text."""

    words = text.split()

    return Counter(words)




def write_output(output_file_path, word_count, word_frequencies):

    """Write the word count and frequencies to an output file."""

    with open(output_file_path, 'w', encoding='utf-8') as file:

        file.write(f"Total number of words: {word_count}\n")

        file.write("Word frequencies:\n")

        for word, count in sorted(word_frequencies.items()):

            file.write(f"{word}: {count}\n")




def clean_text(text):

    """Convert text to lowercase and replace punctuation with spaces."""

    text = text.lower()

    for punct in string.punctuation:

        text = text.replace(punct, ' ')

    return text




def main():

    # Initialize SparkContext
    sc = SparkContext("local", "WordCount")

    # Define the path to the input and output files

    input_file_path = 'Spark_WordCount_Context\AChristmasCarol_CharlesDickens_English.txt'

    output_file_path = 'Spark_WordCount_Context\output_spark.txt'

    

    # Read the text file into an RDD

    text_file = sc.textFile(input_file_path)

    

    # Collect the RDD into a list of lines

    lines = text_file.collect()

    

    # Join the lines into a single string

    text = "\n".join(lines)

    

    # Clean the text

    cleaned_text = clean_text(text)

    

    # Use the count_words function to count words

    word_count = count_words(cleaned_text)

    

    # Use the count_word_frequencies function to count word frequencies

    word_frequencies = count_word_frequencies(cleaned_text)

    

    # Print the word count to the console

    print(f"Total number of words: {word_count}")

    

    # Print the word frequencies to the console

    for word, count in sorted(word_frequencies.items()):

        print(f"{word}: {count}")

    

    # Write the word count and frequencies to the output file

    write_output(output_file_path, word_count, word_frequencies)

    

    # Stop the SparkContext

    sc.stop()




if __name__ == "__main__":

    main()