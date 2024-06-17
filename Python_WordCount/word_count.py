from collections import Counter
import string
from pathlib import Path

def count_words(text):
    # Convert the text to lowercase
    text = text.lower()

    # Replace punctuation with spaces
    for punct in string.punctuation:
        text = text.replace(punct, ' ')

    # Split the text into words
    words = text.split()

    # Use Counter to count the words
    word_count = Counter(words)

    return word_count

def count_words_from_file(filepath):
    path = Path(filepath)
    if not path.is_file():
        raise FileNotFoundError(f"The file {filepath} does not exist.")
    
    with path.open('r', encoding='utf-8') as file:
        text = file.read()
    return count_words(text)

def save_word_count_to_file(word_count, output_filepath):
    total_words = sum(word_count.values())  # Calculate total number of words

    with open(output_filepath, 'w', encoding='utf-8') as file:
        file.write(f'Total Words: {total_words}\n\n')  # Write total word count to file
        for word, count in word_count.most_common():
            file.write(f'{word}: {count}\n')

# Count words from a file
input_filename = 'Python_WordCount/AChristmasCarol_CharlesDickens_English.txt'  # Replace with your input file path
output_filename = 'Python_WordCount/output.txt'  # Replace with your output file path

word_count = count_words_from_file(input_filename)
save_word_count_to_file(word_count, output_filename)

print(f'Word count saved to {output_filename}')
