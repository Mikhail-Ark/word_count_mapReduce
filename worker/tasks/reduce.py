from collections import Counter
from os import listdir

from worker.utils import make_text_generator, write_output


def word_count_reduce(input_path, output_path, job_id, merge_sort=False):
    input_files_paths = find_files_to_reduce(input_path, job_id)
    words = make_text_generator(input_files_paths, merge_sort)
    counter_items = count(words, merge_sort)
    write_output(items_to_str(counter_items), f"{output_path}-{job_id}")
    

def find_files_to_reduce(input_path, job_id=0):
    file_name_ending = f"-{job_id}"
    return [
        input_path + file_name for file_name in listdir(input_path)
        if file_name.endswith(file_name_ending)
    ]


def count(words, sorted=False):
    if sorted:
        try:
            current_word = next(words)
        except StopIteration:
            return []
        if not current_word:
            return []
        n = 1
        for word in words:
            if word == current_word:
                n += 1
            else:
                yield (current_word, n)
                current_word = word
                n = 1
        yield (current_word, n)

    else:
        counter = Counter(words)
        return (item for item in counter.items())


def items_to_str(items):
    return (f"{word} {n}" for word, n in items)
