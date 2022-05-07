from collections import Counter

from tasks.io import find_files_for_task, make_text_generator, write_output


def word_count_reduce(
    input_path, output_path, input_file_names, output_file_name="out",
    job_id=0, merge_join=False
):
    files_for_task = find_files_for_task(
        input_path, input_file_names, job_id
    )
    words = make_text_generator(files_for_task, merge_join)
    counter_items = count(words, merge_join)
    write_output(
        items_to_str(counter_items), f"{output_path}{output_file_name}-{job_id}"
    )
    

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
                assert word > current_word, \
                        "using flag merge_join=True: input should be sorted"
                yield (current_word, n)
                current_word = word
                n = 1
        yield (current_word, n)

    else:
        counter = Counter(words)
        for item in counter.items():
            yield item


def items_to_str(items):
    for word, n in items:
        yield f"{word} {n}"
