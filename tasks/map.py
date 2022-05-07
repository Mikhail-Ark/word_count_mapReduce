from re import findall

from tasks.io import find_files_for_task, make_text_generator, write_output


def word_count_map(
    input_path, output_path, input_file_names=None, output_file_name="mr",
    job_id=0, n_buckets=1, ignore_case=False, sort=False
):
    files_for_task = find_files_for_task(input_path, input_file_names)
    raw_lines = make_text_generator(files_for_task)
    tokenized_text = tokenize_lines(raw_lines, ignore_case)
    if sort:
        tokenized_text = sorted(tokenized_text)
    write_output(
        tokenized_text, f"{output_path}{output_file_name}-{job_id}", n_buckets
    )


def tokenize_lines(raw_lines, ignore_case=True):
    for line in raw_lines:
        line_words = tokenize(line, ignore_case)
        for word in line_words:
            yield word


def tokenize(line, ignore_case=True):
    if ignore_case:
        line = line.lower()
    return findall(r"\w+", line)
