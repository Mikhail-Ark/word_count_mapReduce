from re import findall

from tasks.io import make_text_generator, write_output


def word_count_map(
    input_file_path, output_path, job_id=0,
    ignore_case=True, sort=False, n_buckets=1
):
    raw_lines = make_text_generator(input_file_path)
    tokenized_text = tokenize_lines(raw_lines, ignore_case)
    if sort:
        tokenized_text = sorted(tokenized_text)
    write_output(tokenized_text, f"{output_path}-{job_id}", n_buckets)


def tokenize_lines(raw_lines, ignore_case=True):
    for line in raw_lines:
        line_words = tokenize(line, ignore_case)
        for word in line_words:
            yield word


def tokenize(line, ignore_case=True):
    if ignore_case:
        line = line.lower()
    return findall(r"\w+", line)
