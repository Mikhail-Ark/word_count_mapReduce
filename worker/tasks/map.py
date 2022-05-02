from re import findall


def make_text_generator(input_fp):
    with open(input_fp, "r") as file:
        for line in file:
            yield line


def write_output(words, output_file_path, n_buckets=1):
    files = [None] * n_buckets
    for word in words:
        bucket = which_bucket(word, n_buckets)
        if files[bucket] is None:
            files[bucket] = open(f"{output_file_path}-{bucket}", "w")
        file = files[bucket]
        file.write(word)
        file.write("\n")
    for file in files:
        if file is not None:
            file.close()


def which_bucket(word, n_buckets):
    return ord(word[0]) % n_buckets


def word_count_map(
    input_file_path, output_file_path, sort=False, n_buckets=1
):
    raw_lines = make_text_generator(input_file_path)
    tokenized_text = tokenize_lines(raw_lines)
    if sort:
        tokenized_text = sorted(tokenized_text)
    write_output(tokenized_text, output_file_path, n_buckets)


def tokenize_lines(raw_lines, ignore_case=True):
    for line in raw_lines:
        line_words = tokenize(line, ignore_case)
        for word in line_words:
            yield word


def tokenize(line, ignore_case=True):
    if ignore_case:
        line = line.lower()
    return findall(r"\w+", line)
