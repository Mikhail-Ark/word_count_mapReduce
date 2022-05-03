from itertools import chain


def make_text_generator(input_file_path, merge_sort=False):
    assert not merge_sort, "merge_sort is not implemented"
    if isinstance(input_file_path, str):
        return make_text_generator_single(input_file_path)

    elif isinstance(input_file_path, list):
        assert all(isinstance(x, str) for x in input_file_path), \
        "file path should be str"
        return chain(
            make_text_generator_single(file_path) \
            for file_path in input_file_path
        )
    else:
        raise AssertionError("unknown path obj type")


def make_text_generator_single(input_file_path):
    with open(input_file_path, "r") as file:
        for line in file:
            yield line


def write_output(words, output_file_path, n_buckets=None):
    if n_buckets is None:
        files = [None]
    else:
        files = [None] * n_buckets
    for word in words:
        bucket = which_bucket(word, n_buckets)
        if files[bucket] is None:
            file_path = output_file_path
            if n_buckets is not None:
                file_path = f"{file_path}-{bucket}"
            files[bucket] = open(file_path, "w")
        file = files[bucket]
        file.write(word)
        file.write("\n")
    for file in files:
        if file is not None:
            file.close()


def which_bucket(word, n_buckets):
    if n_buckets is None:
        return 0
    return ord(word[0]) % n_buckets
