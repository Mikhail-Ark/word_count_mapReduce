from os import listdir, path, stat


def make_text_generator(input_file_path, merge_join=False):
    if isinstance(input_file_path, str):
        yield from make_text_generator_single(input_file_path)

    elif isinstance(input_file_path, list):
        assert all(isinstance(x, str) for x in input_file_path), \
                "file path should be str"
        if merge_join:
            yield from make_text_generator_merge_join(input_file_path)
        else:
            for file_path in input_file_path:
                yield from make_text_generator_single(file_path)
    else:
        raise AssertionError("unknown path obj type")


def make_text_generator_single(input_file_path):
    with open(input_file_path, "r") as file:
        for line in file:
            yield line.rstrip("\n")


def make_text_generator_merge_join(input_files_paths):
    if len(input_files_paths) == 1:
        yield from make_text_generator_single(input_files_paths[0])
        return

    files = [open(file_path, "r") for file_path in input_files_paths]
    current_words = [file.readline().rstrip("\n") for file in files]
    active_files = list(range(len(input_files_paths)))
    inactivate = list()

    while active_files:
        min_word = min(current_words[i] for i in active_files)
        for i in active_files:
            current_word = current_words[i]
            file = files[i]
            while current_word == min_word:
                yield current_word
                current_word = file.readline().rstrip("\n")
                if not current_word:
                    file.close()
                    inactivate.append(i)
                    break
                current_words[i] = current_word
        for i in inactivate:
            active_files.remove(i)
        inactivate.clear()


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
    if n_buckets is None and files[0] is None:
        file = open(output_file_path, "w")
        file.close()
    for file in files:
        if file is not None:
            file.close()


def which_bucket(word, n_buckets):
    if n_buckets is None:
        return 0
    return ord(word[0]) % n_buckets


def find_files_for_task(
    input_path, input_file_names=None, job_id=None, only_names=False
):
    file_names = [
        file_name for file_name in listdir(input_path)
        if not is_dir_or_empty_file(input_path + file_name)
    ]
    if input_file_names:
        if isinstance(input_file_names, str):
            input_file_names = [input_file_names]
        file_names = [
            file_name for file_name in file_names \
            if file_name in set(input_file_names)
        ]
    if job_id is not None:
        file_name_ending = f"-{job_id}"
        file_names = [
            file_name for file_name in file_names \
            if file_name.endswith(file_name_ending)
        ]
    if only_names:
        return file_names
    files_for_task = [input_path + file_name for file_name in file_names]
    return files_for_task


def is_dir_or_empty_file(file_path):
    return (stat(file_path).st_size == 0) or path.isdir(file_path)
