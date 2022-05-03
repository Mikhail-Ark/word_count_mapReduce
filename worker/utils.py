

def make_text_generator(input_file_path):
    if isinstance(input_file_path, str):
        for line in make_text_generator_single(input_file_path):
            yield line

    elif isinstance(input_file_path, list):
        assert all(isinstance(x, str) for x in input_file_path), \
        "file path should be str"
        for file_path in input_file_path:
            for line in make_text_generator_single(file_path):
                yield line
    else:
        raise AssertionError("unknown path obj type")


def make_text_generator_single(input_file_path):
    with open(input_file_path, "r") as file:
        for line in file:
            yield line