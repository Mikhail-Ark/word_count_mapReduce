from os import listdir, path, remove
from unittest import TestCase, main

from worker.tasks.map import tokenize, tokenize_lines, word_count_map
from worker.tasks.reduce import count, find_files_to_reduce, word_count_reduce
from worker.utils import is_empty_file, make_text_generator_merge_sort

INPUT_PATH = "./files/inputs/testing/"
INTERMEDIATE_PATH = "./files/intermediate/testing/"
OUTPUT_PATH = "./files/out/testing/"


class TestMethodsSimple(TestCase):
    def test_unit_tokenize_simple(self):
        line = "Hello, world!"
        res = tokenize(line, ignore_case=True)
        expected_res = ["hello", "world"]
        self.assertEqual(res, expected_res)


    def test_unit_tokenize_empty(self):
        line = ""
        res = tokenize(line)
        expected_res = []
        self.assertEqual(res, expected_res)


    def test_unit_tokenize_usual(self):
        line = "Hello, world! Nice to see you, world."
        res = tokenize(line, ignore_case=False)
        expected_res = ["Hello", "world", "Nice", "to", "see", "you", "world"]
        self.assertEqual(res, expected_res)


    def test_unit_tokenize_lines_simple(self):
        line = ["Hello, world!"]
        res = list(tokenize_lines(line, ignore_case=True))
        expected_res = ["hello", "world"]
        self.assertEqual(res, expected_res)


    def test_unit_tokenize_lines_empty(self):
        line = []
        res = list(tokenize_lines(line))
        expected_res = []
        self.assertEqual(res, expected_res)


    def test_unit_tokenize_lines_small(self):
        lines = ["Hello, world!", "Nice to see you, world."]
        res = list(tokenize_lines(lines, ignore_case=False))
        expected_res = ["Hello", "world", "Nice", "to", "see", "you", "world"]
        self.assertEqual(res, expected_res)


    def test_unit_count(self):
        words = (
            w for w in ["Hello", "world", "Nice", "to", "see", "you", "world"]
        )
        res = set(count(words, sorted=False))
        expected_res = {
            ('Hello', 1), ('world', 2), ('Nice', 1),
            ('to', 1), ('see', 1), ('you', 1)
        }
        self.assertEqual(res, expected_res)


    def test_unit_count_sorted(self):
        words = (
            w for w in sorted(
                ["Hello", "world", "Nice", "to", "see", "you", "world"]
            )
        )
        res = set(count(words, sorted=True))
        expected_res = {
            ('Hello', 1), ('world', 2), ('Nice', 1),
            ('to', 1), ('see', 1), ('you', 1)
        }
        self.assertEqual(res, expected_res)
        

    def test_unit_count_unsorted(self):
        words = (
            w for w in ["Hello", "world", "Nice", "to", "see", "you", "world"]
        )
        res = count(words, sorted=True)
        self.assertRaises(AssertionError, list, res)


    def test_unit_count_empty(self):
        words = (w for w in [])
        res = set(count(words, sorted=False))
        expected_res = set()
        self.assertEqual(res, expected_res)


    def test_unit_count_sorted_empty(self):
        words = (w for w in [])
        res = set(count(words, sorted=True))
        expected_res = set()
        self.assertEqual(res, expected_res)


    def test_unit_find_files_to_reduce(self):
        res = set(
            find_files_to_reduce(INTERMEDIATE_PATH, job_id=9)
        )
        expected_res = {
            f"{INTERMEDIATE_PATH}test-merge-sort-{i}-9" for i in (0, 1, 3)
        }
        self.assertEqual(res, expected_res)


    def test_unit_make_text_generator_merge_sort(self):
        file_list = find_files_to_reduce(INTERMEDIATE_PATH, job_id=9)
        res = list(make_text_generator_merge_sort(file_list))
        expected_res = [
            'aa', 'aaa', 'bbb', 'ccc', 'ccc', 'ccc',
            'ccc','ddd', 'ddd', 'eee', 'eee'
        ]
        self.assertEqual(res, expected_res)


class TestMethodsOutputFiles(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.clear_dirs()


    @staticmethod
    def clear_dirs():
        paths = [
            INTERMEDIATE_PATH,
            OUTPUT_PATH
        ]
        for path in paths:
            for file in listdir(path):
                if file.startswith("_test"):
                    remove(path + file)


    def tearDown(self):
        self.clear_dirs()


    def test_functional_word_count_map_single(self):
        input_file_path = INPUT_PATH + "test_single.txt"
        output_path = INTERMEDIATE_PATH + "_test-single"
        word_count_map(
            input_file_path, output_path, n_buckets=1
        )
        with open(output_path + "-0-0", "r") as file:
            res = file.read()
        expected_res = "hello\n"
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_empty(self):
        input_file_path = INPUT_PATH + "test_empty.txt"
        output_file_path = INTERMEDIATE_PATH + "_test-empty"
        word_count_map(
            input_file_path, output_file_path, n_buckets=1
        )
        self.assertFalse(path.isfile(output_file_path + "-0"))


    def test_functional_word_count_map_small(self):
        input_file_path = INPUT_PATH + "test_small.txt"
        output_path = INTERMEDIATE_PATH + "_test-small"
        word_count_map(
            input_file_path, output_path, n_buckets=1
        )
        with open(output_path + "-0-0", "r") as file:
            res = file.read()
        expected_res = """algernon\ni\nreally\ndon\nt\nsee\nanything\nromantic
in\nproposing\nit\nis\nvery\nromantic\nto\nbe\nin\nlove\nbut\nthere\nis\nnothing
romantic\nabout\na\ndefinite\nproposal\nwhy\none\nmay\nbe\naccepted\none
usually\nis\ni\nbelieve\nthen\nthe\nexcitement\nis\nall\nover\nthe\nvery
essence\nof\nromance\nis\nuncertainty\nif\never\ni\nget\nmarried\ni\nll
certainly\ntry\nto\nforget\nthe\nfact\n"""
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_sort(self):
        input_file_path = INPUT_PATH + "test_small.txt"
        output_path = INTERMEDIATE_PATH + "_test-small"
        word_count_map(
            input_file_path, output_path, sort=True, n_buckets=1
        )
        with open(output_path + "-0-0", "r") as file:
            res = file.read()
        expected_res = """a\nabout\naccepted\nalgernon\nall\nanything\nbe\nbe
believe\nbut\ncertainly\ndefinite\ndon\nessence\never\nexcitement\nfact\nforget
get\ni\ni\ni\ni\nif\nin\nin\nis\nis\nis\nis\nis\nit\nll\nlove\nmarried\nmay
nothing\nof\none\none\nover\nproposal\nproposing\nreally\nromance\nromantic
romantic\nromantic\nsee\nt\nthe\nthe\nthe\nthen\nthere\nto\nto\ntry\nuncertainty
usually\nvery\nvery\nwhy\n"""
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_case(self):
        input_file_path = INPUT_PATH + "test_small.txt"
        output_path = INTERMEDIATE_PATH + "_test-small"
        word_count_map(
            input_file_path, output_path, ignore_case=False, n_buckets=1
        )
        with open(output_path + "-0-0", "r") as file:
            res = file.read()
        self.assertEqual(sum(c.isupper() for c in res), 12)


    def test_functional_word_count_map_usual(self):
        input_file_path = INPUT_PATH + "test_usual.txt"
        output_path = INTERMEDIATE_PATH + "_test-usual"
        word_count_map(
            input_file_path, output_path, n_buckets=1
        )
        with open(output_path + "-0-0", "r") as file:
            res = file.read()
        self.assertEqual(len(res), 129609)
        res_split = res.split()
        self.assertEqual(len(res_split), 24255)
        self.assertEqual(len(set(res_split)), 3057)


    def test_functional_word_count_map_buckets_single(self):
        input_file_path = INPUT_PATH + "test_single.txt"
        output_path = INTERMEDIATE_PATH + "_test-single"
        word_count_map(
            input_file_path, output_path, n_buckets=5
        )
        expected_res = "hello\n"
        test_files = [
            f for f in listdir(INTERMEDIATE_PATH) \
            if f.startswith("_test")
        ]
        self.assertEqual(len(test_files), 1)
        with open(f"{INTERMEDIATE_PATH}{test_files[0]}", "r") as file:
            res = file.read()
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_buckets_empty(self):
        input_file_path = INPUT_PATH + "test_empty.txt"
        output_path = INTERMEDIATE_PATH + "_test-empty"
        word_count_map(
            input_file_path, output_path, n_buckets=5
        )
        test_files = [
            f for f in listdir(INTERMEDIATE_PATH) \
            if f.startswith("_test")
        ]
        self.assertEqual(len(test_files), 0)


    def test_functional_word_count_map_buckets_small(self):
        input_file_path = INPUT_PATH + "test_small.txt"
        output_path = INTERMEDIATE_PATH + "_test-small"
        word_count_map(
            input_file_path, output_path, n_buckets=5
        )
        expected_res = [
            "i\ndon\nsee\nin\nit\nis\nin\nis\nnothing\ndefinite\nis\ni\nis\nis\nif\ni\ni\n",
            "t\nto\nthere\none\none\nthen\nthe\nexcitement\nover\nthe\nessence\nof\never\ntry\nto\nthe\n",
            "algernon\nanything\nproposing\nabout\na\nproposal\naccepted\nusually\nall\nuncertainty\nforget\nfact\n",
            "very\nbe\nlove\nbut\nbe\nbelieve\nvery\nget\nll\n",
            "really\nromantic\nromantic\nromantic\nwhy\nmay\nromance\nmarried\ncertainly\n"
        ]
        test_files = [
            f"{INTERMEDIATE_PATH}{file}" for file in \
            listdir(INTERMEDIATE_PATH) if file.startswith("_test")
        ]
        test_files.sort()
        res = list()
        for file_path in test_files:
            with open(file_path, "r") as file:
                res.append(file.read())
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_buckets_sort(self):
        input_file_path = INPUT_PATH + "test_small.txt"
        output_path = INTERMEDIATE_PATH + "_test-small"
        word_count_map(
            input_file_path, output_path, sort=True, n_buckets=5
        )
        expected_res = [
            "definite\ndon\ni\ni\ni\ni\nif\nin\nin\nis\nis\nis\nis\nis\nit\nnothing\nsee\n",
            "essence\never\nexcitement\nof\none\none\nover\nt\nthe\nthe\nthe\nthen\nthere\nto\nto\ntry\n",
            "a\nabout\naccepted\nalgernon\nall\nanything\nfact\nforget\nproposal\nproposing\nuncertainty\nusually\n",
            "be\nbe\nbelieve\nbut\nget\nll\nlove\nvery\nvery\n",
            "certainly\nmarried\nmay\nreally\nromance\nromantic\nromantic\nromantic\nwhy\n"
        ]
        test_files = [
            f"{INTERMEDIATE_PATH}{file}" for file in \
            listdir(INTERMEDIATE_PATH) if file.startswith("_test")
        ]
        test_files.sort()
        res = list()
        for file_path in test_files:
            with open(file_path, "r") as file:
                res.append(file.read())
        self.assertEqual(res, expected_res)


    def test_functional_word_count_reduce_single(self):
        job_id = 0
        word_count_reduce(
            INTERMEDIATE_PATH, OUTPUT_PATH + "_test", job_id, merge_sort=False
        )
        with open(f"{OUTPUT_PATH}_test-{job_id}", "r") as file:
            res = file.read()
        expected_res = "hello 1\n"
        self.assertEqual(res, expected_res)


    def test_functional_word_count_reduce_empty(self):
        job_id = 999
        word_count_reduce(
            INTERMEDIATE_PATH, OUTPUT_PATH + "_test", job_id, merge_sort=False
        )
        output_file_path = f"{OUTPUT_PATH}_test-{job_id}"
        self.assertTrue(path.isfile(output_file_path))
        self.assertTrue(is_empty_file(output_file_path))


    def test_functional_word_count_reduce_small(self):
        job_id = 1
        word_count_reduce(
            INTERMEDIATE_PATH, OUTPUT_PATH + "_test", job_id, merge_sort=False
        )
        with open(f"{OUTPUT_PATH}_test-{job_id}", "r") as file:
            res = set(file.read().split("\n"))
        expected_res = {
            '', 'a 1', 'about 1', 'accepted 1', 'algernon 1', 'all 1',
            'anything 1', 'be 2', 'believe 1', 'but 1', 'certainly 1',
            'definite 1', 'don 1', 'essence 1', 'ever 1', 'excitement 1',
            'fact 1', 'forget 1', 'get 1', 'i 4', 'if 1', 'in 2', 'is 5',
            'it 1', 'll 1', 'love 1', 'married 1', 'may 1', 'nothing 1',
            'of 1', 'one 2', 'over 1', 'proposal 1', 'proposing 1', 'really 1',
            'romance 1', 'romantic 3', 'see 1', 't 1', 'the 3', 'then 1',
            'there 1', 'to 2', 'try 1', 'uncertainty 1', 'usually 1', 'very 2',
            'why 1'
        }
        self.assertEqual(res, expected_res)


    def test_functional_word_count_reduce_small_sorted(self):
        job_id = 2
        word_count_reduce(
            INTERMEDIATE_PATH, OUTPUT_PATH + "_test", job_id, merge_sort=True
        )
        with open(f"{OUTPUT_PATH}_test-{job_id}", "r") as file:
            res = file.read()
        expected_res = """a 1\nabout 1\naccepted 1\nalgernon 1\nall 1
anything 1\nbe 2\nbelieve 1\nbut 1\ncertainly 1\ndefinite 1\ndon 1\nessence 1
ever 1\nexcitement 1\nfact 1\nforget 1\nget 1\ni 4\nif 1\nin 2\nis 5\nit 1\nll 1
love 1\nmarried 1\nmay 1\nnothing 1\nof 1\none 2\nover 1\nproposal 1
proposing 1\nreally 1\nromance 1\nromantic 3\nsee 1\nt 1\nthe 3\nthen 1\nthere 1
to 2\ntry 1\nuncertainty 1\nusually 1\nvery 2\nwhy 1\n"""
        self.assertEqual(res, expected_res)


    def test_functional_word_count_reduce_usual(self):
        job_id = 3
        word_count_reduce(
            INTERMEDIATE_PATH, OUTPUT_PATH + "_test", job_id, merge_sort=False
        )
        with open(f"{OUTPUT_PATH}_test-{job_id}", "r") as file:
            res = file.read()
        self.assertEqual(len(res), 30377)
        self.assertEqual(len(res.split("\n")), 3058)


    def test_functional_word_count_reduce_small_bubkets(self):
        job_id = 0
        word_count_reduce(
            INTERMEDIATE_PATH + "small_buckets/", OUTPUT_PATH + "_test",
            job_id, merge_sort=False
        )
        with open(f"{OUTPUT_PATH}_test-{job_id}", "r") as file:
            res = set(file.read().split("\n"))
        expected_res = {
            '', 'a 1', 'about 1', 'accepted 1', 'algernon 1', 'all 1',
            'anything 1', 'be 2', 'believe 1', 'but 1', 'certainly 1',
            'definite 1', 'don 1', 'essence 1', 'ever 1', 'excitement 1',
            'fact 1', 'forget 1', 'get 1', 'i 4', 'if 1', 'in 2', 'is 5',
            'it 1', 'll 1', 'love 1', 'married 1', 'may 1', 'nothing 1',
            'of 1', 'one 2', 'over 1', 'proposal 1', 'proposing 1', 'really 1',
            'romance 1', 'romantic 3', 'see 1', 't 1', 'the 3', 'then 1',
            'there 1', 'to 2', 'try 1', 'uncertainty 1', 'usually 1', 'very 2',
            'why 1'
        }
        self.assertEqual(res, expected_res)


    def test_functional_word_count_reduce_small_bubkets_sorted(self):
        job_id = 0
        word_count_reduce(
            INTERMEDIATE_PATH + "small_buckets_sorted/", OUTPUT_PATH + "_test",
            job_id, merge_sort=True
        )
        with open(f"{OUTPUT_PATH}_test-{job_id}", "r") as file:
            res = file.read()
        expected_res = """a 1\nabout 1\naccepted 1\nalgernon 1\nall 1
anything 1\nbe 2\nbelieve 1\nbut 1\ncertainly 1\ndefinite 1\ndon 1\nessence 1
ever 1\nexcitement 1\nfact 1\nforget 1\nget 1\ni 4\nif 1\nin 2\nis 5\nit 1\nll 1
love 1\nmarried 1\nmay 1\nnothing 1\nof 1\none 2\nover 1\nproposal 1
proposing 1\nreally 1\nromance 1\nromantic 3\nsee 1\nt 1\nthe 3\nthen 1\nthere 1
to 2\ntry 1\nuncertainty 1\nusually 1\nvery 2\nwhy 1\n"""
        self.assertEqual(res, expected_res)


if __name__ == "__main__":
    main()
