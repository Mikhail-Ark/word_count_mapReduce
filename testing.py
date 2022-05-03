import os
from unittest import TestCase, main

from worker.tasks.map import tokenize, tokenize_lines, word_count_map
from worker.tasks.reduce import count, word_count_reduce


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


class TestMethodsWithFiles(TestCase):
    input_path = "./files/inputs/testing/"
    intermediate_path = "./files/intermediate/testing/"
    output_path = "./files/out/testing/"


    @classmethod
    def setUpClass(cls):
        cls.clear_dirs()


    @staticmethod
    def clear_dirs():
        paths = [
            TestMethodsWithFiles.intermediate_path,
            TestMethodsWithFiles.output_path
        ]
        for path in paths:
            for file in os.listdir(path):
                if file.startswith("_test"):
                    os.remove(path + file)


    def tearDown(self):
        self.clear_dirs()


    def test_functional_word_count_map_single(self):
        input_file_path = self.input_path + "test_single.txt"
        output_file_path = self.intermediate_path + "_test-single"
        word_count_map(
            input_file_path, output_file_path, n_buckets=1
        )
        with open(output_file_path + "-0", "r") as file:
            res = file.read()
        expected_res = "hello\n"
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_empty(self):
        input_file_path = self.input_path + "test_empty.txt"
        output_file_path = self.intermediate_path + "_test-empty"
        word_count_map(
            input_file_path, output_file_path, n_buckets=1
        )
        self.assertFalse(os.path.isfile(output_file_path + "-0"))


    def test_functional_word_count_map_small(self):
        input_file_path = self.input_path + "test_small.txt"
        output_file_path = self.intermediate_path + "_test-small"
        word_count_map(
            input_file_path, output_file_path, n_buckets=1
        )
        with open(output_file_path + "-0", "r") as file:
            res = file.read()
        expected_res = """algernon\ni\nreally\ndon\nt\nsee\nanything\nromantic
in\nproposing\nit\nis\nvery\nromantic\nto\nbe\nin\nlove\nbut\nthere\nis\nnothing
romantic\nabout\na\ndefinite\nproposal\nwhy\none\nmay\nbe\naccepted\none
usually\nis\ni\nbelieve\nthen\nthe\nexcitement\nis\nall\nover\nthe\nvery
essence\nof\nromance\nis\nuncertainty\nif\never\ni\nget\nmarried\ni\nll
certainly\ntry\nto\nforget\nthe\nfact\n"""
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_sort(self):
        input_file_path = self.input_path + "test_small.txt"
        output_file_path = self.intermediate_path + "_test-small"
        word_count_map(
            input_file_path, output_file_path, sort=True, n_buckets=1
        )
        with open(output_file_path + "-0", "r") as file:
            res = file.read()
        expected_res = """a\nabout\naccepted\nalgernon\nall\nanything\nbe\nbe
believe\nbut\ncertainly\ndefinite\ndon\nessence\never\nexcitement\nfact\nforget
get\ni\ni\ni\ni\nif\nin\nin\nis\nis\nis\nis\nis\nit\nll\nlove\nmarried\nmay
nothing\nof\none\none\nover\nproposal\nproposing\nreally\nromance\nromantic
romantic\nromantic\nsee\nt\nthe\nthe\nthe\nthen\nthere\nto\nto\ntry\nuncertainty
usually\nvery\nvery\nwhy\n"""
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_case(self):
        input_file_path = self.input_path + "test_small.txt"
        output_file_path = self.intermediate_path + "_test-small"
        word_count_map(
            input_file_path, output_file_path, ignore_case=False, n_buckets=1
        )
        with open(output_file_path + "-0", "r") as file:
            res = file.read()
        self.assertEqual(sum(c.isupper() for c in res), 12)


    def test_functional_word_count_map_usual(self):
        input_file_path = self.input_path + "test_usual.txt"
        output_file_path = self.intermediate_path + "_test-usual"
        word_count_map(
            input_file_path, output_file_path, n_buckets=1
        )
        with open(output_file_path + "-0", "r") as file:
            res = file.read()
        self.assertEqual(len(res), 129609)
        res_split = res.split()
        self.assertEqual(len(res_split), 24255)
        self.assertEqual(len(set(res_split)), 3057)


    def test_functional_word_count_map_buckets_single(self):
        input_file_path = self.input_path + "test_single.txt"
        output_file_path = self.intermediate_path + "_test-single"
        word_count_map(
            input_file_path, output_file_path, n_buckets=5
        )
        expected_res = "hello\n"
        test_files = [
            f for f in os.listdir(self.intermediate_path) \
            if f.startswith("_test")
        ]
        self.assertEqual(len(test_files), 1)
        with open(f"{self.intermediate_path}{test_files[0]}", "r") as file:
            res = file.read()
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_buckets_empty(self):
        input_file_path = self.input_path + "test_empty.txt"
        output_file_path = self.intermediate_path + "_test-empty"
        word_count_map(
            input_file_path, output_file_path, n_buckets=5
        )
        test_files = [
            f for f in os.listdir(self.intermediate_path) \
            if f.startswith("_test")
        ]
        self.assertEqual(len(test_files), 0)


    def test_functional_word_count_map_buckets_small(self):
        input_file_path = self.input_path + "test_small.txt"
        output_file_path = self.intermediate_path + "_test-small"
        word_count_map(
            input_file_path, output_file_path, n_buckets=5
        )
        expected_res = [
            "i\ndon\nsee\nin\nit\nis\nin\nis\nnothing\ndefinite\nis\ni\nis\nis\nif\ni\ni\n",
            "t\nto\nthere\none\none\nthen\nthe\nexcitement\nover\nthe\nessence\nof\never\ntry\nto\nthe\n",
            "algernon\nanything\nproposing\nabout\na\nproposal\naccepted\nusually\nall\nuncertainty\nforget\nfact\n",
            "very\nbe\nlove\nbut\nbe\nbelieve\nvery\nget\nll\n",
            "really\nromantic\nromantic\nromantic\nwhy\nmay\nromance\nmarried\ncertainly\n"
        ]
        test_files = [
            f"{self.intermediate_path}{file}" for file in \
            os.listdir(self.intermediate_path) if file.startswith("_test")
        ]
        test_files.sort()
        res = list()
        for file_path in test_files:
            with open(file_path, "r") as file:
                res.append(file.read())
        self.assertEqual(res, expected_res)


    def test_functional_word_count_map_buckets_sort(self):
        input_file_path = self.input_path + "test_small.txt"
        output_file_path = self.intermediate_path + "_test-small"
        word_count_map(
            input_file_path, output_file_path, sort=True, n_buckets=5
        )
        expected_res = [
            "definite\ndon\ni\ni\ni\ni\nif\nin\nin\nis\nis\nis\nis\nis\nit\nnothing\nsee\n",
            "essence\never\nexcitement\nof\none\none\nover\nt\nthe\nthe\nthe\nthen\nthere\nto\nto\ntry\n",
            "a\nabout\naccepted\nalgernon\nall\nanything\nfact\nforget\nproposal\nproposing\nuncertainty\nusually\n",
            "be\nbe\nbelieve\nbut\nget\nll\nlove\nvery\nvery\n",
            "certainly\nmarried\nmay\nreally\nromance\nromantic\nromantic\nromantic\nwhy\n"
        ]
        test_files = [
            f"{self.intermediate_path}{file}" for file in \
            os.listdir(self.intermediate_path) if file.startswith("_test")
        ]
        test_files.sort()
        res = list()
        for file_path in test_files:
            with open(file_path, "r") as file:
                res.append(file.read())
        self.assertEqual(res, expected_res)



    def test_functional_word_count_reduce(self):
        pass


    def test_functional_word_count_reduce(self):
        pass


    def test_functional_word_count_reduce(self):
        pass


    def test_functional_word_count_reduce(self):
        pass

if __name__ == "__main__":
    main()
