import unittest

from scripts.codebook_formatting import *


class CodebookFormattingTests(unittest.TestCase):

    def setUp(self):
        self.br_codebook_filename = './test_data/dropzone/CLINICAL/br_test_codebook.txt'
        self.output_check = './test_data/intermediate/br_test_codebook.txt.json'
        self.tmp = tempfile.gettempdir()

        # correct_mapping = {"br_test_codebook.txt": "br_codebook_1"}
        # self.correct_mapping_file = os.path.join(self.tmp, 'codebook_mapping.json')
        # with open(self.correct_mapping_file,'w') as ccm:
        #     json.dump(correct_mapping,ccm)

    def tearDown(self):
        pass

    def test_process_br_codebook(self):
        """Test codebook formatting for br_format codebooks"""
        with open(self.br_codebook_filename, 'r', encoding=get_encoding(self.br_codebook_filename)) as brf:
            br_lines = brf.readlines()
        processed_br = process_br_codebook(br_lines)

        with open(self.output_check, 'r', encoding=get_encoding(self.output_check)) as ocf:
            self.br_json = json.loads(ocf.read())

        self.assertEqual(processed_br, self.br_json)

    def test_incorrect_codebook_format(self):
        """Test if """
        incorrect_mapping = {
            "br_test_codebook.txt": "DOES_NOT_EXISTS"
        }
        incorrect_mapping_file = os.path.join(self.tmp, 'incorrect_codebook_mapping.json')
        with open(incorrect_mapping_file, 'w') as cim:
            json.dump(incorrect_mapping, cim)

        with self.assertLogs('codebook_formatting', 'WARN'):
            outcome = codebook_formatting(self.br_codebook_filename, incorrect_mapping_file, self.tmp)
        self.assertFalse(outcome)


if __name__ == '__main__':
    unittest.main()
