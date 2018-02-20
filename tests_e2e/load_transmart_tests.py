import unittest
import configparser
from tests_e2e import test_helpers


class LoadTransmartTest(unittest.TestCase):
    config = configparser.ConfigParser()
    config.read('luigi.cfg')

    def setUp(self):
        test_helpers.clean_database(self.config)
        test_helpers.clean_dropzone(self.config)

    def test_load_valid_data(self):
        test_helpers.run_pipe()
        self.assertTrue(test_helpers.checkDB(self.config)[2] == 'CSR_STUDY', 'row one in the study table does not contain CSR_STUDY')


if __name__ == '__main__':
    unittest.main()