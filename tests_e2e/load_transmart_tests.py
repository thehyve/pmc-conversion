import unittest
import configparser
from tests_e2e import test_helpers


class LoadTransmartTest(unittest.TestCase):
    config = configparser.ConfigParser()
    config.read('luigi.cfg')

    def setUp(self):
        test_helpers.clean_dropzone(self.config)

    def test_load_valid_data(self):
        test_helpers.run_pipe()
        self.assertTrue(test_helpers.checkDB(self.config)[2] == self.config['GlobalConfig']['study_id'],
                        'row one in the study table does not contain the expected study')


if __name__ == '__main__':
    unittest.main()
