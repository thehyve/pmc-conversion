import shutil
import tempfile
from os import path
import unittest

from scripts import checksum


class ChecksumTests(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_data_file = path.join(self.test_dir, 'data.txt')
        with open(self.test_data_file, 'w') as f:
            f.write('Hello world!')

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_sha1_checksum(self):
        self.assertEqual('d3486ae9136e7856bc42212385ea797094475802', checksum.compute_sha1(self.test_data_file))

    def test_sha256_checksum(self):
        self.assertEqual('c0535e4be2b79ffd93291305436bf889314e4a3faec05ecffcbb7df31ad9e51a',
                         checksum.compute_sha256(self.test_data_file))


if __name__ == '__main__':
    unittest.main()
