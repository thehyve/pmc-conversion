import shutil
import tempfile
import unittest
from filecmp import dircmp
from os import path, makedirs

from scripts import sync


class SyncTests(unittest.TestCase):
    def setUp(self):
        """
         Create 3 data files with checksum files one of which is wrong.
        """
        self.test_dir = tempfile.mkdtemp()

        self.checksum_matches = path.join(self.test_dir, 'checksum_matches')
        makedirs(self.checksum_matches)

        self.data_file1 = path.join(self.checksum_matches, 'data1.txt')
        with open(self.data_file1, 'w') as data_file1:
            data_file1.write('data comes here')
        self.checksum1 = '4d088dde987b4900268c251a788ab524c0ef7699'
        self.checksum_file1 = path.join(self.checksum_matches, 'data1.txt.sha1')
        with open(self.checksum_file1, 'w') as checksum_file1:
            checksum_file1.write(self.checksum1)

        self.data_file2 = path.join(self.checksum_matches, 'data2.txt')
        with open(self.data_file2, 'w') as data_file2:
            data_file2.write('second data file content')
        self.checksum2 = 'a591490d058ce6792a71645af522e01c1f90d5c6'
        self.checksum_file2 = path.join(self.checksum_matches, 'data2.txt.sha1')
        with open(self.checksum_file2, 'w') as checksum_file2:
            checksum_file2.write(self.checksum2)

        self.checksum_does_not_match = path.join(self.test_dir, 'checksum_does_not_match')
        makedirs(self.checksum_does_not_match)
        self.data_file3 = path.join(self.checksum_does_not_match, 'data3.txt')
        with open(self.data_file3, 'w') as data_file3:
            data_file3.write('foo bar baz')
        self.checksum3 = 'c7567e8b39e2428e38bf9c9226ac68de4c67dc39'
        self.checksum_file3 = path.join(self.checksum_does_not_match, 'data3.txt.sha1')
        with open(self.checksum_file3, 'w') as checksum_file3:
            checksum_file3.write('ffffffffffffffffffffffffffffffffffffffff')

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_is_checksum_file(self):
        self.assertTrue(sync.is_checksum_file('data.txt.sha1'))
        self.assertFalse(sync.is_checksum_file('data.txt'))

    def test_get_checksum_file_name(self):
        self.assertEqual('data.txt.sha1', sync.get_checksum_file('data.txt'))

    def test_get_data_checksum_file_pairs(self):
        result = list(sync.get_data_checksum_file_pairs([
            'data.txt',
            'data.txt.sha1',
            'data2.txt',
            'data2.txt.sha1'
        ]))

        self.assertEqual(len(result), 2)
        self.assertTrue(('data.txt', 'data.txt.sha1') in result)
        self.assertTrue(('data2.txt', 'data2.txt.sha1') in result)

        with self.assertRaises(FileNotFoundError) as ctx1:
            list(sync.get_data_checksum_file_pairs(['data.txt']))
        self.assertEqual('The data file data.txt does not have corresponding checksum file.', str(ctx1.exception))

        with self.assertRaises(FileNotFoundError) as ctx2:
            list(sync.get_data_checksum_file_pairs(['data.txt.sha1', 'data2.txt.sha1']))
        self.assertEqual('The following checksum files does not have corresponding data file:'
                         ' data.txt.sha1, data2.txt.sha1', str(ctx2.exception))

    def test_ensure_checksum_matches(self):
        result = list(sync.ensure_checksum_matches([sync.DataChecksumFilesPair(self.data_file1, self.checksum_file1)]))
        self.assertEqual(len(result), 1)
        self.assertTrue((self.data_file1, self.checksum1) in result)

        with self.assertRaises(ValueError) as ctx1:
            list(sync.ensure_checksum_matches([sync.DataChecksumFilesPair(self.data_file3, self.checksum_file3)]))
        self.assertEqual(f'Checksum for {self.data_file3} file does not match.', str(ctx1.exception))

    def test_read_files_checksums(self):
        result = list(sync.scan_files_checksums(self.test_dir))
        self.assertEqual(len(result), 3)
        self.assertIn((self.data_file1, self.checksum1), result)
        self.assertIn((self.data_file2, self.checksum2), result)
        self.assertIn((self.data_file3, self.checksum3), result)

    def test_make_path_relative(self):
        self.assertEqual('def/data.txt', sync.make_path_relative('/abc', '/abc/def/data.txt'))
        self.assertEqual('def/data.txt', sync.make_path_relative('/abc/', '/abc/def/data.txt'))

        with self.assertRaises(ValueError) as ctx1:
            sync.make_path_relative('/abc/', '/def/data.txt')
        self.assertEqual('/def/data.txt is not sub directory of the /abc/.', str(ctx1.exception))

    def test_sync(self):
        copy_dir = path.join(self.test_dir, 'copy_here')
        makedirs(copy_dir)
        with open(path.join(copy_dir, 'data.txt'), 'w') as data_file1_old:
            data_file1_old.write('previous version of content')
        with open(path.join(copy_dir, 'remove_me.txt'), 'w') as data_file_to_remove:
            data_file_to_remove.write('remove me file content')

        self.assertFalse(sync.is_dirs_in_sync(self.checksum_matches, copy_dir))

        sync.sync_dirs(self.checksum_matches, copy_dir)

        self.assertTrue(sync.is_dirs_in_sync(self.checksum_matches, copy_dir))

        dcmp = dircmp(self.checksum_matches, copy_dir)
        self.assertEqual(2, len(dcmp.left_only))
        self.assertTrue('data1.txt.sha1' in dcmp.left_only)
        self.assertTrue('data2.txt.sha1' in dcmp.left_only)
        self.assertFalse(dcmp.right_only)
        self.assertEqual(2, len(dcmp.same_files))
        self.assertTrue('data1.txt' in dcmp.same_files)
        self.assertTrue('data2.txt' in dcmp.same_files)


if __name__ == '__main__':
    unittest.main()
