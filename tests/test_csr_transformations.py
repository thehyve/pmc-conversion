import unittest
import os
import logging
import csr_transformations as ct


# TODO: Refactor test cases to not rely on production config

class CsrTransformationTests(unittest.TestCase):

    def setUp(self):
        self.clinical_test_data = './test_data/input_data/CLINICAL'
        self.dummy_test_data = './test_data/dummy_data'
        self.test_config = './test_data/test_config'
        self.config = './config'

    def tearDown(self):
        pass

    def test_read_dict_from_file(self):
        ref_dict = {'patient': ['age', 'date', 'gender'],
                    'single_item': 'item',
                    'object_item': {'key': 'value'}
                    }
        dict_ = ct.read_dict_from_file('dict_file.json', path=self.test_config)
        self.assertDictEqual(dict_, ref_dict)

    def test_validate_source_file(self):
        source_file = os.path.join(self.clinical_test_data, 'study.txt')
        file_prop_dict = ct.read_dict_from_file('file_headers.json', self.config)
        value = ct.validate_source_file(file_prop_dict, source_file, 'file_headers.json')
        self.assertFalse(value)

    def test_validate_empty_source_file(self):
        source_file = os.path.join(self.dummy_test_data, 'empty_file.txt')
        file_prop_dict = ct.read_dict_from_file('file_headers.json', self.config)
        value = ct.validate_source_file(file_prop_dict, source_file, 'file_headers.json')
        self.assertTrue(value)



    def test_get_overlapping_columns(self):
        file_prop_dict = ct.read_dict_from_file('file_headers.json', self.config)
        header_map = ct.read_dict_from_file('columns_to_csr.json', self.config)
        overlap = ct.get_overlapping_columns(file_prop_dict, header_map)
        expected_keys = sorted(['SRC_BIOSOURCE_ID', 'ID', 'IDAA',
                              'DESCRIPTION', 'MARK:', 'LABEL'])

        self.assertEqual(sorted(overlap.keys()), expected_keys)
        self.assertEqual(sorted(['biomaterial.txt', 'biosource.txt']), sorted(overlap['DESCRIPTION']))

    @unittest.skip('todo')
    def test_check_column_priority(self):
        self.assertFalse(True)

    @unittest.skip('todo')
    def test_merge_entity_data_frames(self):
        self.assertFalse(True)

    @unittest.skip('todo')
    def combine_column_data(self):
        self.assertFalse(True)

    @unittest.skip('todo')
    def add_biosource_identifiers(self):
        self.assertFalse(True)

    @unittest.skip('todo')
    def test_build_csr_dataframe(self):
        self.assertFalse(True)

    @unittest.skip('todo')
    def test_resolve_data_conflicts(self):
        self.assertFalse(True)


if __name__ == '__main__':
    unittest.main()