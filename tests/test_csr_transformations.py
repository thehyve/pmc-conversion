import unittest
import os
#import csr_transformations as ct
import scripts.csr_transformations as ct
from pathlib import Path
from definitions import ROOT_DIR, TEST_DATA_DIR

import tempfile


# TODO: Refactor test cases to not rely on production config

class CsrTransformationTests(unittest.TestCase):

    def setUp(self):
        self.default_data = TEST_DATA_DIR.joinpath('default_data')
        self.dummy_test_data = TEST_DATA_DIR.joinpath('dummy_data')
        self.test_config = TEST_DATA_DIR.joinpath('test_config')
        self.config = ROOT_DIR.joinpath('config')

    def tearDown(self):
        pass

    def test_csr_transformation(self):
        # given
        output_dir = tempfile.mkdtemp()
        output_filename = 'csr_transformation_data.tsv'
        output_study_filename = 'study_registry.tsv'

        # when
        ct.csr_transformation(
            input_dir=self.default_data,
            output_dir=output_dir,

            config_dir=self.config,
            data_model='data_model.json',
            column_priority='column_priority.json',
            file_headers='file_headers.json',
            columns_to_csr='columns_to_csr.json',

            output_filename=output_filename,
            output_study_filename=output_study_filename
        )

        # then
        output_filename_path = os.path.join(output_dir, output_filename)
        self.assertTrue(os.path.exists(output_filename_path))
        output_study_filename_path = os.path.join(output_dir, output_study_filename)
        self.assertTrue(os.path.exists(output_study_filename_path))

    def test_read_dict_from_file(self):
        ref_dict = {'patient': ['age', 'date', 'gender'],
                    'single_item': 'item',
                    'object_item': {'key': 'value'}
                    }
        dict_ = ct.read_dict_from_file('dict_file.json', path=self.test_config)
        self.assertDictEqual(dict_, ref_dict)

    def test_validate_source_file(self):
        source_file = os.path.join(self.default_data, 'study.tsv')
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

        self.assertIn('SRC_BIOSOURCE_ID', overlap.keys())
        self.assertEqual(sorted(overlap['SRC_BIOSOURCE_ID']), sorted(['biomaterial.tsv', 'biosource.tsv']))

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