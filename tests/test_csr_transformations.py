import os
import tempfile
import unittest

import pandas as pd

import scripts.csr_transformations as ct
from definitions import TEST_DATA_DIR, TEST_EXPECTED_OUT_DIR, ROOT_DIR


# TODO: Refactor test cases to not rely on production config

class CsrTransformationTests(unittest.TestCase):

    def setUp(self):
        self.default_data = TEST_DATA_DIR.joinpath('default_data')
        self.dummy_test_data = TEST_DATA_DIR.joinpath('dummy_data')
        self.missing_diag_data = TEST_DATA_DIR.joinpath('missing_diagnosis_date')
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

    def test_calculate_age_at_diagnosis(self):
        """Test that the CSR pipeline correctly handles missing first diagnosis date data
        by comparing the resulting csr_transformation_data.tsv file with the expected output."""
        # given
        output_dir = tempfile.mkdtemp()
        output_filename = 'csr_transformation_data.tsv'
        output_study_filename = 'study_registry.tsv'

        # when
        ct.csr_transformation(
            input_dir=self.missing_diag_data,
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
        reference_df_path = TEST_EXPECTED_OUT_DIR.joinpath('missing_diagnosis_date', 'csr_transformation_data.tsv')
        reference_df = pd.read_csv(reference_df_path, sep='\t')
        reference_df = reference_df.reindex(sorted(reference_df.columns), axis=1)

        csr_output_path = os.path.join(output_dir, output_filename)
        csr_df = pd.read_csv(csr_output_path, sep='\t')
        csr_df = csr_df.reindex(sorted(csr_df.columns), axis=1)
        self.assertTrue(reference_df.equals(csr_df))

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

    def test_col_prio_triggers_unknown_prio_col_warning(self):
        """Test that provided priority for a column not present in the data triggers a warning."""
        with self.assertLogs(level='WARNING') as log:
            ct.check_column_prio(column_prio_dict={'COL2': ['FILE_A', 'FILE_B']},
                                 col_file_dict={},
                                 col_prio_file='cp.txt', file_headers_file='fh.txt')
        self.assertEqual(len(log.output), 1)
        self.assertIn("'COL2', but the column was not found in the expected columns", log.output[0])

    def test_col_prio_triggers_unknown_file_warning(self):
        """Test that column priority containing more files than there are files
        that actually have that column triggers a warning."""
        # Unknown file in priority warning
        with self.assertLogs(level='WARNING') as log:
            ct.check_column_prio(column_prio_dict={'COL1': ['FILE_A', 'FILE_B']},
                                 col_file_dict={'COL1': ['FILE_A']},
                                 col_prio_file='cp.txt', file_headers_file='fh.txt')
        self.assertEqual(len(log.output), 1)
        self.assertIn("The following priority files are not used: ['FILE_B']", log.output[0])

    def test_col_prio_triggers_absent_priority_error(self):
        """Test that absent column priority for a column that occurs in multiple files
        triggers an error log statement and results in SystemExit."""
        with self.assertRaises(SystemExit) as cm, self.assertLogs(level='WARNING') as log:
            ct.check_column_prio(column_prio_dict={},
                                 col_file_dict={'COL1': ['FILE_A', 'FILE_B']},
                                 col_prio_file='cp.txt', file_headers_file='fh.txt')
        self.assertEqual(len(log.output), 1)
        self.assertIn("'COL1' column occurs in multiple data files: ['FILE_A', 'FILE_B'], but no prio", log.output[0])
        self.assertEqual(cm.exception.code, 1)

    def test_col_prio_triggers_incomplete_priority_error(self):
        """Test that incomplete column priority for a column that occurs in more files
        than priority was provided triggers an error log statement and results in SystemExit."""
        with self.assertRaises(SystemExit) as cm, self.assertLogs(level='WARNING') as log:
            ct.check_column_prio(column_prio_dict={'COL1': ['FILE_A', 'FILE_B']},
                                 col_file_dict={'COL1': ['FILE_A', 'FILE_B', 'FILE_C']},
                                 col_prio_file='cp.txt', file_headers_file='fh.txt')
        self.assertEqual(len(log.output), 1)
        self.assertIn("Incomplete priority provided for column 'COL1'", log.output[0])
        self.assertEqual(cm.exception.code, 1)

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