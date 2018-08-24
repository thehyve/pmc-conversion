import unittest
import os
from scripts.csr_read_data import get_encoding, input_file_to_df, bool_is_file, validate_source_file, \
    check_for_codebook, set_date_fields, get_date_as_string, apply_header_map, check_file_list, determine_file_type


class CsrTransformationTests(unittest.TestCase):

    def setUp(self):
        self.clinical_test_data = '../test_data/input_data/CLINICAL'
        self.dummy_test_data = '../test_data/dummy_data'
        self.test_config = '../test_data/test_config'
        self.config = '../config'

    def tearDown(self):
        pass

    def test_get_encodiing(self):
        assert (True)

    def test_apply_header_map(self):
        filename = 'br_test.txt'
        filepath = os.path.join(self.clinical_test_data, filename)
        df = input_file_to_df(filepath, get_encoding(filepath), codebook=None)

        # Define header map
        header_map = {
            "br_test.txt": {
                "CIDDIAG2": "DIAGNOSIS_ID",
                "IDAABA_pseudo": "DIAGNOSIS_DATE",
                "HOSPDIAG": "CENTER_TREATMENT",
                "DIAGCD": "TUMOR_TYPE",
                "PLOCCD": "TOPOGRAPHY",
                "DIAGGRSTX": "TUMOR_STAGE",
                "IDAA_PMC": "INDIVIDUAL_ID"
            }
        }
        new_columns = apply_header_map(df.columns, header_map[filename])

        expected_columns = ['IDAA', 'CID', 'DIAGNOSIS_DATE', 'DIAGNOSIS_ID',
                            'CENTER_TREATMENT', 'TUMOR_TYPE', 'TOPOGRAPHY',
                            'TUMOR_STAGE', 'INDIVIDUAL_ID', 'TREATMENT_PROTOCOL']

        self.assertEqual(new_columns, expected_columns)

    def test_determine_file_type(self):
        files = dict(
            bm_file='biomaterial.txt',
            bs_file='biosource.txt',
            idv_file='individual.txt',
            dia_file='diagnosis.txt',
            st_file='study.txt',
            ist_file='individual_study.txt'
        )
        correct_types = {
            'bm_file': 'biomaterial',
            'bs_file': 'biosource',
            'idv_file': 'individual',
            'dia_file': 'diagnosis',
            'st_file': 'study',
            'ist_file': 'individual_study'
        }

        checked_types = {}
        for key, file in files.items():
            filepath = os.path.join(self.dummy_test_data, file)
            df = input_file_to_df(filepath, get_encoding(filepath), codebook=None)
            checked_types.update({key: determine_file_type(df.columns, file)})

        self.assertEqual(correct_types, checked_types)


if __name__ == '__main__':
    unittest.main()
