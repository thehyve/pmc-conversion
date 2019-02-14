import unittest
from scripts.validations import BlueprintValidations

extra_dimensions = {'Diagnosis Id', 'Biosource Id', 'Biomaterial Id'}

class BlueprintValidationsTestCase(unittest.TestCase):

    def test_valid_tree_dimensions(self):
        # given
        blueprint = {
            'patient_lvl_column': {
                'metadata_tags': {
                    'dimension': 'patient'
                }
            },
            'diagnosis_lvl_column': {
                'metadata_tags': {
                    'dimension': 'Diagnosis Id'
                }
            },
            'biosource_lvl_column': {
                'metadata_tags': {
                    'dimension': 'Biosource Id'
                }
            },
            'biomaterial_lvl_column': {
                'metadata_tags': {
                    'dimension': 'Biomaterial Id'
                }
            },
        }
        # when
        violations = list(BlueprintValidations(extra_dimensions).collect_tree_node_dimension_violations(blueprint))
        # then
        self.assertEqual(violations, [])

    def test_no_dimension_specified_violations(self):
        # given
        blueprint = {
            'no_dim_meta_column1': {},
            'no_dim_meta_column2': { 'metadata_tags': {} },
            'no_dim_meta_column3': { 'metadata_tags': { 'key': 'value' } },
        }
        # when
        violations = list(BlueprintValidations(extra_dimensions).collect_tree_node_dimension_violations(blueprint))
        # then
        self.assertEqual(violations, [
            'no_dim_meta_column1: No dimension metadata tag specified.',
            'no_dim_meta_column2: No dimension metadata tag specified.',
            'no_dim_meta_column3: No dimension metadata tag specified.',
        ])

    def test_unknown_dimension_specified_violations(self):
        # given
        blueprint = {
            'unknown_dim_meta_column1': { 'metadata_tags': { 'dimension': 'diagnosis' } },
            'unknown_dim_meta_column2': { 'metadata_tags': { 'dimension': 'Patient Id' } },
            'unknown_dim_meta_column3': { 'metadata_tags': { 'dimension': 'DIAGNOSIS ID' } },
        }
        # when
        violations = list(BlueprintValidations(extra_dimensions).collect_tree_node_dimension_violations(blueprint))
        # then
        self.assertEqual(violations, [
            'unknown_dim_meta_column1: "diagnosis" dimension is not recognised.',
            'unknown_dim_meta_column2: "Patient Id" dimension is not recognised.',
            # dimension matters
            'unknown_dim_meta_column3: "DIAGNOSIS ID" dimension is not recognised.'
        ])


if __name__ == '__main__':
    unittest.main()
