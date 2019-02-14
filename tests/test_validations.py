import unittest
from scripts.validations import collect_tree_node_dimension_violations

class ValidationsTestCase(unittest.TestCase):

    def test_valid_tree_dimensions(self):
        # given valid dimensions in different character case
        blueprint = {
            'patient_lvl_column': {
                'metadata_tags': {
                    'dimension': 'patient'
                }
            },
            'diagnosis_lvl_column': {
                'metadata_tags': {
                    'dimension': 'diagnosis id'
                }
            },
            'biosource_lvl_column': {
                'metadata_tags': {
                    'dimension': 'BIOSOURCE ID'
                }
            },
            'biomaterial_lvl_column': {
                'metadata_tags': {
                    'dimension': 'Biomaterial ID'
                }
            },
        }
        # when
        violations = collect_tree_node_dimension_violations(blueprint)
        # then
        self.assertEqual(violations, [])


if __name__ == '__main__':
    unittest.main()
