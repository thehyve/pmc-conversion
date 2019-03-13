from scripts.csr_transformations import csr_transformation
from scripts.transmart_transformation import transmart_transformation
from os import makedirs, path, listdir
import unittest
import tempfile

class TransmartTransformationTestCase(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def test_that_transformation_finishes_successfully(self):
        csr_transformation(
            './test_data/default_data',
            self.test_dir,
            './config',
            'data_model.json',
            'column_priority.json',
            'file_headers.json',
            'columns_to_csr.json',
            'csr_transformation_data.tsv',
            'study_registry.tsv',
        )
        transmart_transformation(
            path.join(self.test_dir, 'csr_transformation_data.tsv'),
            path.join(self.test_dir, 'study_registry.tsv'),
            self.test_dir,
            './config',
            'blueprint.json',
            'modifiers.txt',
            'CSR',
            '\\Public Studies\\CSR\\',
            False,
            None
        )
        self.assertEqual(set(listdir(self.test_dir)), {
            'study_registry.tsv',
            'i2b2demodata',
            'i2b2metadata',
            'csr_transformation_data.tsv',
        })
