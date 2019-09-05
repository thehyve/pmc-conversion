import json
from collections import Counter

import pandas as pd
import pytest

from scripts.cbioportal_transformation.cbio_transform_clinical import transform_clinical_data


@pytest.fixture
def patient_clinical_data(tmp_path) -> pd.DataFrame:
    target_path = tmp_path.as_posix()
    output_dir = target_path + '/data'
    input_dir = './test_data/default_data'
    descriptions_file = './test_data/config/cbioportal_header_descriptions.json'
    study_id = 'CSR'
    with open(descriptions_file, 'r') as des:
        description_map = json.loads(des.read())
    return transform_clinical_data(input_dir, output_dir, 'patient', study_id, description_map)


@pytest.fixture
def sample_clinical_data(tmp_path) -> pd.DataFrame:
    target_path = tmp_path.as_posix()
    output_dir = target_path + '/data'
    input_dir = './test_data/default_data'
    descriptions_file = './test_data/config/cbioportal_header_descriptions.json'
    study_id = 'CSR'
    with open(descriptions_file, 'r') as des:
        description_map = json.loads(des.read())
    return transform_clinical_data(input_dir, output_dir, 'sample', study_id, description_map)


def test_patient_clinical_data(patient_clinical_data):
    assert len(patient_clinical_data) == 2
    assert list(patient_clinical_data.get('PATIENT_ID')) == ['P1', 'P2']
    assert Counter(list(patient_clinical_data)) == Counter(['PATIENT_ID', 'TAXONOMY', 'BIRTH_DATE', 'GENDER',
                                                            'IC_TYPE', 'IC_GIVEN_DATE',
                                                            'IC_WITHDRAWN_DATE', 'IC_MATERIAL', 'IC_DATA',
                                                            'IC_LINKING_EXT', 'REPORT_HER_SUSC', 'REPORT_INC_FINDINGS'])


def test_sample_clinical_data(sample_clinical_data):
    assert len(sample_clinical_data) == 4
    assert list(sample_clinical_data.get('SAMPLE_ID')) == ['BS1_BM1', 'BS2_BM2', 'BS3_BM3', 'BS4_BM4']
    assert Counter(list(sample_clinical_data)) == Counter(['ANALYSIS_TYPE', 'BIOMATERIAL_DATE', 'BIOMATERIAL_ID',
                                                           'BIOSOURCE_DATE', 'BIOSOURCE_DEDICATED', 'BIOSOURCE_ID',
                                                           'CENTER_TREATMENT', 'DIAGNOSIS_DATE', 'DIAGNOSIS_ID',
                                                           'DISEASE_STATUS', 'PATIENT_ID', 'LIBRARY_STRATEGY',
                                                           'SAMPLE_ID', 'SRC_BIOMATERIAL_ID', 'TISSUE', 'TOPOGRAPHY',
                                                           'TREATMENT_PROTOCOL', 'TUMOR_PERCENTAGE', 'TUMOR_STAGE',
                                                           'TUMOR_TYPE', 'TYPE', 'SRC_BIOSOURCE_ID'])
