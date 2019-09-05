#!/usr/bin/env python3

# Code to transform clinical data
# Author: Sander Tan, The Hyve

import argparse
import json
import logging
import os
import re
import sys

import numpy as np
import pandas as pd
from csr.csr import CentralSubjectRegistry
from csr.study_registry_reader import SubjectRegistryReader

from .cbio_create_metafile import create_meta_content

sys.dont_write_bytecode = True

logger = logging.getLogger(__name__)
logger.name = logger.name.rsplit('.', 1)[1]

# Create map to rename column names

# Rename attributes after creating header
# These are the attribute names that will be saved as column names in the database
# This is only necessary for cBioPortal specific columns, such as 'Overal Survival Status' - 'OS_STATUS'
# Other columns are automatically parsed to cBioPortal format, for example '% tumor cells' -> 'TUMOR_CELLS'
# Please make sure the SAMPLE_ID column name is created
RENAME_AFTER_CREATING_HEADER_MAP = {}

# Rename values in OS_STATUS and DFS_STATUS columns
RENAME_OS_STATUS_MAP = {}
RENAME_DFS_STATUS_MAP = {}

# Force some datatypes to be remappad to STRING
FORCE_STRING_LIST = ['CENTER_TREATMENT', 'CID', 'DIAGNOSIS_ID', 'GENDER', 'IC_DATA', 'IC_LINKING_EXT', 'IC_MATERIAL',
                     'IC_TYPE', 'INDIVIDUAL_STUDY_ID', 'TOPOGRAPHY', 'TUMOR_TYPE']


def create_clinical_header(df):
    """ Function to create header in cBioPortal format for clinical data
    """

    # Create attribute name and attribute description
    header_data = pd.DataFrame(data=df.columns)
    header_data[1] = header_data

    # Data type header line
    type_values = []
    for i in df.dtypes:
        if np.issubdtype(i, np.number):
            type_values.append('NUMBER')
        else:
            type_values.append('STRING')

    # Create attribute type and attribute priority
    header_data[2] = type_values
    header_data[3] = 1

    # Transpose to put in correct format
    header_data = header_data.transpose()

    # Transform to dataframe
    header_data = pd.DataFrame(header_data)
    header_data.columns = df.columns

    # Adding hash # to the first column
    header_data.iloc[:, 0] = '#' + header_data.iloc[:, 0].astype(str)

    return header_data


def subject_registry_to_clinical_data_df(subject_registry: CentralSubjectRegistry, clinical_type, description_map) -> pd.DataFrame:

    # Loading clinical data
    patient_data = pd.DataFrame.from_records([s.__dict__ for s in subject_registry.individuals])
    diagnosis_data = pd.DataFrame.from_records([s.__dict__ for s in subject_registry.diagnoses])
    biosource_data = pd.DataFrame.from_records([s.__dict__ for s in subject_registry.biosources])
    biomaterial_data = pd.DataFrame.from_records([s.__dict__ for s in subject_registry.biomaterials])

    # Rename column, else duplicate columns
    entity_map = {
        'diagnosis': diagnosis_data,
        'biosource': biosource_data,
        'biomaterial': biomaterial_data,
        'individual': patient_data
    }

    for key, descriptions in description_map.items():
        entity_map[key].columns = [x.upper() for x in entity_map[key].columns]
        entity_map[key].rename(columns=descriptions, inplace=True)

    # For patient data, no merging is required
    if clinical_type == 'patient':
        return patient_data
    elif clinical_type == 'sample':
        # Merge diagnoses, biosources and biomaterials
        diagnosis_biosource_data = pd.merge(biosource_data, diagnosis_data, how='left', on=['DIAGNOSIS_ID'])
        sample_data = pd.merge(biomaterial_data, diagnosis_biosource_data, how='left',
                               left_on=['SRC_BIOSOURCE_ID'], right_on=['BIOSOURCE_ID'])
        # remove suffixes for biomaterials and biosources
        if 'SRC_BIOSOURCE_ID_x' in sample_data.columns:
            x_ = sample_data['SRC_BIOSOURCE_ID_x']
            y_ = sample_data['SRC_BIOSOURCE_ID_y']
            sample_data['SRC_BIOSOURCE_ID'] = x_.combine_first(y_)
            sample_data = sample_data.drop(columns=['SRC_BIOSOURCE_ID_x','SRC_BIOSOURCE_ID_y'])
        # remove suffixes for patients
        if 'PATIENT_ID_x' in sample_data.columns:
            x_ = sample_data['PATIENT_ID_x']
            y_ = sample_data['PATIENT_ID_y']
            sample_data['PATIENT_ID'] = x_.combine_first(y_)
            sample_data = sample_data.drop(columns=['PATIENT_ID_x','PATIENT_ID_y'])
        sample_data['SAMPLE_ID'] = diagnosis_biosource_data['BIOSOURCE_ID'] + "_" + biomaterial_data['BIOMATERIAL_ID']
        return sample_data
    else:
        logger.error('Clinical type not recognized, exiting')
        sys.exit(1)


def check_integer_na_columns(numerical_column):
    """ Integer columns containing NA values are converted to Float in pandas
        This function checks for a column if it only contains integers (.0)
        In this case we attempt to convert all columns, not just the columns
        that contain an NA value, because we sliced the dataframes from a bigger
        dataframe that could contain NA values.
    """
    for i in numerical_column:
        if str(i) != 'nan':
            decimal_value = str(i).split('.')[1]
            # If float is other than 0, we know for sure it's not an integer column
            if float(decimal_value) != 0:
                return False
    # If loop ends by only encountering .0, return true
    return True


def fix_integer_na_columns(df):
    """ Integer columns containing NA values are converted to Float in pandas
        This function selects these columns and converts them to back to integers
    """

    # Select data with floats
    numerical_data = df.select_dtypes(include=[np.float64])

    # Round values to 4 decimals
    # clinical_data.loc[:, numerical_data.columns] = np.round(numerical_data, 4)

    # Select columns which contain integers with NA values (numpy has converted them to floats)
    integer_na_columns = numerical_data.columns[numerical_data.apply(check_integer_na_columns, axis=0)]

    # Convert integer columns with NA to string(integer) columns
    if len(integer_na_columns) > 0:
        df.loc[:, integer_na_columns] = df.loc[:, integer_na_columns].applymap(
            lambda x: str(x).split('.')[0].replace('nan', 'NA'))
    return df


def transform_clinical_data(input_dir, output_dir, clinical_type, study_id, description_map) -> pd.DataFrame:
    """ Converting the input file to a cBioPortal staging file.
    """
    subject_registry_reader = SubjectRegistryReader(input_dir)
    subject_registry: CentralSubjectRegistry = subject_registry_reader.read_subject_registry()

    # Loading clinical data
    clinical_data = subject_registry_to_clinical_data_df(subject_registry, clinical_type, description_map)

    # Modify column names
    # Strip possible trailing white spaces
    clinical_data = clinical_data.rename(columns=lambda x: x.strip())

    # Remove empty columns
    clinical_data.dropna(axis=1, how='all', inplace=True)

    # Create header
    clinical_header = create_clinical_header(clinical_data)

    # Rename attributes after creating header
    # These are the attribute names for the database
    clinical_data.rename(columns=RENAME_AFTER_CREATING_HEADER_MAP, inplace=True)

    # TODO: Major - Check FORCE_STRING_LIST assumption --> make configurable?
    # Set datatype of specific columns before after header
    for string_column_name in FORCE_STRING_LIST:
        if string_column_name in clinical_header.columns:
            clinical_header.loc[2, string_column_name] = 'STRING'

    # Remove symbols from attribute names and make them uppercase
    clinical_data = clinical_data.rename(columns=lambda s: re.sub('[^0-9a-zA-Z_]+', '_', s))
    clinical_data = clinical_data.rename(columns=lambda x: x.strip('_'))

    ############################
    # Modify values in columns #
    ############################

    # Remap values to cBioPortal format for certain columns
    if clinical_type == 'patient':
        # Remap overal survival
        if 'OS_STATUS' in clinical_data.columns:
            clinical_data.replace({'OS_STATUS': RENAME_OS_STATUS_MAP})

        # Remap disease free survival
        if 'DFS_STATUS' in clinical_data.columns:
            clinical_data.replace({'DFS_STATUS': RENAME_DFS_STATUS_MAP})

        # # Possibly convert days to months
        # if 'OS_MONTHS' in clinical_data.columns:
        #     # Convert days to months
        #     clinical_data.loc[clinical_data['OS_MONTHS'].notnull(), 'OS_MONTHS'] = (clinical_data
        # .loc[clinical_data['OS_MONTHS'].notnull(), 'OS_MONTHS'] / 30).round().astype(int)

        # # Possibly convert days to months
        # if 'DFS_MONTHS' in clinical_data.columns:
        #     # Convert days to months
        #     clinical_data.loc[clinical_data['DFS_MONTHS'].notnull(), 'DFS_MONTHS'] = (clinical_data
        # .loc[clinical_data['DFS_MONTHS'].notnull(), 'DFS_MONTHS'] / 30).round().astype(int)

    # Fix integers that are converted to floats due to NA in column
    if len(clinical_data.select_dtypes(include=[np.float64]).columns) > 0:
        clinical_data = fix_integer_na_columns(clinical_data)

    # Check if column names are unique.
    # In case of duplicates, rename them manually before or after header creation
    # TODO: Nice to have - Extract mapping dictionaries to external config, now they are hardcoded
    if not len(set(clinical_data.columns.tolist())) == len(clinical_data.columns.tolist()):
        logger.error('Attribute names not unique, not writing data_clinical. Rename them using the remap dictionary.')
        sys.exit(1)

    # Drop duplicate rows, this does not check for duplicate sample ID's
    # cd_row_count = clinical_data.shape[0]
    # clinical_data = clinical_data.drop_duplicates(keep='first')
    # logger.debug('Found and dropped {} duplicates in the {} data'
    #              .format(cd_row_count - clinical_data.shape[0], clinical_type))

    write_clinical(clinical_data, clinical_header, clinical_type, output_dir, study_id)

    return clinical_data


def write_clinical(clinical_data, clinical_header, clinical_type, output_dir, study_id):
    # Writing clinical patient file
    clinical_filename = os.path.join(output_dir, 'data_clinical_{}.txt'.format(clinical_type))
    clinical_header.to_csv(clinical_filename, sep='\t', index=False, header=False, mode='w')
    clinical_data.to_csv(clinical_filename, sep='\t', index=False, header=True, mode='a')
    # Set clinical type for meta file
    if clinical_type == 'sample':
        meta_datatype = 'SAMPLE_ATTRIBUTES'
    elif clinical_type == 'patient':
        meta_datatype = 'PATIENT_ATTRIBUTES'
    else:
        logger.error("Unknown clinical data type")
        sys.exit(1)
    # Create meta file
    meta_filename = os.path.join(output_dir, 'meta_clinical_{}.txt'.format(clinical_type))
    create_meta_content(file_name=meta_filename,
                        cancer_study_identifier=study_id,
                        genetic_alteration_type='CLINICAL',
                        datatype=meta_datatype,
                        data_filename='data_clinical_{}.txt'.format(clinical_type))


def main(clinical_inputfile, output_dir, clinical_type, study_id, description_map):

    if os.path.isfile(description_map):
        with open(description_map, 'r') as dmf:
            description_dict = json.load(dmf)
    else:
        print('Description map file is not a file')
        sys.exit(1)

    transform_clinical_data(clinical_inputfile, output_dir, clinical_type, study_id, description_dict)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        usage='-i <input_file> -o <dir_for_output_files> -c <datatype: sample or patient>',
        description='Read input and output filenames.')

    requiredNamed = parser.add_argument_group('required named arguments')

    requiredNamed.add_argument('-i', '--input_file',
                               required=True,
                               help='Data file containing donor/patient/sample information.')

    requiredNamed.add_argument('-o', '--dir_for_output_files',
                               required=True,
                               help='Directory in which the staging file created by this script will be placed.')

    requiredNamed.add_argument('-c', '--clinical_type',
                               required=True,
                               help='Specify whether the data is a patient or sample file.')

    requiredNamed.add_argument('-s', '--study_id',
                               required=True,
                               help='Specify study id.')

    requiredNamed.add_argument('-d', '--description_map_file',
                               required=True,
                               help='Specify path to description map file. File is expected to be in JSON format')

    args = parser.parse_args()

    main(args.input_file, args.dir_for_output_files, args.clinical_type, args.study_id, args.description_map_file)
