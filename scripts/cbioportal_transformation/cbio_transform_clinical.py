#!/usr/bin/env python3

# Code to transform clinical data
# Author: Sander Tan, The Hyve

import sys
import pandas as pd
import numpy as np
import argparse
import logging
import json
import os
import re
from .cbio_create_metafile import create_meta_content

sys.dont_write_bytecode = True

logger = logging.getLogger(__name__)
logger.name = logger.name.rsplit('.', 1)[1]

# Create maps to rename column names

# Rename attributes before creating header
# These are the attribute names that will show up in cBioPortal UI.
# This is useful for when the column name is too long, or if you just want to modify the text.
# These will end up in the 1st and 2nd line of the staging file header,
# thereby defining the 'name' and 'description' that will show up in the UI.
RENAME_BEFORE_CREATING_HEADER_MAP = {}

# Rename attributes after creating header
# These are the attribute names that will be saved as column names in the database
# This is only necessary for cBioPortal specific columns, such as 'Overal Survival Status' - 'OS_STATUS'
# Other columns are automatically parsed to cBioPortal format, for example '% tumor cells' -> 'TUMOR_CELLS'
# Please make sure the SAMPLE_ID column name is created
RENAME_AFTER_CREATING_HEADER_MAP = {'INDIVIDUAL_ID': 'Patient ID'}

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


def pmc_data_restructuring(unfiltered_clinical_data, clinical_type, description_map):
    mapped_columns = set()
    for value in description_map.values():
        mapped_columns.update(value.keys())

    clinical_columns = unfiltered_clinical_data.columns.intersection(mapped_columns)

    len_c_c = len(clinical_columns)
    len_mp_c = len(mapped_columns)
    len_unf_c = len(unfiltered_clinical_data.columns)

    logger.info('Found {} out of total {} columns in mapping.'.format(len_c_c, len_mp_c))
    if len_c_c < len_mp_c:
        logger.warning('Columns defined in mapping but missing in the CSR data: {}'.format(
            mapped_columns.difference(clinical_columns))
        )
    if len_c_c < len_unf_c:
        logger.info('Columns found in the CSR data but not defined in the mapping: {}'.format(
            set(unfiltered_clinical_data.columns.difference(clinical_columns))
        ))

    clinical_data = unfiltered_clinical_data[clinical_columns]

    # Add entity of the row (Patient, Diagnosis, Biosource, Biomaterial)
    # Use assign to avoid pandas errors
    clinical_data = clinical_data.assign(Entity='')
    for index, row in clinical_data.iterrows():
        if pd.notnull(row['BIOMATERIAL_ID']):
            clinical_data.loc[index, 'Entity'] = 'Biomaterial'
        elif pd.notnull(row['BIOSOURCE_ID']):
            clinical_data.loc[index, 'Entity'] = 'Biosource'
        elif pd.notnull(row['DIAGNOSIS_ID']):
            clinical_data.loc[index, 'Entity'] = 'Diagnosis'
        else:
            clinical_data.loc[index, 'Entity'] = 'Patient'

    # Create separate data frames per entity
    diagnosis_data = clinical_data.loc[clinical_data['Entity'] == 'Diagnosis', :].drop(
        ['Entity', 'INDIVIDUAL_ID'], axis=1).dropna(axis=1, how='all')
    biomaterial_data = clinical_data.loc[clinical_data['Entity'] == 'Biomaterial', :].drop(
        ['Entity', 'DIAGNOSIS_ID'], axis=1).dropna(axis=1, how='all')
    biosource_data = clinical_data.loc[clinical_data['Entity'] == 'Biosource', :].drop(
        ['Entity', 'INDIVIDUAL_ID'], axis=1).dropna(axis=1, how='all')
    study_data = clinical_data.loc[clinical_data['Entity'] == 'Study', :].drop(
        'Entity', axis=1).dropna(axis=1, how='all')
    patient_data = clinical_data.loc[clinical_data['Entity'] == 'Patient', :].drop(
        'Entity', axis=1).dropna(axis=1, how='all')

    # Rename column, else duplicate columns
    entity_map = {
        'diagnosis': diagnosis_data,
        'biosource': biosource_data,
        'biomaterial': biomaterial_data,
        'individual': patient_data
    }

    for key, descriptions in description_map.items():
        entity_map[key].rename(columns=descriptions, inplace=True)

    # For patient data, no merging is required
    if clinical_type == 'patient':
        return patient_data
    elif clinical_type == 'sample':
        # Merge columns
        biosource_data = pd.merge(biosource_data, diagnosis_data, how='left', on=['DIAGNOSIS_ID'])
        biomaterial_data = pd.merge(biomaterial_data, biosource_data, how='left', on=['BIOSOURCE_ID'])
        biomaterial_data['Sample ID'] = biomaterial_data['BIOSOURCE_ID'] + "_" + biomaterial_data['BIOMATERIAL_ID']

        # In case of clinical sample data, return merged biomaterial dataframe
        return biomaterial_data
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


def transform_clinical_data(clinical_inputfile, output_dir, clinical_type, study_id, description_map):
    """ Converting the input file to a cBioPortal staging file.
    """

    # Loading clinical data
    clinical_data = pd.read_csv(clinical_inputfile, sep='\t', na_values=[''], low_memory=False)

    # PMC data restructuring
    clinical_data = pmc_data_restructuring(clinical_data, clinical_type, description_map)

    # Modify column names
    # Strip possible trailing white spaces
    clinical_data = clinical_data.rename(columns=lambda x: x.strip())

    # Remove empty columns
    clinical_data.dropna(axis=1, how='all', inplace=True)

    # Rename attributes before creating header
    # These are the attribute names that will show up in cBioPortal UI
    #clinical_data.rename(columns=RENAME_BEFORE_CREATING_HEADER_MAP, inplace=True)

    # Create header
    clinical_header = create_clinical_header(clinical_data)

    # Rename attributes after creating header
    # These are the attribute names for the database
    #clinical_data.rename(columns=RENAME_AFTER_CREATING_HEADER_MAP, inplace=True)

    # TODO: Major - Check FORCE_STRING_LIST assumption --> make configurable?
    # Set datatype of specific columns before after header
    for string_column_name in FORCE_STRING_LIST:
        if string_column_name in clinical_header.columns:
            clinical_header.loc[2, string_column_name] = 'STRING'

    # Remove symbols from attribute names and make them uppercase
    clinical_data = clinical_data.rename(columns=lambda s: re.sub('[^0-9a-zA-Z_]+', '_', s))
    clinical_data = clinical_data.rename(columns=lambda x: x.strip('_'))
    clinical_data.columns = clinical_data.columns.str.upper()

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
    cd_row_count = clinical_data.shape[0]
    clinical_data = clinical_data.drop_duplicates(keep='first')
    logger.debug('Found and dropped {} duplicates in the {} data'
                 .format(cd_row_count - clinical_data.shape[0], clinical_type))

    ################
    # Write output #
    ################
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

    if clinical_type == 'sample':
        return clinical_data['SAMPLE_ID'].unique().tolist()
    else:
        #return clinical_data['PATIENT_ID'].unique().tolist()
        # Is not being used so returns an empty list
        return []


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
