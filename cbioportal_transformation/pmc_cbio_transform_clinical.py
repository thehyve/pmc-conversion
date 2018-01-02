#!/usr/bin/env python2.7

### Code to transform clinical data
### Author: Sander Tan, The Hyve

import sys
sys.dont_write_bytecode = True

import pandas as pd
import numpy as np
import argparse
import os
import re
import pmc_cbio_create_metafile

### Create maps to rename column names

### Rename attributes before creating header
### These are the attribute names that will show up in cBioPortal UI.
### This is useful for when the column name is too long, or if you just want to modify the text.
### These will end up in the 1st and 2nd line of the staging file header,
### thereby defining the 'name' and 'description' that will show up in the UI.
RENAME_BEFORE_CREATING_HEADER_MAP = {}

### Rename attributes after creating header
### These are the attribute names that will be saved as column names in the database
### This is only necessary for cBioPortal specific columns, such as 'Overal Survival Status' - 'OS_STATUS'
### Aspecific columns are automatically parsed to cBioPortal format, such as '% tumor cells' -> 'TUMOR_CELLS'
### Please make sure the SAMPLE_ID column name is created
RENAME_AFTER_CREATING_HEADER_MAP = {'individual_id': 'PATIENT_ID',
                                    'src_biosource_id': 'SAMPLE_ID'}

### Rename values in OS_STATUS and DFS_STATUS columns
RENAME_OS_STATUS_MAP = {}
RENAME_DFS_STATUS_MAP = {}

def create_clinical_header(df):
    """ Function to create header in cBioPortal format for clinical data
    """

    ### Create attribute name and attribute description
    header_data = pd.DataFrame(data = df.columns)
    header_data[1] = header_data

    ### Data type header line
    type_values = []
    for i in df.dtypes:
        if i in ['int64', 'float64']:
            type_values.append('NUMBER')
        else:
            type_values.append('STRING')

    ### Create attribute type and attribute priority
    header_data[2] = type_values
    header_data[3] = 1

    ### Transpose to put in correct format
    header_data = header_data.transpose()

    ### Transform to dataframe
    header_data = pd.DataFrame(header_data)
    header_data.columns = df.columns

    ### Adding hash # to the first column
    header_data.iloc[:,0] = '#' + header_data.iloc[:,0].astype(str)

    return header_data


def check_integer_na_columns(numerical_column):
    """ Integer columns containing NA values are converted to Float in pandas
        This function checks for which columns this is the case
    """
    ### Only check columns that contain NA
    if numerical_column.isnull().values.any() and not numerical_column.isnull().values.all():
        for i in numerical_column:
            if str(i) != 'nan':
                decimal_value = str(i).split('.')[1]
                ### If float is other than 0, we know for sure it's not an integer column
                if float(decimal_value) != 0:
                    return False
        ### If loop ends by only encountering 0s, return true
        return True
    else:
        return False


def fix_integer_na_columns(df):
    """ Integer columns containing NA values are converted to Float in pandas
        This function selects these columns and converts them to back to integers.
    """

    # Select data with floats
    numerical_data = df.select_dtypes(include=[np.float64])

    ### Round values to 4 decimals
    #clinical_data.loc[:, numerical_data.columns] = np.round(numerical_data, 4)

    # Select columns which contain integers with NA values (numpy has converted them to floats)
    integer_na_columns = numerical_data.columns[numerical_data.apply(check_integer_na_columns, axis = 0)]

    # Convert integer columns with NA to string(integer) columns
    if len(integer_na_columns) > 0:
        df.loc[:, integer_na_columns] = df.loc[:, integer_na_columns].applymap(lambda x: str(x).split('.')[0].replace('nan', 'NA'))
    return df

def remove_empty_columns(df):
    """ Columns without any values should be removed from dataframe. """
    for column in df:
        if df[column].isnull().values.all():
            df.drop(column, axis = 1, inplace=True)
    return df

def transform_clinical_data(clinical_inputfile, output_dir, clinical_type, study_id):
    """ Converting the input file to a cBioPortal staging file.
    """

    ### Check arguments
    if clinical_type == 'sample':
        if specimen_file == '':
            print '-s should contain specimen file for clinical sample table'
            sys.exit(1)
    elif clinical_type == 'patient':
        pass
    else:
        print "-c should contain contain 'sample' or 'patient'"
        sys.exit(1)

    ### Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    ### Loading clinical data
    clinical_data = pd.read_csv(clinical_inputfile, sep='\t', na_values=[''])

    ###############################
    ### Modify column names #######
    ###############################

    ### Strip possible trailing white spaces
    clinical_data = clinical_data.rename(columns=lambda x: x.strip())

    ## Remove empty columns
    clinical_data = remove_empty_columns(clinical_data)
    
    ### Rename attributes before creating header
    ### These are the attribute names that will show up in cBioPortal UI
    clinical_data.rename(columns= RENAME_BEFORE_CREATING_HEADER_MAP, inplace = True)

    ### Set datatype of specific columns before creating header
    # if '' in clinical_data.columns:
    #     clinical_data[''] = clinical_data[''].astype(object)

    ### Create header
    clinical_header = create_clinical_header(clinical_data)

    ### Rename attributes after creating header
    ### These are the attribute names for the database
    clinical_data.rename(columns= RENAME_AFTER_CREATING_HEADER_MAP, inplace = True)

    ### Set datatype of specific columns before after header
    # if '' in clinical_header.columns:
    #     clinical_header.loc[2, ''] = 'STRING'

    ### Remove symbols from attribute names and make them uppercase
    clinical_data = clinical_data.rename(columns=lambda s: re.sub('[^0-9a-zA-Z_]+', '_', s))
    clinical_data = clinical_data.rename(columns=lambda x: x.strip('_'))
    clinical_data.columns = clinical_data.columns.str.upper()

    ################################
    ### Modify values in columns ###
    ################################

    ### Remap values to cBioPortal format for certain columns
    if clinical_type == 'patient':
        ### Remap overal survival
        if 'OS_STATUS' in clinical_data.columns:
            clinical_data.replace({'OS_STATUS': RENAME_OS_STATUS_MAP})

        ### Remap disease free survival
        if 'DFS_STATUS' in clinical_data.columns:
            clinical_data.replace({'DFS_STATUS': RENAME_DFS_STATUS_MAP})

        ### Possibly convert days to months
        # if 'OS_MONTHS' in clinical_data.columns:
        #     ### Convert days to months
        #     clinical_data.loc[clinical_data['OS_MONTHS'].notnull(), 'OS_MONTHS'] = (clinical_data.loc[clinical_data['OS_MONTHS'].notnull(), 'OS_MONTHS'] / 30).round().astype(int)

        ### Possibly convert days to months
        # if 'DFS_MONTHS' in clinical_data.columns:
        #     ### Convert days to months
        #     clinical_data.loc[clinical_data['DFS_MONTHS'].notnull(), 'DFS_MONTHS'] = (clinical_data.loc[clinical_data['DFS_MONTHS'].notnull(), 'DFS_MONTHS'] / 30).round().astype(int)

    # Fix integers that are converted to floats due to NA in column
    if len(clinical_data.select_dtypes(include=[np.float64]).columns) > 0:
        clinical_data = fix_integer_na_columns(clinical_data)

    ### Fill NA values
    clinical_data = clinical_data.fillna('NA')

    ### Check if column names are unique.
    ### In case of duplicates, rename them manually before or after header creation
    if not len(set(clinical_data.columns.tolist())) == len(clinical_data.columns.tolist()):
        print('Attribute names are not unique, not writing data_clinical. Rename them using the remap dictionary.')
        sys.exit(1)
        
    ##############################
    ### Write output #############
    ##############################
    ### Writing clinical patient file
    clinical_filename = os.path.join(output_dir, 'data_clinical_%s.txt' % clinical_type)
    clinical_header.to_csv(clinical_filename, sep='\t', index=False, header=False, mode = 'w')
    clinical_data.to_csv(clinical_filename, sep='\t', index=False, header=True, mode = 'a')

    ### Set clinical type for meta file
    if clinical_type == 'sample':
        meta_datatype = 'SAMPLE_ATTRIBUTES'
    elif clinical_type == 'patient':
        meta_datatype = 'PATIENT_ATTRIBUTES'
    else:
        print "Unknown clinical data type"
        sys.exit(1)

    ### Create meta file
    meta_content = pmc_cbio_create_metafile.create_meta_content(study_id, 'CLINICAL', meta_datatype, 'data_clinical_%s.txt' % clinical_type)
    meta_filename = os.path.join(output_dir, 'meta_clinical_patient.txt')
    pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

    return


def main(clinical_inputfile, output_dir, clinical_type, study_id):
    transform_clinical_data(clinical_inputfile, output_dir, clinical_type, study_id)
    return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        usage = '-i <input_file> -o <dir_for_output_files> -c <datatype: sample or patient>',
        description = 'Read input and output filenames.')

    requiredNamed = parser.add_argument_group('required named arguments')

    requiredNamed.add_argument('-i', '--input_file',
        required = True,
        help = 'Data file containing donor/patient/sample information.')

    requiredNamed.add_argument('-o', '--dir_for_output_files',
        required = True,
        help = 'Directory in which the staging file created by this \
script will be placed.')

    requiredNamed.add_argument('-c', '--clinical_type',
        required = True,
        help = 'Specify whether the data is a patient or sample file.')

    requiredNamed.add_argument('-s', '--study_id',
        required = True,
        help = 'Specify study id.')

    args = parser.parse_args()

    main(args.input_file, args.dir_for_output_files, args.clinical_type, args.study_id)
