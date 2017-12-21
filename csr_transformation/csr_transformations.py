import pandas as pd
import chardet
import os
import configparser
import click
import sys
import json


class Config(object):
    """A config object containing all information needed to transform the input data to the Central Subject Registry

    Attributes:
        config_file: The name of the config file passed as input to the script
        file_list: A list of files that is expected as input
        file_headers: A dict with files from the file list as keys and the expected headers for each file as a list.

    """
    def __init__(self, config_file):

        def read_config_dict_from_file(config_item):
            with open(self.config.get('GENERAL', config_item)) as f:
                dict_ = json.loads(f.read())
            # dict_upper = {k.upper():v for k,v in dict_.items()}
            return dict_

        def read_config_list_from_file(config_item):
            with open(self.config.get('GENERAL', config_item)) as f:
                list_ = [line.rstrip('\n') for line in f]
            return list_

        self.config_file = config_file.name
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)
        self.file_dir = self.config.get('GENERAL', 'file_dir')
        self.file_list = read_config_list_from_file('file_list_config')
        self.file_headers = read_config_dict_from_file('file_header_config')
        self.file_headers_list = sum(self.file_headers.values(), [])
        self.header_mapping = read_config_dict_from_file('header_mapping_config')
        self.file_order = read_config_list_from_file('file_order_config')

        self.output_file = self.config.get('OUTPUT', 'target_file')

# This stuff is not needed but fun to have.
    def add_to_file_list(self, items):
        if isinstance(items, list):
            self.file_list += items

    def add_to_file_headers(self, items):
        if isinstance(items, dict):
            self.file_headers.update(items)

    def set_file_header(self, new_header):
        self.file_headers = new_header

    def set_file_list(self, new_file_list):
        self.file_list = new_file_list


@click.command()
@click.argument('config_file', type=click.File('r'))
def main(config_file):
    error_messages = []  # TODO: implement python logger?
    config = Config(config_file)

    files = {'individual': {}, 'diagnosis': {}, 'biosource': {}, 'biomaterial': {}, 'study': {}}
    all_files = []
    for path, dir_, filenames in os.walk(config.file_dir):
        all_files += filenames
        for filename in filenames:
            working_dir = path.rsplit('/', 1)[1].upper()
            if dir_ == [] and working_dir in ['EPD', 'LIMMS', 'STUDY']:
                # print(path,filename)
                file = '/'.join([path, filename])
                encoding = get_encoding(file)

                # Implement column mapping
                if filename in config.header_mapping:
                    header_map = config.header_mapping[filename]
                else:
                    header_map = None

                # Read data from file and create a pandas DataFrame. If a header mapping is provided the columns
                # are mapped before the DataFrame is returned
                df = input_file_to_df(file, encoding, column_mapping=header_map)

                columns = df.columns
                # Check if headers are present
                file_type = determine_file_type(columns)
                # TODO: extend the error checking and input config so files do not need all the data but together
                # TODO: have a set of needed columns
                error_messages = error_messages + (check_file_header(columns, config.file_headers[file_type], filename))

                files[file_type].update({filename: df})

    error_messages += check_file_list(all_files, config.file_list)
    # Temporarily disabled
    # print_errors(error_messages)

    # Merge multiple individual data objects into one dataframe
    # - Requires hierarchy thingy --> Taken from file order
    # - Requires data deduplication
    # Figure out how I should take into account data from different sources
    # TODO: figure out if id_cols should be moved to a config file
    individuals = merge_data_frames(df_dict=files['individual'],
                                    file_order=config.file_order,
                                    id_col='INDIVIDUAL_ID')

    # Merge diagnosis data files
    diagnosis = merge_data_frames(df_dict=files['diagnosis'],
                                  file_order=config.file_order,
                                  id_col=['DIAGNOSIS_ID', 'INDIVIDUAL_ID'])

    # Merge study data files
    studies = merge_data_frames(df_dict=files['study'],
                                file_order=config.file_order,
                                id_col=['STUDY_ID', 'INDIVIDUAL_ID'])

    # Add patient identifiers to the biomaterial files.
    # Steps:
    # - Merge biomaterial files into one data file based on biomaterial id
    biomaterials = merge_data_frames(df_dict=files['biomaterial'],
                                     file_order=config.file_order,
                                     id_col=['BIOMATERIAL_ID', 'SRC_BIOSOURCE_ID'])

    # - Merge biosource files into one data file based on biosource id
    biosources = merge_data_frames(df_dict=files['biosource'],
                                   file_order=config.file_order,
                                   id_col=['BIOSOURCE_ID', 'INDIVIDUAL_ID', 'DIAGNOSIS_ID'])

    # - Add INDIVIDUAL_ID and DIAGNOSIS_ID to biomaterials using the biosources
    biomaterials = add_biosource_identifiers(biosources, biomaterials)

    # TODO: Add advanced deduplication for double values from for example individual and diagnosis.
    # TODO: idea to capture this in a config file where per column for the CSR the main source is described.
    # TODO: This could reuse the file_header mapping to create subselections and see if there are duplicates.
    # TODO: The only thing that is not covered is some edge cases where a data point from X should lead instead of the
    # TODO: normal Y data point.

    # Concat all data starting with individual into the CSR dataframe, study, diagnosis, biosource and biomaterial
    subject_registry = pd.concat([individuals, diagnosis, studies, biosources, biomaterials])
    # - Check if all CSR headers are in the merged dataframe
    missing_header = [l for l in config.file_headers_list if l not in subject_registry.columns]
    if len(missing_header) > 0:
        print('[ERROR] Missing columns from Subject Registry data model:\n', missing_header)
        sys.exit(9)

    subject_registry.to_csv(config.output_file, sep='\t', index=False)
# TODO:
# - Output to a tsv file --> Encode to UTF-8 format
# Things to keep in mind:
# - need a mapping to deduplicate potential double values from the CSR --> requires a order of correct datafiles
# - find a way to manage inconsitency in the column headers --> toupper?
# - config file needs mappings
# - Throw error if patient ID not found
    sys.exit(0)


def get_encoding(file_name):
    """Open the file and determine the encoding, returns the encoding cast to lower"""
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()


def input_file_to_df(file_name, encoding, seperator='\t', column_mapping=None):
    """Read in a DataFrame from a plain text file. Columns are cast to uppercase.
     If a column_mapping is specified the columns will also be tried to map"""
    df = pd.read_csv(file_name, sep=seperator, encoding=encoding)
    df.columns = map(lambda x: str(x).upper(), df.columns)
    if column_mapping:
        df.columns = apply_header_map(df.columns, column_mapping)
    return df


def apply_header_map(df_columns, header):
    """Generate a new header for a pandas Dataframe. Returns uppercased column names from the header"""
    new_header = []
    for i in df_columns:
        if i in header:
            new_header.append(header[i].upper())
        else:
            new_header.append(i.upper())
    return new_header


def determine_file_type(columns):
    """Based on pandas DataFrame columns determine the type of entity to assign to the dataframe.

    :param columns:
    :return:
    """
    if 'BIOMATERIAL_ID' in columns:
        return 'biomaterial'
    if 'BIOSOURCE_ID' in columns:
        return 'biosource'
    if 'DIAGNOSIS_ID' in columns:
        return 'diagnosis'
    if 'STUDY_ID' in columns:
        return 'study'
    if 'INDIVIDUAL_ID' in columns:
        return 'individual'
    else:
        # TODO: implement error messages
        # Fails if none of the key identifiers are present
        print('\nI am not working! - No Key Identifier found: Individual, diagnosis, study, biosource or biomaterial\n')


def check_file_header(file_header, expected_header, name):
    msgs = []
    for column in expected_header:
        if column not in file_header:
            msgs.append('[ERROR] {0} header: expected {1} as a column but not found'.format(name, column))
    # Returns a list of strings with file name and missing headers and error msg (again list of strings)
    return msgs


def check_file_list(files, expected_files):
    msgs = []
    for file in expected_files:
        if file not in files:
            msgs.append('[ERROR] data file: {} expected but not found'.format(file))

    return msgs


def print_errors(messages):
    # TODO: Document function
    # just a list of files for now, Don't start with blocks
    if not messages:
        print('No ERRORS found in the input files!!')
    else:
        print('--------- input file ERRORS ---------')
        for msg in messages:
            print(msg)
        sys.exit(9)


def merge_data_frames(df_dict, file_order, id_col):
    df_list = [f for f in file_order if f in df_dict.keys()]
    ref_df = df_dict[df_list[0]]
    for i in df_list[1:]:
        ref_df = ref_df.merge(df_dict[i], how='outer', on=id_col, suffixes=('_x', '_y'))
        combine_column_data(ref_df)
    return ref_df


# TODO: come up with better name
def combine_column_data(df):
    col_map = {}
    for i in df.columns:
        i_split = i.rsplit('_', 1)
        if len(i_split) > 1 and i_split[1] in ['x', 'y']:
            if not i_split[0] in col_map.keys():
                col_map[i_split[0]] = [i_split[1]]
            else:
                col_map[i_split[0]].append(i_split[1])

    for key, value in col_map.items():
        df[key] = pd.np.nan
        for item in value:
            df[key] = df[key].combine_first(df.pop('_'.join([key, item])))


def add_biosource_identifiers(biosource, biomaterial, biosource_merge_on='BIOSOURCE_ID',
                              biomaterial_merge_on='SRC_BIOSOURCE_ID'):
    """Merges two pandas DataFrames adding predefined columns from the biosource to the biomaterial

    predefined columns to be added: INDIVIDUAL_ID, DIAGNOSIS_ID

    :param biosource: a pandas DataFrame with identifiers that have to be merged into biomaterial
    :param biomaterial: a pandas DataFrame to which the identifiers will be added
    :param biosource_merge_on: A column name
    :param biomaterial_merge_on: The column containing the identifiers to use
    :return: biomaterial DataFrame with added predefined identifier columns
    """
    select_header = ['INDIVIDUAL_ID', 'DIAGNOSIS_ID'] + biomaterial.columns.tolist()
    id_look_up = biosource[['INDIVIDUAL_ID', 'BIOSOURCE_ID', 'DIAGNOSIS_ID']]
    df = biomaterial.merge(id_look_up, how='left',
                           left_on=biomaterial_merge_on, right_on=biosource_merge_on)[select_header]
    return df


if __name__ == '__main__':
    main()
