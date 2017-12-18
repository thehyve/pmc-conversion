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
        file_headers: A dictionary with files from the file list as keys and the expected headers for each file as a list.

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
        self.file_dir = self.config.get('GENERAL','file_dir')
        self.file_list = read_config_list_from_file('file_list_config')
        self.file_headers = read_config_dict_from_file('file_header_config')
        self.header_mapping = read_config_dict_from_file('header_mapping_config')

# This stuff is not needed but fun to have.
    def add_to_file_list(self, items):
        if isinstance(items, list):
            self.file_list.append(items)

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
    error_messages = [] # TODO: implement python logger?
    config = Config(config_file)
    #print(config.file_list)
    #print(config.config_file)
    #print(config.file_dir)

    ## Test file dir !!!
    #os.chdir(config.file_dir)

    files = {'individual': {}, 'diagnosis': {}, 'biosource': {}, 'biomaterial': {}, 'study': {}}
    all_files = []
    for path, dir, filenames in os.walk(config.file_dir):
        all_files = all_files + filenames
        for filename in filenames:
            working_dir = path.rsplit('/', 1)[1].upper()
            if dir == [] and working_dir in ['EPD', 'LIMMS', 'STUDY']:
                #print(path,filename)
                file = '/'.join([path, filename])
                encoding = get_encoding(file)

                if filename in config.header_mapping:
                    header_map = config.header_mapping[filename]
                else:
                    header_map = None

                df = input_file_to_df(file, encoding, column_mapping=header_map)
                # Check if headers are present
                file_type = determine_file_type(df)
                # TODO: extend the error checking and input config so files do not need all the data but together
                # TODO: have a set of needed columns
                error_messages = error_messages + (check_file_header(df.columns, config.file_headers[file_type], filename))

                files[file_type].update({filename:df})

    error_messages = error_messages + check_file_list(all_files, config.file_list)
    # Temporarily disabled
    # print_errors(error_messages)


    sys.exit(1)


# TODO:
# Steps to implement:
# - Check if all expected files are present
# - Check encoding --> use correct encoding to read the file

# - Check if the files have the correct headers

# - Combine files into one big overview

# - Output to a tsv file --> Encode to UTF-8 format

# Things to keep in mind:
# - need a mapping to deduplicate potential double values from the CSR --> requires a order of correct datafiles
# - find a way to manage inconsitency in the column headers --> toupper?
# - config file needs mappings




## Check file encoding, assuming utf-8.
def get_encoding(file_name):
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()


def input_file_to_df(file_name, encoding, seperator='\t', column_mapping = None):
    df = pd.read_csv(file_name, sep=seperator, encoding=encoding)
    df.columns = map(lambda x: str(x).upper(), df.columns)
    if column_mapping:
        df.columns = apply_header_map(df, column_mapping)

    return df


def apply_header_map(df, header):
    new_header = []
    for i in df.columns:
        if i in header:
            new_header.append(header[i].upper())
        else:
            new_header.append(i.upper())
    return new_header


def determine_file_type(df):
    col = df.columns
    if 'BIOSOURCE_ID' in col:
        return 'biosource'
    if 'BIOMATERIAL_ID' in col:
        return 'biomaterial'
    if 'DIAGNOSIS_ID' in col:
        return 'diagnosis'
    if 'STUDY_ID' in col:
        return 'study'
    if 'INDIVIDUAL_ID' in col:
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
    ## Returns a list of strings with file name and missing headers and error msg (again list of strings)
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



if __name__ == '__main__':
    main()
