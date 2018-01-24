import json
import logging
import os
import sys

import chardet
import click
import pandas as pd

ALLOWED_ENCODINGS = {'utf-8', 'ascii'}


def configure_logging(log_type):
    log_format = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%d-%m-%y %H:%M:%S')
    logging.getLogger().setLevel(logging.INFO)

    # Add console handler
    if log_type in ['console', 'both']:
        console = logging.StreamHandler()
        console.setFormatter(log_format)
        logging.getLogger('').addHandler(console)

    # Add file handler
    if log_type in ['file', 'both']:
        log_file = logging.FileHandler('validation.log')
        log_file.setFormatter(log_format)
        logging.getLogger('').addHandler(log_file)


def read_json_dictionary(path):
    logging.info('Collecting properties from JSON file: {}'.format(path))
    file_prop_dict = json.load(open(path))
    return file_prop_dict


def get_source_files(path, file_prop_dict):
    logging.info('Collecting source files.')
    expected_files = set(file_prop_dict.keys())
    source_paths = {os.path.join(path, file) for file in os.listdir(path)}
    source_paths = {path for path in source_paths if os.path.isfile(path) and
                    not os.path.basename(path).startswith('.') and '~$' not in path}

    source_files = {os.path.basename(path) for path in source_paths}
    valid_files = expected_files.intersection(source_files)

    if expected_files != valid_files:
        missing = expected_files - source_files
        logging.error('({0}/{1}) expected files were not found: {2}'.format(len(missing), len(expected_files), missing))
    else:
        logging.info('All expected files were found in the source folder.')

    additional_files = source_files - expected_files
    if additional_files:
        logging.warning('Additional files were found; these will be ignored: {}'.format(additional_files))

    return {sp for sp in source_paths if os.path.basename(sp) in valid_files}


def get_encoding(file_name):
    """Open the file and determine the encoding, returns the encoding cast to lower"""
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower() if file_encoding else None


def check_arguments(source_dir, header_file):
    if not os.path.isdir(source_dir):
        logging.error("Provided SOURCE_DIR is not a directory: {}".format(source_dir))
        sys.exit(1)
    if not os.path.isfile(header_file) or os.path.splitext(header_file)[1].lower() != '.json':
        logging.error("Provided HEADER_FILE is not a valid json file: {}".format(header_file))
        sys.exit(1)


def check_date_fields(file_prop_dict, filename, file_header_fields, df):
    expected_date_fields = file_prop_dict[filename].get('date_columns')
    expected_date_fields = {field.upper() for field in expected_date_fields} if expected_date_fields else None
    if expected_date_fields:
        date_cols_present = {field for field in file_header_fields if field in expected_date_fields}
        if date_cols_present:
            args = (len(date_cols_present), len(expected_date_fields), date_cols_present)
            logging.info('Checking ({0}/{1}) available date fields: {2}'.format(*args))
            for col in date_cols_present:
                expected_date_format = file_prop_dict[filename]['date_format']
                try:
                    pd.to_datetime(df[col], format=expected_date_format)
                except ValueError:
                    args = (filename, col, expected_date_format)
                    logging.error('Incorrect date format for {0} in field {1}, expected {2}'.format(*args))
                    logging.error(df[col])
                    continue
            logging.info('Date format checking complete.')
        else:
            logging.error('None of the expected date fields were present in: {}'.format(filename))
    else:
        logging.info('There are no expected date fields to check.')


def validate_source_files(file_prop_dict, source_files):
    for path in source_files:
        filename = os.path.basename(path)
        logging.info('-' * 80)
        logging.info('Checking file: {}'.format(filename))

        # Check file is not empty
        if os.stat(path).st_size == 0:
            logging.error('File is empty: {}'.format(filename))
            continue

        # Check encoding
        encoding = get_encoding(path)
        if encoding not in ALLOWED_ENCODINGS:
            logging.error('Invalid file encoding ({0}) detected for: {1}. Must be {2}.'.format(encoding,
                          filename, '/'.join(ALLOWED_ENCODINGS)))
            continue

        # Try to read the file as df
        try:
            df = pd.read_csv(path, sep=None, engine='python', dtype='object')
        except ValueError:  # Catches far from everything
            logging.error('Could not read file: {}. Is it a valid data frame?'.format(filename))
            continue
        else:
            df.columns = [field.upper() for field in df.columns]
            file_header_fields = set(df.columns)
        logging.info('Detected fields: {}'.format(file_header_fields))

        # Check mandatory header fields are present
        required_header_fields = {field.upper() for field in file_prop_dict[filename]['headers']}
        if not required_header_fields.issubset(file_header_fields):
            missing = required_header_fields - file_header_fields
            logging.error('{0} is missing mandatory header fields: {1}'.format(filename, missing))
        else:
            logging.info('All mandatory header fields are present.')

        # Check date format
        check_date_fields(file_prop_dict, filename, file_header_fields, df)


@click.command()
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('properties_file', type=click.Path(exists=True))
@click.option('-l', '--log_type', type=click.Choice(['console', 'file', 'both']), default='console', show_default=True,
              help='Log validation results to screen ("console"), log file ("file"), or both ("both")')
def main(source_dir, properties_file, log_type):
    """1. SOURCE_DIR path to the folder containing the source files.
    All files in this folder will be checked for encoding and the required header fields.\n
    2. PROPERTIES_FILE path to json file that provides the required properties per file.
    A copy of this file can be found at:\n
    \tpmc-conversion/config/file_headers.json"""
    configure_logging(log_type)
    logging.info('Source folder: {}'.format(source_dir))
    logging.info('Properties file: {}'.format(properties_file))

    # check provided arguments
    check_arguments(source_dir, properties_file)
    # Collect expected file properties from json file
    file_prop_dict = read_json_dictionary(properties_file)
    # Collect all source files
    source_files = get_source_files(source_dir, file_prop_dict)
    # Validate encoding and header fields of each source file
    validate_source_files(file_prop_dict, source_files)

    logging.info('Validation complete.')


if __name__ == "__main__":
    main()
