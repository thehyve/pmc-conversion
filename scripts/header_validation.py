import json
import logging
import os
import sys

import chardet
import click
import pandas as pd

ALLOWED_ENCODINGS = ['utf-8', 'ascii']


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


def get_required_header_fields(header_file):
    data = json.load(open(header_file))
    required_header_fields = set(data['mandatory_columns'])
    required_header_fields = {field.upper() for field in required_header_fields}
    logging.info('Expected fields: {}'.format(required_header_fields))
    return required_header_fields


def get_source_files(path):
    logging.info('Collecting source files')
    source_files = [os.path.join(path, file) for file in os.listdir(path)]
    source_files = [path for path in source_files if os.path.isfile(path) and
                    not os.path.basename(path).startswith('.') and '~$' not in path]
    logging.info('Source files: {}'.format([os.path.basename(file) for file in source_files]))
    return source_files


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


def validate_source_files(required_header_fields, source_files):
    for file in source_files:
        logging.info('Checking file: {}'.format(os.path.basename(file)))

        if os.stat(file).st_size == 0:
            logging.error('File is empty: {}'.format(file))
            continue

        encoding = get_encoding(file)
        if encoding not in ALLOWED_ENCODINGS:
            logging.error('Invalid file encoding ({0}) detected for: {1}. Must be {2}.'.format(encoding,
                          file, '/'.join(ALLOWED_ENCODINGS)))
            continue

        try:
            file_header_fields = set(pd.read_csv(file, nrows=1, sep=None, engine='python').columns)
        except ValueError:  # Catches far from everything
            logging.error('Could not read file: {}. Is it a valid data frame?'.format(file))
            continue
        else:
            file_header_fields = {field.upper() for field in file_header_fields}
        logging.info('Detected fields: {}'.format(file_header_fields))

        if not required_header_fields.issubset(file_header_fields):
            missing = required_header_fields - file_header_fields
            logging.error('{0} is missing mandatory header fields: {1}'.format(file, missing))
        else:
            logging.info('Validation successful.')


@click.command()
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('header_file', type=click.Path(exists=True))
@click.option('-l', '--log_type', type=click.Choice(['console', 'file', 'both']), default='console', show_default=True,
              help='Log validation results to screen ("console"), log file ("file"), or both ("both")')
def main(source_dir, header_file, log_type):
    """1. SOURCE_DIR path to the folder containing the source files.
    All files in this folder will be checked for encoding and the required header fields.\n
    2. HEADER_FILE path to json file that provides the required header fields.
    A copy of this file can be found at:\n
    \tpmc-conversion/config/mandatory_columns.json"""
    configure_logging(log_type)
    logging.info('Source folder: {}'.format(source_dir))
    logging.info('Header file: {}'.format(header_file))

    # check provided arguments
    check_arguments(source_dir, header_file)
    # Collect mandatory header fields from json file
    required_header_fields = get_required_header_fields(header_file)
    # Collect all source files
    source_files = get_source_files(source_dir)
    # Validate encoding and header fields of each source file
    validate_source_files(required_header_fields, source_files)

    logging.info('Validation complete')


if __name__ == "__main__":
    main()
