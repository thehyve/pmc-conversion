import logging
import os
import sys
from collections import Counter

import click
import pandas as pd

ALLOWED_FILE_TYPES = ['.xls', '.xlsx']


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
        log_file = logging.FileHandler('xlsx_transform.log')
        log_file.setFormatter(log_format)
        logging.getLogger('').addHandler(log_file)


def get_source_files(path):
    logging.info('Collecting source files')
    source_files = [os.path.join(path, file) for file in os.listdir(path)
                    if os.path.splitext(file)[1] in ALLOWED_FILE_TYPES]
    source_files = [path for path in source_files if os.path.isfile(path) and
                    not os.path.basename(path).startswith('.') and '~$' not in path]
    logging.info('Source files: {}'.format([os.path.basename(file) for file in source_files]))
    return source_files


def check_arguments(source_dir, target_dir):
    if not os.path.isdir(source_dir):
        logging.error("Provided SOURCE_DIR is not a directory: {}".format(source_dir))
        sys.exit(1)
    if not os.path.exists(target_dir):
        logging.info('Target directory does not exist, trying to create it.')
        os.makedirs(target_dir)
        logging.info('Created new directory at: {}'.format(target_dir))


def source_files_to_csv(source_files, target_dir, file_name_counter=Counter()):
    for file in source_files:
        logging.info('Opening file: {}'.format(os.path.basename(file)))

        kwargs = {'sheetname': None} if pd.__version__ < '0.21.0' else {'sheet_name': None}
        df_dict = pd.read_excel(file, header=None, dtype='object', **kwargs)
        logging.info('Sheets present: {}'.format(list(df_dict.keys())))

        for sheet_name, df in df_dict.items():
            file_name_counter[sheet_name] += 1
            output_file_name = sheet_name
            if file_name_counter[sheet_name] > 1:
                logging.warning('Occurrence {0} of sheet name "{1}"'.format(file_name_counter[sheet_name], sheet_name))
                output_file_name += '_' + str(file_name_counter[sheet_name])
            target_path = os.path.join(target_dir, output_file_name+'.tsv')
            logging.info('Writing file at: {}'.format(target_path))
            df.to_csv(target_path, sep='\t', index=False, header=False)


@click.command()
@click.argument('source_dir', type=click.Path(exists=True))
@click.argument('target_dir', type=click.Path())
@click.option('-l', '--log_type', type=click.Choice(['console', 'file', 'both']), default='console', show_default=True,
              help='Log validation results to screen ("console"), log file ("file"), or both ("both")')
def main(source_dir, target_dir, log_type):
    """1. SOURCE_DIR folder containing the Excel files.
    All sheets from all xls/xlsx files in this folder will be written to the TARGET_DIR.\n
    2. TARGET_DIR folder to which the data should be written."""
    configure_logging(log_type)
    logging.info('Source folder: {}'.format(source_dir))
    logging.info('Target folder: {}'.format(target_dir))

    # check provided arguments
    check_arguments(source_dir, target_dir)
    # Collect all source files
    source_files = get_source_files(source_dir)
    # Validate encoding and header fields of each source file
    source_files_to_csv(source_files, target_dir)

    logging.info('Excel to tsv conversion complete')


if __name__ == "__main__":
    main()
