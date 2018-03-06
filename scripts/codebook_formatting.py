import os
import json
import click
import chardet
import logging
import tempfile

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@click.command()
@click.argument('codebook_file', type=click.Path(exists=True))
@click.argument('codebook_mapping', type=click.Path(exists=True))
def main(codebook_file, codebook_mapping):
    
    file = os.path.abspath(codebook_file)
    codebook_formatting(file, codebook_mapping)
    

def codebook_formatting(file, codebook_mapping, output_dir=tempfile.gettempdir()):
    logger.debug('Codebook formatting for: {}'.format(file))
    basename = os.path.basename(file)

    cm_file = os.path.abspath(codebook_mapping)
    logger.debug('Retrieving codebook mapping from: {}'.format(cm_file))
    with open(cm_file, 'r', encoding=get_encoding(cm_file)) as cm:
        codebook_type = json.loads(cm.read())


    codebook_out = os.path.join(output_dir,basename+'.json')

    with open(file, 'r', encoding=get_encoding(file)) as file:
        logger.debug('Processing: {}'.format(basename))
        lines = file.readlines()


    codebook= None
    # TODO: implement format checker
    if codebook_type[basename] == 'format1':
        logger.debug('Format found: format1. Start processing')
        codebook = process_br_codebook(lines)
    else:
        logger.info('Format not found for {}, skipping'.format(file))

    if codebook:
        logger.info('Writing formatted codebook {} to {}'.format(basename, codebook_out))
        with open(codebook_out, 'w') as f:
            f.write(json.dumps(codebook))


def get_encoding(file_name):
    """Open the file and determine the encoding, returns the encoding cast to lower"""
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()


def process_br_codebook(lines):
    """Process the content of a codebook and return the reformatted codebook as a dict. Expected import format is a
    string with all lines from a file. Fields are tab separated

    Supported format:
    - First a header line with a number and column names the codes apply to. The first field has a number, the second
    field a space separated list of column names. i.e. 1\tSEX GENDER
    - The lines following the header start with an empty field. Then the lines follow the format of code\tvalue until
    the end of the line. i.e. ''\t1\tMale\t2\tFemale
    - The start of a new header, which is detected by the first field not being empty starts the process over again.

    :param lines: string object with all lines from the codebook file
    :return: dict object with codebook values {'COLUMN_NAME': {'CODE': 'VALUE'}}
    """
    codebook = {}
    for line in lines:
        line = line.strip('\n')
        if line.startswith('\t'):
            split_line = line.split('\t')[1:]
            it = iter(split_line)
            for code, value in zip(it, it):
                if code != '' and value != '':
                    clean_value = value.replace('"', '')
                    for column in column_code:
                        codebook[column].update({code: clean_value})
        else:
            split_line = line.split('\t')
            column_code = split_line[1].split(' ')
            for column in column_code:
                codebook[column] = {}
    return codebook


if __name__ == '__main__':
    main()
