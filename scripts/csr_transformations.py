import os
import sys
import json
import click
import chardet
import collections
import pandas as pd
from click import UsageError


class MissingHeaderException(Exception):
    pass


class IndividualIdentifierMissing(Exception):
    pass


@click.command()
@click.option('--input_dir', type=click.Path(exists=True))  # file_dir
@click.option('--output_dir', type=click.Path(exists=True))  # target_dir
@click.option('--config_dir', type=click.Path(exists=True))
@click.option('--data_model', default=None)  # file_header_config
@click.option('--file_list', default=None)  # file_list_config, file_order_config
@click.option('--file_headers', default=None)  # Correct file headers per file
@click.option('--columns_to_csr', default=None)  # header_mapping_config
@click.option('--output_filename', default='csv_data_transformation.tsv')  # target_file
def main(input_dir, output_dir, config_dir, data_model,
         file_list, file_headers, columns_to_csr, output_filename):
    ## Check which params need to be set
    if not (config_dir or data_model or file_list or columns_to_csr):
        # TODO: provide which variable was not provided
        raise UsageError('Missing input arguments')

    with open(os.path.join(config_dir, file_list), 'r') as f:
        expected_files = [line.strip() for line in f]

    csr_data_model = read_dict_from_file(filename=data_model, path=config_dir)
    expected_file_headers = read_dict_from_file(filename=file_headers, path=config_dir)
    columns_to_csr_map = read_dict_from_file(filename=columns_to_csr, path=config_dir)

    output_file = os.path.join(output_dir, output_filename)

    files_per_entity = read_data_files(input_dir=input_dir,
                                       output_dir=output_dir,
                                       columns_to_csr=columns_to_csr_map,
                                       file_list=expected_files)

    subject_registry = build_csr_dataframe(file_dict=files_per_entity,
                                           file_list=expected_files,
                                           csr_data_model=csr_data_model)

    # - Check if all CSR headers are in the merged dataframe

    csr_expected_header = []
    for key in csr_data_model:
        csr_expected_header += list(csr_data_model[key])

    missing_header = [l for l in csr_expected_header if l not in subject_registry.columns]
    if len(missing_header) > 0:
        raise MissingHeaderException(
            '[ERROR] Missing columns from Subject Registry data model:\n {}'.format(missing_header))

    subject_registry.to_csv(output_file, sep='\t', index=False)
    if pd.isnull(subject_registry['INDIVIDUAL_ID']).any():
        raise IndividualIdentifierMissing('Some individuals do not have an identifier')

    sys.exit(0)


def read_dict_from_file(filename, path=None):
    if filename:
        file = os.path.join(path, filename)
        if os.path.exists(file):
            with open(file, 'r') as f:
                dict_ = json.loads(f.read())
            return dict_
        else:
            return None
    else:
        return None


def get_encoding(file_name):
    """Open the file and determine the encoding, returns the encoding cast to lower"""
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()


def input_file_to_df(file_name, encoding, seperator='\t', column_mapping=None, codebook=None):
    """Read in a DataFrame from a plain text file. Columns are cast to uppercase.
     If a column_mapping is specified the columns will also be tried to map.
     If a codebook was specified it will be applied before the column mapping"""
    df = pd.read_csv(file_name, sep=seperator, encoding=encoding, dtype=object)
    df.columns = map(lambda x: str(x).upper(), df.columns)
    # TODO: write check to see if the df values are all captured in the codebook
    if codebook:
        df.replace(codebook, inplace=True)
    if column_mapping:
        df.columns = apply_header_map(df.columns, column_mapping)
    return df


def apply_header_map(df_columns, header):
    """Generate a new header for a pandas Dataframe using either the column name or the mapped column name if provided
    in the header object. Returns uppercased column names from the header"""
    header_upper = {k.upper(): v.upper() for k, v in header.items()}
    new_header = []
    for i in df_columns:
        if i in header_upper:
            new_header.append(header_upper[i])
        else:
            new_header.append(i)
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
        print('I am not working! - No Key Identifier found: Individual, diagnosis, study, biosource or biomaterial')
        # sys.exit(9)


def check_file_header(file_header, expected_header, name):
    msgs = []
    for column in expected_header:
        if column not in file_header:
            msgs.append('[ERROR] {0} header: expected {1} as a column but not found'.format(name, column))
    return msgs


def check_file_list(files, expected_files):
    msgs = []
    for file in expected_files:
        if file not in files:
            msgs.append('[ERROR] data file: {} expected but not found'.format(file))

    return msgs


def merge_data_frames(df_dict, file_order, id_columns):
    """Takes a dictionary with filenames as keys and dataframes as values and merges these into one pandas dataframe
    based on the id_columns. This should be done per entity type.

    :param df_dict: A dictionary of filenames and dataframes
    :param file_order: List of files to indicate which data to use in case of duplicate observations.
    :param id_columns: ID columns to merge data on (INDIVIDUAL, STUDY, DIAGNOSIS, BIOSOURCE, BIOMATERIAL).
    :return: Merged pandas Dataframe
    """
    # TODO: add warning to indicate there are files in the dict that are not in the order list
    # TODO: implement test to see if dataframe not empty
    df_list = [f for f in file_order if f in df_dict.keys()]
    ref_df = df_dict[df_list[0]]
    for i in df_list[1:]:
        ref_df = ref_df.merge(df_dict[i], how='outer', on=id_columns, suffixes=('_x', '_y'))
        combine_column_data(ref_df)
    return ref_df


def combine_column_data(df):
    """Take a pandas dataframe and combine columns based on the suffixes after using the pandas merge() function.
    Assumes the suffixes are X and Y. Using combine_first combines the suffix columns into a column without the suffix.

    :param df: pandas Dataframe, optionally with suffixes (_x and _y only)
    :return: pandas Dataframe without _x and _y suffixes
    """
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


def build_csr_dataframe(file_dict, file_list, csr_data_model):
    """Takes the complete data dictionary with all of the read input files. Merges the data per entity type and adds
     INDIVIDUAL_ID and DIAGNOSIS_ID to the biomaterial entity. Data duplication is taken care of within entities

     Returns the complete central subject registry as a
     pandas Dataframe using pandas concat()

    :param file_dict: dictionary with all the files to be merged
    :param file_list: an ordered list of expected files
    :param csr_data_model: dictionary with the csr_data_model description
    :return: Central Subject Registry as pandas Dataframe
    """
    check_msgs = []

    entity_to_columns = collections.OrderedDict()
    entity_to_columns['individual'] = ['INDIVIDUAL_ID']
    entity_to_columns['diagnosis'] = ['DIAGNOSIS_ID', 'INDIVIDUAL_ID']
    entity_to_columns['study'] = ['STUDY_ID', 'INDIVIDUAL_ID']
    entity_to_columns['biomaterial'] = ['BIOMATERIAL_ID', 'SRC_BIOSOURCE_ID']
    entity_to_columns['biosource'] = ['BIOSOURCE_ID', 'INDIVIDUAL_ID', 'DIAGNOSIS_ID']

    entity_to_data_frames = collections.OrderedDict()
    for entity, columns in entity_to_columns.items():
        if entity not in file_dict:
            raise ValueError('Missing entity: {} does not have a corresponding file.'.format(entity))
        df = merge_data_frames(df_dict=file_dict[entity],
                               file_order=file_list,
                               id_columns=columns)
        entity_to_data_frames[entity] = df
        check_msgs += check_file_header(df.columns, csr_data_model[entity], entity)

    entity_to_data_frames['biomaterial'] = add_biosource_identifiers(entity_to_data_frames['biosource'],
                                                                     entity_to_data_frames['biomaterial'])
    check_msgs += check_file_header(entity_to_data_frames['biomaterial'].columns, csr_data_model['biomaterial'],
                                    'biomaterial')

    if check_msgs:
        raise MissingHeaderException('Missing headers for entities', print_errors(check_msgs))

    # TODO: ADD DEDUPLICATION ACROSS ENTITIES:
    # TODO: Add advanced deduplication for double values from for example individual and diagnosis.
    # TODO: idea to capture this in a config file where per column for the CSR the main source is described.
    # TODO: This could reuse the file_header mapping to create subselections and see if there are duplicates.
    # TODO: The only thing that is not covered is some edge cases where a data point from X should lead instead of the
    # TODO: normal Y data point.

    # Concat all data starting with individual into the CSR dataframe, study, diagnosis, biosource and biomaterial
    subject_registry = pd.concat(entity_to_data_frames.values())

    return subject_registry


def read_data_files(input_dir, output_dir, columns_to_csr, file_list):
    error_messages = []  # TODO: implement python logger?

    # Input is taken in per entity.
    files_per_entity = {'individual': {}, 'diagnosis': {}, 'biosource': {}, 'biomaterial': {}, 'study': {}}
    all_files = []
    # Assumption is that all the files are in EPD, LIMMS or STUDY folders.
    for path, dir_, filenames in os.walk(input_dir):
        for filename in filenames:
            working_dir = path.rsplit('/', 1)[1].upper()
            if dir_ == [] and working_dir in ['EPD', 'LIMMS',
                                              'STUDY'] and 'codebook' not in filename and not filename.startswith('.'):
                all_files += filename
                file = os.path.join(path, filename)

                codebook = check_for_codebook(filename, output_dir)

                # Check if mapping for columns to CSR fields is present
                if filename in columns_to_csr:
                    header_map = columns_to_csr[filename]
                else:
                    header_map = None

                # Read data from file and create a
                #  pandas DataFrame. If a header mapping is provided the columns
                # are mapped before the DataFrame is returned
                df = input_file_to_df(file, get_encoding(file), column_mapping=header_map, codebook=codebook)

                columns = df.columns
                # Check if headers are present
                file_type = determine_file_type(columns)
                files_per_entity[file_type].update({filename: df})

    error_messages += check_file_list(all_files,
                                      file_list)  # TODO: is this step needed? Covered by checking CSR output?
    # Temporarily disabled
    # if error_messages:
    #    raise InputFilesIncomplete('{} Input files missing'.format(print_errors(error_messages)))

    return files_per_entity


class InputFilesIncomplete(Exception):
    pass


def print_errors(messages):
    # just a list of files for now, Don't start with blocks
    print('--------- ERRORS ---------')
    for msg in messages:
        print(msg)
    return len(messages)


def check_for_codebook(filename, path):
    f_name, f_extension = filename.rsplit('.', 1)
    code_file = '{}_codebook.{}.json'.format(f_name, f_extension)
    if code_file in os.listdir(path):
        with open(os.path.join(path, code_file), 'r') as cf:
            codebook = json.loads(cf.read())
        return codebook
    else:
        return None


if __name__ == '__main__':
    main()
