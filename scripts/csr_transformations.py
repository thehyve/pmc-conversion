import os
import sys
import json
import click
import chardet
import collections
import logging
import pandas as pd
from click import UsageError

ALLOWED_ENCODINGS = {'utf-8', 'ascii'}
PK_COLUMNS = {'INDIVIDUAL_ID',
              'DIAGNOSIS_ID',
              'STUDY_ID',
              'BIOMATERIAL_ID',
              #'SRC_BIOSOURCE_ID',  # Remove this "Identifying column"?
              'BIOSOURCE_ID'}


class MissingHeaderException(Exception):
    pass


class IndividualIdentifierMissing(Exception):
    pass


@click.command()
@click.option('--input_dir', type=click.Path(exists=True))  # file_dir
@click.option('--output_dir', type=click.Path(exists=True))  # target_dir
@click.option('--config_dir', type=click.Path(exists=True))
@click.option('--data_model', default=None)  # file_header_config
@click.option('--column_priority', default=None)  # JSON file indicating file priority per column
@click.option('--file_headers', default=None)  # Correct file headers per file
@click.option('--columns_to_csr', default=None)  # header_mapping_config
@click.option('--output_filename', default='csr_data_transformation.tsv')  # target_file
@click.option('--log_type', type=click.Choice(['console', 'file', 'both']), default='console', show_default=True,
              help='Log validation results to screen ("console"), log file ("file"), or both ("both")')
def main(input_dir, output_dir, config_dir, data_model,
         column_priority, file_headers, columns_to_csr, output_filename, log_type):
    configure_logging(log_type)

    # Check which params need to be set
    mandatory = {'--config_dir': config_dir, '--data_model': data_model, '--column_priority': column_priority,
                 '--columns_to_csr': columns_to_csr, '--file_headers': file_headers, '--input_dir': input_dir,
                 '--output_dir': output_dir}
    if not all(mandatory.values()):
        for option_name, option_value in mandatory.items():
            if not option_value:
                logging.error('Input argument missing: {}'.format(option_name))
        raise UsageError('Missing input arguments')

    csr_data_model = read_dict_from_file(filename=data_model, path=config_dir)
    file_prop_dict = read_dict_from_file(filename=file_headers, path=config_dir)
    columns_to_csr_map = read_dict_from_file(filename=columns_to_csr, path=config_dir)
    columns_to_csr_map = {k: {k.upper(): v.upper() for k, v in vals.items()} for k, vals in columns_to_csr_map.items()}
    column_prio_dict = read_dict_from_file(filename=column_priority, path=config_dir)
    column_prio_dict = {k.upper(): files for k, files in column_prio_dict.items()}

    col_file_dict = get_overlapping_columns(file_prop_dict, columns_to_csr_map)
    check_column_prio(column_prio_dict, col_file_dict)

    # write priority as constructed from file_headers.json
    # with open('../config/column_priority.json', 'w') as fp:
    #    json.dump(col_file_dict, fp)

    expected_files = file_prop_dict.keys()

    output_file = os.path.join(output_dir, output_filename)

    files_per_entity = read_data_files(input_dir=input_dir,
                                       output_dir=output_dir,
                                       columns_to_csr=columns_to_csr_map,
                                       file_list=expected_files,
                                       file_headers=file_prop_dict)

    subject_registry = build_csr_dataframe(file_dict=files_per_entity,
                                           file_list=expected_files,
                                           csr_data_model=csr_data_model)

    subject_registry = resolve_data_conflicts(df=subject_registry,
                                              column_priority=column_prio_dict,
                                              csr_data_model=csr_data_model)

    subject_registry.reset_index(inplace=True)
    subject_registry.to_csv('/tmp/TMP_SUBJ_REGI.txt', sep='\t', index=False)

    # - Check if all CSR headers are in the merged dataframe
    csr_expected_header = []
    for key in csr_data_model:
        csr_expected_header += list(csr_data_model[key])

    missing_header = [l for l in csr_expected_header if l not in subject_registry.columns]
    if len(missing_header) > 0:
        logging.error('Missing columns from Subject Registry data model:\n {}'.format(missing_header))
        sys.exit(1)

    if pd.isnull(subject_registry['INDIVIDUAL_ID']).any():
        logging.error('Some individuals do not have an identifier')
        sys.exit(1)

    logging.info('Writing CSR data to {}'.format(output_file))
    subject_registry.to_csv(output_file, sep='\t', index=False)

    sys.exit(0)


def resolve_data_conflicts(df, column_priority, csr_data_model):
    df.set_index(['INDIVIDUAL_ID', 'DIAGNOSIS_ID', 'STUDY_ID', 'BIOMATERIAL_ID', 'BIOSOURCE_ID'],
                 inplace=True)
    df.to_csv('/tmp/CONF_SR.txt', '\t')
    missing_column = False
    df = df.reorder_levels(order=[1, 0], axis=1).sort_index(axis=1, level=0)
    subject_registry = pd.DataFrame()
    for column in df.columns.get_level_values(0).unique():
        if df[column].shape[1] > 1:
            ref_df = df.pop(column)
            if column not in column_priority:
                logging.error(
                    'Column: {} missing from column priority \
mapping for the following files {}'.format(column, ref_df.columns.tolist()))
                missing_column = True
                continue
            ref_files = column_priority[column]
            base = pd.DataFrame(data={column: ref_df[ref_files[0]]})
            for file in ref_files[1:]:
                base = base.combine_first(pd.DataFrame(data={column: ref_df[file]}))
            if subject_registry.empty:
                subject_registry = base
            else:
                subject_registry = subject_registry.merge(base, left_index=True, right_index=True, how='outer')

    df.columns = df.columns.droplevel(1)
    subject_registry = subject_registry.merge(df, left_index=True, right_index=True, how='outer')

    if missing_column:
        logging.error('Can not resolve data conflicts due to missing columns from column priority mapping, exiting')
        sys.exit(1)
    return subject_registry  # Conflict free df


def get_overlapping_columns(file_prop_dict, columns_to_csr_map):
    col_file_dict = dict()
    for filename in file_prop_dict:
        colname_dict = columns_to_csr_map[filename] if filename in columns_to_csr_map else None
        for colname in file_prop_dict[filename]['headers']:
            colname = colname.upper()
            if colname in PK_COLUMNS:
                continue
            if colname_dict and colname in colname_dict:
                colname = colname_dict[colname]
            if colname not in col_file_dict:
                col_file_dict[colname] = [filename]
            else:
                col_file_dict[colname].append(filename)

    # Remove all columns that occur in only one file
    col_file_dict = {colname: filenames for colname, filenames in col_file_dict.items() if len(filenames) > 1}
    return col_file_dict


def check_column_prio(column_prio_dict, col_file_dict):
    """Compare the priority definition from column_priority.json with what was found in file_headers.json and log
    all mismatches."""
    missing_in_priority = set(col_file_dict.keys()) - set(column_prio_dict.keys())
    missing_in_file_headers = set(column_prio_dict.keys()) - set(col_file_dict.keys())

    # Column name missing entirely
    if missing_in_priority:
        for col in missing_in_priority:
            logging.error(('"{0}" column occurs in multiple data files: {1}, but no priority is '
                           'defined.'.format(col, col_file_dict[col])))

    if missing_in_file_headers:
        for col in missing_in_file_headers:
            logging.warning('Priority is defined for "{0}", but it not present in file_headers.json'.format(col))

    # Priority present, but incomplete or unknown priority provided
    shared_columns = set(column_prio_dict.keys()).intersection(set(col_file_dict.keys()))
    for col in shared_columns:
        files_missing_in_prio = [filename for filename in col_file_dict[col] if filename not in column_prio_dict[col]]
        files_only_in_prio = [filename for filename in column_prio_dict[col] if filename not in col_file_dict[col]]
        if files_missing_in_prio:
            logging.error(('Incomplete priority provided for column "{0}". It occurs in files: {1}, but priority '
                           'found only for files: {2}').format(col, col_file_dict[col], column_prio_dict[col]))

        if files_only_in_prio:
            logging.warning(('Provided priority for column "{0}" contains more files than present in '
                             'file_headers.json. Priority files not used: {1}').format(col, files_only_in_prio))


def configure_logging(log_type, level=logging.INFO):
    log_format = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%d-%m-%y %H:%M:%S')
    logging.getLogger().setLevel(level)

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


def read_dict_from_file(filename, path=None):
    file = os.path.join(path, filename)
    if os.path.exists(file):
        with open(file, 'r') as f:
            dict_ = json.loads(f.read())
        return dict_
    else:
        logging.error('Config file: {} - not found. Aborting'.format(file))
        sys.exit(1)


def get_encoding(file_name):
    """Open the file and determine the encoding, returns the encoding cast to lower"""
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()


def input_file_to_df(file_name, encoding, seperator=None, column_mapping=None, codebook=None):
    """Read in a DataFrame from a plain text file. Columns are cast to uppercase.
     If a column_mapping is specified the columns will also be tried to map.
     If a codebook was specified it will be applied before the column mapping"""
    df = pd.read_csv(file_name, sep=seperator, encoding=encoding, dtype=object, engine="python")
    df.columns = map(lambda x: str(x).upper(), df.columns)
    # TODO: write check to see if the df values are all captured in the codebook (check file headers)
    if codebook:
        df.replace(codebook, inplace=True)
    # if column_mapping:
    #     df.columns = apply_header_map(df.columns, column_mapping)
    return df


def apply_header_map(df_columns, header):
    """Generate a new header for a pandas Dataframe using either the column name or the mapped column name if provided
    in the header object. Returns uppercased column names from the header"""
    header_upper = {k.upper(): v.upper() for k, v in header.items()}
    new_header = [header_upper[col] if col in header_upper else col for col in df_columns]
    return new_header


def determine_file_type(columns, filename):
    """Based on pandas DataFrame columns determine the type of entity to assign to the dataframe."""
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
        logging.error(('No key identifier found (individual, diagnosis, study, biosource, '
                       'biomaterial) in {}'.format(filename)))


def check_file_list(files_found):
    for filename, found in files_found.items():
        if not found:
            logging.error('Data file: {} expected but not found in source folder.'.format(filename))


def merge_entity_data_frames(df_dict, id_columns):
    df_list = []
    key_list = []
    for key in df_dict.keys():
        df_list.append(df_dict[key].set_index(id_columns))
        key_list.append(key)

    df = pd.concat(df_list, keys=key_list, join='outer', axis=1)
    df.reset_index(inplace=True)
    return df


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
    bios = biosource[['INDIVIDUAL_ID', 'BIOSOURCE_ID', 'DIAGNOSIS_ID']].copy()
    biom = biomaterial.copy()

    bios.columns = bios.columns.map('|'.join)
    biom.columns = ['|'.join([col,'']) for col in biom.columns]

    biomaterial_merge_on = '|'.join([biomaterial_merge_on, ''])
    biosource_merge_on = '|'.join([biosource_merge_on, ''])

    df = biom.merge(bios,
                    left_on=biomaterial_merge_on,
                    right_on=biosource_merge_on,
                    how='left')

    df.columns = [i.split('|')[0] for i in df.columns]
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

    entity_to_columns = collections.OrderedDict()
    entity_to_columns['individual'] = ['INDIVIDUAL_ID']
    entity_to_columns['diagnosis'] = ['DIAGNOSIS_ID', 'INDIVIDUAL_ID']
    entity_to_columns['study'] = ['STUDY_ID', 'INDIVIDUAL_ID']
    entity_to_columns['biosource'] = ['BIOSOURCE_ID', 'INDIVIDUAL_ID', 'DIAGNOSIS_ID']
    #entity_to_columns['biomaterial'] = ['BIOMATERIAL_ID', 'SRC_BIOSOURCE_ID']
    entity_to_columns['biomaterial'] = ['BIOMATERIAL_ID','BIOSOURCE_ID', 'INDIVIDUAL_ID', 'DIAGNOSIS_ID']

    missing_entities = False

    entity_to_data_frames = collections.OrderedDict()
    for entity, columns in entity_to_columns.items():
        if not file_dict[entity]:
            logging.error('Missing data for entity: {} does not have a corresponding file.'.format(entity))
            missing_entities = True
            continue

        if entity == 'biomaterial' and 'biosource' in entity_to_data_frames.keys():
            for filename in file_dict[entity]:
                file_dict[entity][filename] = add_biosource_identifiers(entity_to_data_frames['biosource'],
                                                                        file_dict[entity][filename])

        df = merge_entity_data_frames(df_dict=file_dict[entity],
                                      id_columns=columns)
        entity_to_data_frames[entity] = df

    if missing_entities:
        logging.error('Missing data for one or more entities, cannot continue.')
        sys.exit(1)

    # TODO: ADD DEDUPLICATION ACROSS ENTITIES:
    # TODO: Add advanced deduplication for double values from for example individual and diagnosis.
    # TODO: idea to capture this in a config file where per column for the CSR the main source is described.
    # TODO: This could reuse the file_header mapping to create subselections and see if there are duplicates.
    # TODO: The only thing that is not covered is some edge cases where a data point from X should lead instead of the
    # TODO: normal Y data point.

    # Concat all data starting with individual into the CSR dataframe, study, diagnosis, biosource and biomaterial
    subject_registry = pd.concat(entity_to_data_frames.values())
    subject_registry['INDIVIDUAL_ID'] = subject_registry['INDIVIDUAL_ID'].combine_first(subject_registry['index'])
    subject_registry.drop('index', axis=1, inplace=True)

    return subject_registry


def validate_source_file(file_prop_dict, path):
    filename = os.path.basename(path)

    # Check file is not empty
    if os.stat(path).st_size == 0:
        logging.error('File is empty: {}'.format(filename))
        return

    # Check encoding
    encoding = get_encoding(path)
    if encoding not in ALLOWED_ENCODINGS:
        logging.error('Invalid file encoding ({0}) detected for: {1}. Must be {2}.'.format(encoding,
                                                                                           filename,
                                                                                           '/'.join(ALLOWED_ENCODINGS)))
        return

    # Try to read the file as df
    try:
        df = pd.read_csv(path, sep=None, engine='python', dtype='object')
    except ValueError:  # Catches far from everything
        logging.error('Could not read file: {}. Is it a valid data frame?'.format(filename))
        return
    else:
        df.columns = [field.upper() for field in df.columns]
        file_header_fields = set(df.columns)

    # Check mandatory header fields are present
    required_header_fields = {field.upper() for field in file_prop_dict[filename]['headers']}
    if not required_header_fields.issubset(file_header_fields):
        missing = required_header_fields - file_header_fields
        logging.warning('{0} is missing mandatory header fields: {1}'.format(filename, missing))


def read_data_files(input_dir, output_dir, columns_to_csr, file_list, file_headers):
    # Input is taken in per entity.
    files_per_entity = {'individual': {}, 'diagnosis': {}, 'biosource': {}, 'biomaterial': {}, 'study': {}}

    files_found = {filename: False for filename in file_list}
    # Assumption is that all the files are in EPD, LIMMS or STUDY folders.
    for path, dir_, filenames in os.walk(input_dir):
        for filename in filenames:
            working_dir = os.path.basename(path).upper()
            if working_dir in ['EPD', 'LIMMS', 'STUDY'] and 'codebook' not in filename and not filename.startswith('.'):
                file = os.path.join(path, filename)

                if filename in files_found:
                    files_found[filename] = True

                validate_source_file(file_headers, file)

                codebook = check_for_codebook(filename, output_dir)

                # Read data from file and create a
                # pandas DataFrame. If a header mapping is provided the columns
                # are mapped before the DataFrame is returned
                df = input_file_to_df(file, get_encoding(file), codebook=codebook)

                ## Update date format
                set_date_fields(df, file_headers, filename)

                # Check if mapping for columns to CSR fields is present
                header_map = columns_to_csr[filename] if filename in columns_to_csr else None
                if header_map:
                    df.columns = apply_header_map(df.columns, header_map)

                columns = df.columns
                # Check if headers are present
                file_type = determine_file_type(columns, filename)
                if not file_type:
                    continue
                files_per_entity[file_type].update({filename: df})

    check_file_list(files_found)

    return files_per_entity


def set_date_fields(df, file_prop_dict, filename):
    date_fields = file_prop_dict[filename].get('date_columns')
    date_fields = [field.upper() for field in date_fields] if date_fields else None

    if date_fields:
        expected_date_format = file_prop_dict[filename].get('date_format')
        logging.info('Setting date fields for {}'.format(filename))
        for col in date_fields:
            try:
                # df[col] = pd.to_datetime(df[col], format=expected_date_format).dt.strftime('%Y-%m-%d').astype(object)
                df[col] = df[col].apply(get_date_as_string, args=(expected_date_format,))
            except ValueError:
                args = (filename, col, expected_date_format)
                logging.error('Incorrect date format for {0} in field {1}, expected {2}'.format(*args))
                continue
    else:
        logging.info('There are no expected date fields to check for {}'.format(filename))


def get_date_as_string(item, dateformat, str_format='%Y-%m-%d'):
    if pd.isnull(item):
        return pd.np.nan
    else:
        return pd.datetime.strptime(item, dateformat).strftime(str_format)


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
