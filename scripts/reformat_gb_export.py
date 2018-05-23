import pandas as pd
import zipfile
import os
import sys
import tempfile
import datetime as dt
import logging
import click

LOG_FORMAT = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s', '%d-%m-%y %H:%M:%S')
SEP = '\t'
DATE_FORMAT = '%d-%m-%Y'


@click.command()
@click.argument('input_file', type=click.Path(exists=True, dir_okay=False))
@click.argument('output_filename')
@click.option('--output_dir', default=None, help='Set directory to save the output file to')
@click.option('--log_level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']), default='INFO',
              show_default=True,
              help='Set level on which the logger should produce output')
def main(input_file, output_filename, output_dir, log_level):
    """This script reformats the TSV output from Glowing bear to a flat tab separated 2-dimensional table.
    By default the script expects the exported zip file from Glowing Bear and requires an output name to be defined.
    The default location of the output file is the directory in which the input zip file is located.
    """

    logging.getLogger().setLevel(log_level)
    console = logging.StreamHandler()
    console.setFormatter(LOG_FORMAT)
    logging.getLogger('').addHandler(console)

    if output_dir:
        output_file = os.path.join(output_dir, output_filename)
    else:
        output_file = os.path.join(os.path.dirname(input_file), output_filename)

    logging.info('Output file: {}'.format(output_file))

    working_dir = tempfile.TemporaryDirectory()
    os.chdir(working_dir.name)

    export_data = read_zip_file(input_file)

    observations = export_data.pop('observations')
    logging.info('Building mapping dictionary from export files')
    mapping_dict = get_mapping_dict(export_data)

    # Replace dimension values with values from the mapping dict
    logging.info('Applying mapping dictionary to the observations')
    observations.replace(mapping_dict, inplace=True)

    logging.info('Transformating observation data')
    observations.to_csv(os.path.join(os.path.dirname(input_file),'obs_{}'.format(output_filename)))
    obs_pivot = pivot_observation_data(observations)

    logging.info('Updating date fields with format: {}'.format(DATE_FORMAT))
    for column in obs_pivot.columns:
        if column.upper().startswith('DATE'):
            obs_pivot[column] = obs_pivot[column].apply(get_date_string)

    logging.info('Writting data to file: {}'.format(output_file))
    obs_pivot.to_csv(output_file, sep=SEP, index=False)

    logging.info('Done formatting export data!')
    sys.exit(0)


def read_zip_file(input_file):
    export_data = {}
    with zipfile.ZipFile(input_file) as z:
        for filename in z.namelist():
            item = filename.split('_', 1)[1].split('.')[0].replace('_', ' ')
            export_data.update({
                item: pd.read_csv(z.extract(filename), sep='\t', dtype=object)
            })
    return export_data


def get_mapping_dict(export_data):
    mods = ['PMC Diagnosis ID', 'PMC Biosource ID', 'PMC Study ID', 'PMC Biomaterial ID']
    replace_dict = {}
    for mod in mods:
        replace_dict.update(export_data.pop(mod).set_index('dimensionId').to_dict())

    concepts = export_data.pop('concept')
    replace_dict.update({'concept': concepts.set_index('dimensionId')['name'].to_dict()})

    study = export_data.pop('study')
    replace_dict.update({'study': study.set_index('dimensionId')['name'].to_dict()})

    patient = export_data.pop('patient')
    replace_dict.update({'patient': patient.set_index('dimensionId')['trial'].to_dict()})

    return replace_dict


def pivot_observation_data(observations):
    # Fill NaNs with empty string and drop study column as it is not important in the current export
    observations.fillna('', inplace=True)
    observations.drop('study', axis=1, inplace=True)

    # Define a list to use as index where the last item has to be the new column names
    index_list = ['patient', 'PMC Study ID', 'PMC Diagnosis ID', 'PMC Biosource ID', 'PMC Biomaterial ID', 'concept']
    observations.set_index(index_list, inplace=True)

    # Use unstack to move the last level of the index to column names
    obs_pivot = observations.unstack(level=-1, fill_value='')

    ## Update column names by dropping value level
    obs_pivot.columns = obs_pivot.columns.droplevel(level=0)
    obs_pivot.reset_index(inplace=True)

    return obs_pivot


def get_date_string(timestamp, string_format=DATE_FORMAT):
    if pd.notnull(timestamp) and timestamp != None and timestamp != '':
        return dt.datetime.utcfromtimestamp(float(timestamp) / 1000).strftime(string_format)
    else:
        return timestamp


if __name__ == '__main__':
    main()
