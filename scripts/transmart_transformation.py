import os
import sys
import tmtk
import click
import chardet
import pandas as pd
import configparser

class Config(object):

    def __init__(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        self.blueprint = self.config.get('DATA','blueprint_file')
        self.csr_data_file = self.config.get('DATA','csr_data_file')
        self.modifiers = self.config.get('DATA','modifier_file')

        self.output_file = self.config.get('OUTPUT', 'export_dir')

        self.study_id = self.config.get('STUDY', 'study_id')
        self.top_node = self.config.get('STUDY', 'top_node')
        self.security_required = self.config.getboolean('STUDY','security_required')


@click.command()
@click.argument('config_file', type=click.Path())
@click.option('--config_dir', type=click.Path(exists=True))
def main(config_file, config_dir):
    if config_dir:
        config = Config(os.path.join(config_dir, config_file))
    else:
        config = Config(config_file)

    transmart_transformation(config)

def transmart_transformation(config):

    df = pd.read_csv(config.csr_data_file, sep='\t', encoding=get_encoding(config.csr_data_file))
    df = add_modifiers(df)

    study = tmtk.Study()
    study.study_id = config.study_id
    study.top_node = config.top_node
    study.security_required = config.security_required

    study.Clinical.add_datafile(filename='csr_study.txt', dataframe=df)
    study.Clinical.Modifiers.df = pd.read_csv(config.modifiers, sep='\t')

    study.apply_blueprint(config.blueprint, omit_missing=True)

    tm_study = tmtk.toolbox.SkinnyExport(study, config.output_file)

    tm_study.build_observation_fact()

    print(tm_study.observation_fact.df.shape)
    tm_study.to_disk()

    sys.exit(0)

def get_encoding(file_name):
    """Open the file and determine the encoding, returns the encoding cast to lower"""
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()


def add_modifiers(df):

    df['CSR_DIAGNOSE_MOD'] = df['DIAGNOSIS_ID']
    df['CSR_STUDY_MOD'] = df['STUDY_ID']
    df['CSR_BIOSOURCE_MOD'] = df['BIOSOURCE_ID']
    df['CSR_BIOMATERIAL_MOD'] = df['BIOMATERIAL_ID']

    df.loc[pd.notnull(df['BIOMATERIAL_ID']), 'CSR_BIOMATERIAL_MOD'] = df.loc[
        pd.notnull(df['BIOMATERIAL_ID']), 'SRC_BIOSOURCE_ID']

    return df

if __name__ == '__main__':
    main()