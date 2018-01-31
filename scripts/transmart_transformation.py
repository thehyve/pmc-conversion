import os
import sys
import tmtk
import click
import chardet
import pandas as pd


@click.command()
@click.option('--csr_data_file', type=click.Path(exists=True))
@click.option('--output_dir', type=click.Path(exists=True))
@click.option('--config_dir', type=click.Path(exists=True))
@click.option('--blueprint')
@click.option('--modifiers')
@click.option('--study_id')
@click.option('--top_node')
@click.option('--security_required')
def main(csr_data_file, output_dir, config_dir, blueprint, modifiers, study_id, top_node, security_required):

    df = pd.read_csv(csr_data_file, sep='\t', encoding=get_encoding(csr_data_file), dtype=object)
    df = add_modifiers(df)

    study = tmtk.Study()
    study.study_id = study_id
    study.top_node = top_node
    if security_required == 'N':
        study.security_required = False
    else:
        study.security_required = True

    study.Clinical.add_datafile(filename='csr_study.txt', dataframe=df)
    modifier_file = os.path.join(config_dir, modifiers)
    study.Clinical.Modifiers.df = pd.read_csv(modifier_file, sep='\t')

    blueprint_file = os.path.join(config_dir, blueprint)
    study.apply_blueprint(blueprint_file, omit_missing=True)

    tm_study = tmtk.toolbox.SkinnyExport(study, output_dir)

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
    df['CSR_DIAGNOSIS_MOD'] = df['DIAGNOSIS_ID']
    df['CSR_STUDY_MOD'] = df['STUDY_ID']
    df['CSR_BIOSOURCE_MOD'] = df['BIOSOURCE_ID']
    df['CSR_BIOMATERIAL_MOD'] = df['BIOMATERIAL_ID']

    df.loc[pd.notnull(df['BIOMATERIAL_ID']), 'CSR_BIOMATERIAL_MOD'] = df.loc[
        pd.notnull(df['BIOMATERIAL_ID']), 'SRC_BIOSOURCE_ID']

    return df


if __name__ == '__main__':
    main()
