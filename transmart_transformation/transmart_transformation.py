import tmtk
import pandas as pd
import click
import configparser
import chardet

class Config(object):

    def __init__(self, config_file):
        # def read_config_dict_from_file(config_item):
        #     with open(self.config.get('GENERAL', config_item)) as f:
        #         dict_ = json.loads(f.read())
        #     # dict_upper = {k.upper():v for k,v in dict_.items()}
        #     return dict_

        # def read_config_list_from_file(config_item):
        #     with open(self.config.get('GENERAL', config_item)) as f:
        #         list_ = [line.rstrip('\n') for line in f]
        #     return list_

        self.config_file = config_file.name
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)

        self.blueprint = self.config.get('DATA','blueprint_file')
        self.csr_data_file = self.config.get('DATA','csr_data_file')
        self.modifiers = self.config.get('DATA','modifier_file')

        # self.output_file = self.config.get('OUTPUT', 'target_file')
        # self.output_file = self.config.get('OUTPUT','output_dir')
        self.output_file = self.config.get('OUTPUT', 'export_dir')

        self.study_id = self.config.get('STUDY', 'study_id')
        self.top_node = self.config.get('STUDY', 'top_node')
        self.security_required = self.config.getboolean('STUDY','security_required')


@click.command()
@click.argument('config_file', type=click.File('r'))
def main(config_file):

    config = Config(config_file)


    df = pd.read_csv(config.csr_data_file, sep='\t', encoding=get_encoding(config.csr_data_file))

    # Add modifiers
    df['CSR_DIAGNOSE_MOD'] = df['DIAGNOSIS_ID']
    df['CSR_STUDY_MOD'] = df['STUDY_ID']
    df['CSR_BIOSOURCE_MOD'] = df['BIOSOURCE_ID']
    df['CSR_BIOMATERIAL_MOD'] = df['BIOMATERIAL_ID']

    df.loc[pd.notnull(df['BIOMATERIAL_ID']),'CSR_BIOMATERIAL_MOD'] = df.loc[
        pd.notnull(df['BIOMATERIAL_ID']),'SRC_BIOSOURCE_ID']

    #csr.loc[pd.notnull(csr['BIOMATERIAL_ID']), 'CSR_BIOSOURCE_MOD'] = csr.loc[
    #    pd.notnull(csr['BIOMATERIAL_ID']), 'SRC_BIOSOURCE_ID']


    study = tmtk.Study()
    study.study_id = config.study_id
    study.top_node = config.top_node
    study.security_required = config.security_required


    study.Clinical.add_datafile(filename='csr_study.txt', dataframe=df)
    study.Clinical.Modifiers.df = pd.read_csv(config.modifiers, sep='\t')

    study.apply_blueprint(config.blueprint, omit_missing=True)

    tm_study = tmtk.toolbox.SkinnyExport(study, config.output_file)

    #print('\n\n\n\n\nWorking hard!\n\n\n')

    tm_study.build_observation_fact()
    #print('\n\n\n\n\nI\'ve build the observations\n\n\n')

    print(tm_study.observation_fact.df.shape)
    tm_study.to_disk()

    # save column mapping etc?
    # Run skinny loader export
    # Save to disc in desginated area


def get_encoding(file_name):
    """Open the file and determine the encoding, returns the encoding cast to lower"""
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()




if __name__ == '__main__':
    main()