import os
import sys
import tmtk
import json
import click
import chardet
import pandas as pd
import datetime as dt


@click.command()
@click.option('--csr_data_file', type=click.Path(exists=True))
@click.option('--study_registry_data_file', type=click.Path(exists=True))
@click.option('--output_dir', type=click.Path(exists=True))
@click.option('--config_dir', type=click.Path(exists=True))
@click.option('--blueprint')
@click.option('--modifiers')
@click.option('--study_id')
@click.option('--top_node')
@click.option('--security_required')
@click.option('--save_batch_study', is_flag=True)
def main(csr_data_file, study_registry_data_file, output_dir,
         config_dir, blueprint, modifiers, study_id, top_node, security_required, save_batch_study):

    # Process Central subject registry data
    df = pd.read_csv(csr_data_file, sep='\t', encoding=get_encoding(csr_data_file), dtype=object)
    df = add_modifiers(df)

    # Create study object
    study = tmtk.Study()
    study.study_id = study_id
    study.top_node = top_node
    if security_required == 'N':
        study.security_required = False
    else:
        study.security_required = True

    study.Clinical.add_datafile(filename='csr_study.txt', dataframe=df)
    modifier_file = os.path.join(config_dir, modifiers)
    try:
        study.Clinical.Modifiers.df = pd.read_csv(modifier_file, sep='\t')
    except FileNotFoundError as fnfe:
        print('Modifier file, {} not found. {}'.format(modifier_file, fnfe))
        #logger.error('')
        sys.exit(1)

    blueprint_file = os.path.join(config_dir, blueprint)
    try:
        study.apply_blueprint(blueprint_file, omit_missing=True)
        study = add_meta_data(study)
    except FileNotFoundError as fnfe:
        print('Blueprint file, {} not found. {}'.format(blueprint_file,fnfe))
        #logger.error()
        sys.exit(1)

    # Process study registry data
    std_reg = pd.read_csv(study_registry_data_file, sep='\t', encoding=get_encoding(study_registry_data_file), dtype=object)
    study_filename = 'study_data.txt'
    with open(blueprint_file,'r') as bpf:
        bp = json.load(bpf)
    study_data, study_col_map = generate_study_column_mapping(std_reg, study_filename, bp)

    # Combine CSR and study registry data in study object
    study.Clinical.add_datafile(filename=study_filename, dataframe=study_data)
    study_cm = study.Clinical.ColumnMapping.df.copy()
    study_cm = study_cm.drop(index=(study_filename,)).reset_index(drop=True)
    study_cm = study_cm.append(study_col_map, ignore_index=True)
    col_map_index = tmtk.utils.Mappings.column_mapping_header[0:3:2]
    study.Clinical.ColumnMapping.df = study_cm.set_index(col_map_index, drop=False)

    if save_batch_study:
        study_dir = os.path.dirname(csr_data_file)
        study.write_to(os.path.join(study_dir, study_id), overwrite=True)

    tm_study = tmtk.toolbox.SkinnyExport(study, output_dir)
    tm_study.build_observation_fact()
    tm_study.to_disk()

    sys.exit(0)


def generate_study_column_mapping(study, filename, blueprint):
    col_map = pd.DataFrame(columns=['filename','cat_cd','col_num','data_label','col5','col6','concept_type'],
                           data={'filename': [filename],'cat_cd': ['Subjects'],'col_num': [1],'data_label': ['SUBJ_ID'],
                            'col5': [''],'col6': [''],'concept_type': ['']}
                           )

    # Set index to individual and update column index to multiindex (on per study basis)
    study_ = set_study_index(study)

    # Build column mapping by looping over studies, and or each study looping over the columns
    # col_num starts at 2 as the first column will be the SUBJ_ID
    col_num = 2
    for study_id in study_.columns.get_level_values(0).unique():
        subset = study_.loc[:,study_id]
        try:
            unique_acronym = subset['ACRONYM'].dropna().unique()
        except KeyError as ke:
            print('Expected ACRONYM, but not found for study id {}, skipping. {}'.format(study_id, ke))
            col_num += subset.shape[1]
            continue

        if unique_acronym.size == 1:
            acronym = unique_acronym[0]
        else:
            print('Error, ACRONYM not unique for study id {}, skipping'.format(study_id))
            col_num += subset.shape[1]
            continue

        for col in subset.columns:
            if col in blueprint:
                cat_cd = '+'.join([blueprint[col]['path'],acronym])
                data_label = blueprint[col]['label']
                concept_type = 'CATEGORICAL' if blueprint[col]['force_categorical'] == 'Y' else ''
            else:
                print('Error, no mapping found for {} in blueprint.json. Setting to OMIT'.format(col))
                cat_cd = ''
                data_label = 'OMIT'
                concept_type = ''

            col_map = col_map.append({'filename': filename,
                            'cat_cd': cat_cd,
                            'col_num': col_num,
                            'data_label': data_label,
                            'col5': '',
                            'col6': '',
                            'concept_type': concept_type}, ignore_index=True)
            col_num+=1

    # Change column mapping column names and set study data column index back to single index
    col_map.columns = tmtk.utils.Mappings.column_mapping_header
    study_.columns = ['|'.join(col) for col in study_.columns]
    study_ = study_.reset_index(drop=False)

    return study_, col_map


def set_study_index(study):
    study_ = study.set_index('INDIVIDUAL_ID')
    multi_index = []
    for col in study_.columns:
        split = col.split('|')
        if len(split) == 2:
            multi_index.append((split[1],split[0]))
        else:
            print('Column {} does not provide 2 items when split on \'|\''.format(col))
    study_.columns = pd.MultiIndex.from_tuples(multi_index)

    return study_




def get_encoding(file_name):
    """Open the file and determine the encoding, returns the encoding cast to lower"""
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()


def add_modifiers(df):
    df['CSR_DIAGNOSIS_MOD'] = df['DIAGNOSIS_ID']
    df['CSR_BIOSOURCE_MOD'] = df['BIOSOURCE_ID']
    df['CSR_BIOMATERIAL_MOD'] = df['BIOMATERIAL_ID']
    return df


def add_meta_data(study):
    date = dt.datetime.now().strftime('%d-%m-%Y')
    study.ensure_metadata()

    header = study.Tags.header
    study_meta_data = [
        ['\\'],
        ['Load date'],
        [date],
        ['3']
    ]

    meta_data_df = pd.DataFrame.from_items(list((zip(header, study_meta_data))))
    study.Tags.df = study.Tags.df.append(meta_data_df, ignore_index=True)

    return study


if __name__ == '__main__':
    main()
