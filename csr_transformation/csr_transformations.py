import pandas as pd
import chardet
import os


def main():
    ## Test file dir !!!
    os.chdir('/Users/wibopipping/Projects/PMC/csr_exampe_files')

    # from config get input directory
    files = {'individual':{}, 'diagnosis':{}, 'biobank':{}, 'study':{}}
    for file in os.listdir():# CONFIG.INPUTDIR
        encoding = get_encoding(file)

        df = input_file_to_df(file, encoding)

        file_type = determine_file_type(df)
        files[file_type].update({file:df})

    files['individual']

# TODO:
# Steps to implement:
# - Check if all expected files are present
# - Check encoding --> use correct encoding to read the file

# - Check if the files have the correct headers

# - Combine files into one big overview

# - Output to a tsv file --> Encode to UTF-8 format

# Things to keep in mind:
# - need a mapping to deduplicate potential double values from the CSR --> requires a order of correct datafiles
# - find a way to manage inconsitency in the column headers --> toupper?
# - config file needs mappings




## Check file encoding, assuming utf-8.
def get_encoding(file_name):
    with open(file_name, 'rb') as file:
        file_encoding = chardet.detect(file.read())['encoding']
    return file_encoding.lower()


def input_file_to_df(file_name, encoding, seperator='\t'):
    df = pd.read_csv(file_name, sep=seperator, encoding=encoding)
    df.columns = map(lambda x: str(x).upper(), df.columns)
    return df


def determine_file_type(df):
    col = df.columns

    if 'BIOSOURCE_ID' in col or 'BIOMATERIAL_ID' in col:
        return 'biobank'
    if 'DIAGNOSIS_ID' in col:
        return 'diagnosis'
    if 'STUDY_ID' in col:
        return 'study'
    if 'INDIVIDUAL_ID' in col:
        return 'individual'


if __name__ == '__main__':
    main()
