#!/usr/bin/env python3

### Code to transform PMC processed data to cBioPortal staging files
### Author: Sander Tan, The Hyve

import sys

sys.dont_write_bytecode = True

import os
import argparse
import gzip
import pmc_cbio_transform_clinical
import pmc_cbio_create_metafile
import pmc_cbio_create_caselist
import pandas as pd
import shutil
import time
import json

### Define study properties
# TODO: Move study properties to configuration file
STUDY_ID = 'pmc_test'
NAME = "PMC - Test Study"
NAME_SHORT = "PMC - Test Study"
DESCRIPTION = '%s Transformed from PMC Test Data to cBioPortal format.' % time.strftime("%d-%m-%Y %H:%M")
TYPE_OF_CANCER = 'mixed'

def transform_study(clinical_input_file, ngs_dir, output_dir, descriptions):
    ### Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(descriptions,'r') as des:
        descriptions_dict = json.loads(des.read())

    ### Clinical data
    print('Transforming clinical data: %s' % clinical_input_file)

    ### Transform patient file
    # TODO: Nice to have - Duplicate steps by calling function twice --> Change flow of program
    patient_ids = pmc_cbio_transform_clinical.transform_clinical_data(clinical_inputfile=clinical_input_file,
                            output_dir=output_dir, clinical_type='patient',
                            study_id=STUDY_ID,
                            description_map=descriptions_dict)

    ### Transform sample file
    sample_ids = pmc_cbio_transform_clinical.transform_clinical_data(clinical_inputfile=clinical_input_file,
                            output_dir=output_dir, clinical_type='sample',
                            study_id=STUDY_ID,
                            description_map=descriptions_dict)

    ### Select NGS study files
    study_files = []
    for study_file in os.listdir(ngs_dir):
        if study_file[0] != '.':
            study_files.append(study_file)

    ### Create sample list, required for cnaseq case list
    mutation_samples = []
    cna_samples = []

    ### Transform mutation data files
    maf_result_df = pd.DataFrame()
    for study_file in study_files:
        if study_file.split('.')[-2:] == ['maf', 'gz']:
            maf_file_location = os.path.join(ngs_dir, study_file)
            maf_df = pd.read_csv(maf_file_location, comment = '#', sep = '\t')
            maf_result_df = pd.concat([maf_result_df, maf_df], ignore_index=True)

    if maf_result_df.shape[0] > 0:
        output_file = 'data_mutations.maf'
        output_file_location = os.path.join(output_dir, output_file)
        maf_result_df.to_csv(output_file_location, sep="\t", index = False, header=True)

        ### Create meta file
        meta_filename = os.path.join(output_dir, 'meta_mutations.txt')
        pmc_cbio_create_metafile.create_meta_content(meta_filename, cancer_study_identifier=STUDY_ID,
                                                     genetic_alteration_type='MUTATION_EXTENDED', datatype='MAF',
                                                     stable_id='mutations',
                                                     show_profile_in_analysis_tab='true', profile_name='Mutations',
                                                     profile_description='Mutation data', data_filename=output_file,
                                                     variant_classification_filter='', swissprot_identifier='accession')

        ### Create case list
        mutation_samples = maf_result_df['Tumor_Sample_Barcode'].unique().tolist()
        pmc_cbio_create_caselist.create_caselist(output_dir=output_dir, file_name='cases_sequenced.txt',
                                                 cancer_study_identifier=STUDY_ID,
                                                 stable_id='%s_sequenced' % STUDY_ID, case_list_name='Sequenced samples',
                                                 case_list_description='All sequenced samples',
                                                 case_list_category='all_cases_with_mutation_data',
                                                 case_list_ids="\t".join(mutation_samples))

        ### Test for samples in MAF files that are not in clinical data
        if not set(sample_ids).issuperset(set(mutation_samples)):
            raise ValueError("Found samples in MAF files that are not in clinical data:\n%s"
                             % ", ".join(set(mutation_samples).difference(set(sample_ids))))

    ### Transform CNA data files
    for study_file in study_files:
        if study_file.endswith('sha1'):
            continue
        file_type = study_file.split('.')[0]
        study_file_location = os.path.join(ngs_dir, study_file)

        ### CNA Segment data
        if study_file.split('.')[-1] == 'seg':
            print('Transforming segment data: %s' % study_file)

            # TODO: Merge possible multiple files per study, into 1 file per study
            output_file = 'data_cna_segments.seg'
            output_file_location = os.path.join(output_dir, output_file)
            shutil.copyfile(study_file_location, output_file_location)

            ### Replace header in old file
            with open(output_file_location) as segment_file:
                segment_lines = segment_file.readlines()
                segment_lines[0] = 'ID	chrom	loc.start	loc.end	num.mark	seg.mean\n'

            ### Write new file
            with open(output_file_location, 'w') as segment_file:
                segment_file.writelines(segment_lines)

            ### Create meta file
            meta_filename = os.path.join(output_dir, 'meta_cna_segments.txt')
            pmc_cbio_create_metafile.create_meta_content(meta_filename, cancer_study_identifier=STUDY_ID,
                                genetic_alteration_type='COPY_NUMBER_ALTERATION', datatype='SEG',
                                reference_genome_id='hg38', description='Segment data', data_filename=output_file)

        ### CNA Continuous
        elif 'data_by_genes' in study_file:
            print('Transforming continuous CNA data: %s' % study_file)

            # TODO: Merge possibly multiple files per study, into 1 file per study
            output_file = 'data_cna_continuous.txt'
            output_file_location = os.path.join(output_dir, output_file)
            shutil.copyfile(study_file_location, output_file_location)

            # Remove column and rename column names
            cna_data = pd.read_csv(output_file_location, sep='\t', na_values=[''], dtype={'Gene ID': str})
            cna_data.drop('Cytoband', axis=1, inplace=True)
            cna_data.rename(columns={'Gene Symbol': 'Hugo_Symbol', 'Gene ID': 'Entrez_Gene_Id'}, inplace=True)

            ### Remove negative Entrez IDs. This can lead to incorrect mapping in cBioPortal
            for index, row in cna_data.iterrows():
                if int(row['Entrez_Gene_Id']) < -1:
                    cna_data.loc[index, 'Entrez_Gene_Id'] = ''
            cna_data.to_csv(output_file_location, sep='\t', index=False, header=True)

            ### Create meta file
            meta_filename = os.path.join(output_dir, 'meta_cna_continuous.txt')
            pmc_cbio_create_metafile.create_meta_content(meta_filename, cancer_study_identifier=STUDY_ID,
                                genetic_alteration_type='COPY_NUMBER_ALTERATION', datatype='LOG2-VALUE',
                                stable_id='log2CNA', show_profile_in_analysis_tab='false',
                                profile_name='Copy-number alteration values',
                                profile_description='Continuous copy-number alteration values for each gene.',
                                data_filename=output_file)

            ### Create case list
            cna_samples = cna_data.columns[2:].tolist()
            pmc_cbio_create_caselist.create_caselist(output_dir=output_dir, file_name='cases_cna.txt', cancer_study_identifier=STUDY_ID,
                            stable_id='%s_cna' % STUDY_ID, case_list_name='CNA samples',
                            case_list_description='All CNA samples', case_list_category='all_cases_with_cna_data',
                            case_list_ids="\t".join(cna_samples))

            ### Test for samples in CNA files that are not in clinical data
            if not set(sample_ids).issuperset(set(cna_samples)):
                raise ValueError("Found samples in CNA files that are not in clinical data:\n%s"
                                 % ", ".join(set(cna_samples).difference(set(sample_ids))))


        ### CNA Discrete
        elif 'thresholded.by_genes' in study_file:
            print('Transforming discrete CNA data: %s' % study_file)

            # TODO: Merge possibly multiple files per study, into 1 file per study
            output_file = 'data_cna_discrete.txt'
            output_file_location = os.path.join(output_dir, output_file)
            shutil.copyfile(study_file_location, output_file_location)

            # Remove column and rename column names
            cna_data = pd.read_csv(output_file_location, sep='\t', na_values=[''], dtype={'Gene ID': str})
            cna_data.drop('Cytoband', axis=1, inplace=True)
            cna_data.rename(columns={'Gene Symbol': 'Hugo_Symbol', 'Locus ID': 'Entrez_Gene_Id'}, inplace=True)

            ### Remove negative Entrez IDs. This can lead to incorrect mapping in cBioPortal
            for index, row in cna_data.iterrows():
                if int(row['Entrez_Gene_Id']) < -1:
                    cna_data.loc[index, 'Entrez_Gene_Id'] = ''
            cna_data.to_csv(output_file_location, sep='\t', index=False, header=True)

            ### Create meta file
            meta_filename = os.path.join(output_dir, 'meta_cna_discrete.txt')
            pmc_cbio_create_metafile.create_meta_content(meta_filename, cancer_study_identifier=STUDY_ID,
                                genetic_alteration_type='COPY_NUMBER_ALTERATION', datatype='DISCRETE',
                                stable_id='gistic', show_profile_in_analysis_tab='true',
                                profile_name='Putative copy-number alterations from GISTIC',
                                profile_description='Putative copy-number alteration values for each gene from GISTIC 2.0. Values: -2 = homozygous deletion; -1 = hemizygous deletion; 0 = neutral / no change; 1 = gain; 2 = high level amplification.',
                                data_filename=output_file)
        elif study_file.split('.')[-2:] == ['maf', 'gz']:
            ### Mutations file are transformed in an other loop
            pass

        else:
            print("Unknown file type: %s" % study_file)

    ### Create cnaseq case list
    cnaseq_samples = list(set(mutation_samples + cna_samples))
    if len(cnaseq_samples) > 0:
        pmc_cbio_create_caselist.create_caselist(output_dir=output_dir, file_name='cases_cnaseq.txt', cancer_study_identifier=STUDY_ID,
                        stable_id='%s_cnaseq' % STUDY_ID, case_list_name='Sequenced and CNA samples',
                        case_list_description='All sequenced and CNA samples',
                        case_list_category='all_cases_with_mutation_and_cna_data',
                        case_list_ids="\t".join(cnaseq_samples))

    ### Create meta study file
    meta_filename = os.path.join(output_dir, 'meta_study.txt')
    pmc_cbio_create_metafile.create_meta_content(meta_filename, STUDY_ID, type_of_cancer=TYPE_OF_CANCER, name=NAME, short_name=NAME_SHORT,
                        description=DESCRIPTION, add_global_case_list='true')

    print('Done transforming files for %s' % STUDY_ID)

    ### Transformation completed
    print('Transformation of studies complete.')


def main(clinical_input_file, ngs_dir, output_dir, description_mapping):
    transform_study(clinical_input_file, ngs_dir, output_dir, description_mapping)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        usage="-c <clinical_input_file> -n <dir_for_ngs_files> -o <dir_for_output_files>",
        description="Transforms all files for all studies in input folder to cBioPortal staging files")

    arguments = parser.add_argument_group('Named arguments')

    arguments.add_argument("-c", "--clinical_input_file",
                           required=True,
                           help="Clinical input file")

    arguments.add_argument("-n", "--ngs_dir",
                           required=True,
                           help="Directory NGS files")

    arguments.add_argument("-o", "--output_dir",
                           required=True,
                           help="Directory where studies with cBioPortal staging files are written.")

    arguments.add_argument("-d", "--description_mapping",
                           required=True,
                           help="JSON file with a description mapping per CSR entity, {ENTITY: {COLUMN_NAME: DESCRIPTION}")

    args = parser.parse_args()
    main(args.clinical_input_file, args.ngs_dir, args.output_dir, args.description_mapping)
