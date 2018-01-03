#!/usr/bin/python2.7

### Code to transform PMC processed data to cBioPortal staging files
### Author: Sander Tan, The Hyve

import sys
sys.dont_write_bytecode = True

import os
import argparse
import pmc_cbio_transform_clinical
import pmc_cbio_create_metafile
import pmc_cbio_create_caselist
import pandas as pd
from shutil import copyfile


### Define study properties
STUDY_ID = 'pmc_test'
NAME = "PMC - Study name"
NAME_SHORT = "PMC - Study name"
DESCRIPTION = 'Transformed from .... to cBioPortal format.'
TYPE_OF_CANCER = 'mixed'

def transform_study(input_dir, output_dir, only_meta_files, only_clinical_files):
    
    ### Create output directories
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    ### Select study files
    study_files = []
    for study_file in os.listdir(input_dir):
        if study_file[0] != '.':
            study_files.append(study_file)

    ### Create sample list, required for cnaseq case list
    mutation_samples = []
    cna_samples = []

    ### Transform data files
    for study_file in study_files:
        file_type = study_file.split('.')[0]
        study_file_location = os.path.join(input_dir, study_file)

        ### Clinical data
        if study_file.split('_')[0] == 'clinical':
            print 'Transforming clinical patient data: %s' % study_file

            ### Transform patient file
            pmc_cbio_transform_clinical.transform_clinical_data(clinical_inputfile=study_file_location, output_dir= output_dir, clinical_type='patient', study_id = STUDY_ID)

            ### Transform sample file
            pmc_cbio_transform_clinical.transform_clinical_data(clinical_inputfile=study_file_location, output_dir= output_dir, clinical_type='sample', study_id = STUDY_ID)

        ### CNA Segment data
        elif study_file.split('.')[-1] == 'seg':
            print 'Transforming segment data: %s' % study_file

            # TODO: Concatenate all these files into 1 file per study

            ### Replace header in old file
            with open(study_file_location) as segment_file:
                segment_lines = segment_file.readlines()
                segment_lines[0] = 'ID	chrom	loc.start	loc.end	num.mark	seg.mean\n'

            ### Write new file
            data_filename = os.path.join(output_dir, study_file)
            with open(data_filename, 'w') as segment_file:
                segment_file.writelines(segment_lines)

            ### Create meta file
            meta_content = pmc_cbio_create_metafile.create_meta_content(cancer_study_identifier = STUDY_ID, genetic_alteration_type = 'COPY_NUMBER_ALTERATION', datatype = 'SEG', reference_genome_id = 'hg38', description = 'Segment data', data_filename = study_file)
            meta_filename = os.path.join(output_dir, 'meta_cna_segment.txt')
            pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

        ### Mutation data
        elif study_file.split('.')[-1] == 'maf':
            print 'Transforming mutation data: %s' % study_file

            # TODO: Concatenate all these files into 1 file per study
            data_filename = os.path.join(output_dir, study_file)
            copyfile(study_file_location, data_filename)

            ### Create meta file
            meta_content = pmc_cbio_create_metafile.create_meta_content(cancer_study_identifier = STUDY_ID, genetic_alteration_type = 'MUTATION_EXTENDED', datatype = 'MAF', stable_id = 'mutations', show_profile_in_analysis_tab = 'true', profile_name = 'Mutations', profile_description = 'Mutation data', data_filename = study_file, variant_classification_filter = '', swissprot_identifier = 'accession')
            meta_filename = os.path.join(output_dir, 'meta_mutations.txt')
            pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

            ### Create case list
            mutation_data = pd.read_csv(data_filename, sep='\t', dtype= {'Tumor_Sample_Barcode': str}, usecols = ['Tumor_Sample_Barcode'], skiprows = 1)
            mutation_samples = mutation_data['Tumor_Sample_Barcode'].unique().tolist()
            pmc_cbio_create_caselist.create_caselist(output_dir = output_dir, file_name = 'cases_sequenced.txt', cancer_study_identifier = STUDY_ID, stable_id = '%s_sequenced' % STUDY_ID, case_list_name = 'Sequenced samples', case_list_description = 'All sequenced samples', case_list_category = 'all_cases_with_mutation_data', case_list_ids = "\t".join(mutation_samples))

        ### CNA Continuous
        elif 'data_by_genes' in study_file:
            print 'Transforming continuous CNA data: %s' % study_file

            # TODO: Merge all these files into 1 file per study
            data_filename = os.path.join(output_dir, study_file)

            ### Extract the gene id and data columns
            os.system("cut -f1-2,4- %s > %s" % (study_file_location, data_filename))
            
            ### Rename column names
            os.system("sed -i '' 's/Gene ID/Entrez_Gene_Id/' %s" % data_filename)
            os.system("sed -i '' 's/Gene Symbol/Hugo_Symbol/' %s" % data_filename)

            ### Remove negative Entrez IDs. This can lead to incorrect mapping in cBioPortal
            cna_data = pd.read_csv(data_filename, sep='\t', na_values=[''], dtype= {'Entrez_Gene_Id': str})
            for index, row in cna_data.iterrows():
                if int(row['Entrez_Gene_Id']) < -1:
                    cna_data.loc[index, 'Entrez_Gene_Id'] = ''
            cna_data.to_csv(data_filename, sep='\t', index=False, header=True)

            ### Create meta file
            meta_content = pmc_cbio_create_metafile.create_meta_content(cancer_study_identifier = STUDY_ID, genetic_alteration_type = 'COPY_NUMBER_ALTERATION', datatype = 'CONTINUOUS', stable_id = 'linear_CNA', show_profile_in_analysis_tab = 'false', profile_name = 'Copy-number alteration values', profile_description = 'Continuous copy-number alteration values for each gene.', data_filename = study_file)
            meta_filename = os.path.join(output_dir, 'meta_cna_continuous.txt')
            pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

            ### Create case list
            cna_samples = cna_data.columns[2:].tolist()
            pmc_cbio_create_caselist.create_caselist(output_dir = output_dir, file_name = 'cases_cna.txt', cancer_study_identifier = STUDY_ID, stable_id = '%s_cna' % STUDY_ID, case_list_name = 'CNA samples', case_list_description = 'All CNA samples', case_list_category = 'all_cases_with_cna_data', case_list_ids = "\t".join(cna_samples))

        ### CNA Discrete
        elif 'thresholded.by_genes' in study_file:
            print 'Transforming discrete CNA data: %s' % study_file

            # TODO: Merge all these files into 1 file per study
            data_filename = os.path.join(output_dir, study_file)

            ### Extract the gene id and data columns
            os.system("cut -f1-2,4- %s > %s" % (study_file_location, data_filename))

            ### Rename column names
            os.system("sed -i '' 's/Locus ID/Entrez_Gene_Id/' %s" % data_filename)
            os.system("sed -i '' 's/Gene Symbol/Hugo_Symbol/' %s" % data_filename)

            ### Remove negative Entrez IDs. This can lead to incorrect mapping in cBioPortal
            cna_data = pd.read_csv(data_filename, sep='\t', na_values=[''], dtype= {'Entrez_Gene_Id': str})
            for index, row in cna_data.iterrows():
                if int(row['Entrez_Gene_Id']) < -1:
                    cna_data.loc[index, 'Entrez_Gene_Id'] = ''
            cna_data.to_csv(data_filename, sep='\t', index=False, header=True)

            ### Create meta file
            meta_content = pmc_cbio_create_metafile.create_meta_content(cancer_study_identifier = STUDY_ID, genetic_alteration_type = 'COPY_NUMBER_ALTERATION', datatype = 'DISCRETE', stable_id = 'gistic', show_profile_in_analysis_tab = 'true', profile_name = 'Putative copy-number alterations from GISTIC', profile_description = 'Putative copy-number alteration values for each gene from GISTIC 2.0. Values: -2 = homozygous deletion; -1 = hemizygous deletion; 0 = neutral / no change; 1 = gain; 2 = high level amplification.', data_filename = study_file)
            meta_filename = os.path.join(output_dir, 'meta_cna_discrete.txt')
            pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

        else:
            print "Unknown file type: %s" % study_file

    ### Create cnaseq case list
    cnaseq_samples = list(set(mutation_samples + cna_samples))
    if len(cnaseq_samples) > 0:
        pmc_cbio_create_caselist.create_caselist(output_dir = output_dir, file_name = 'cases_cnaseq.txt', cancer_study_identifier = STUDY_ID, stable_id = '%s_cnaseq' % STUDY_ID, case_list_name = 'Sequenced and CNA samples', case_list_description = 'All sequenced and CNA samples', case_list_category = 'all_cases_with_mutation_and_cna_data', case_list_ids = "\t".join(cnaseq_samples))

    ### Create meta study file
    meta_content = pmc_cbio_create_metafile.create_meta_content(STUDY_ID, type_of_cancer = TYPE_OF_CANCER, name = NAME, short_name = NAME_SHORT, description = DESCRIPTION, add_global_case_list = 'true')
    meta_filename = os.path.join(output_dir, 'meta_study.txt')
    pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

    print 'Done transforming files for %s' % STUDY_ID

    ### Transformation completed
    print 'Transformation of studies complete.'


def main(input_dir, output_dir, only_meta_files, only_clinical_files):
    transform_study(input_dir, output_dir, only_meta_files, only_clinical_files)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        usage = "-i <dir_for_input_files> -o <dir_for_output_files>",
        description = "Transforms all files for all studies in input folder to cBioPortal staging files")

    arguments = parser.add_argument_group('Named arguments')

    arguments.add_argument("-i", "--input_dir",
        required = True,
        help = "Directory input files")

    arguments.add_argument("-o", "--output_dir",
        required = True,
        help = "Directory where studies with cBioPortal staging files are written.")

    arguments.add_argument("-m", "--only_meta_files",
        required = False,
        action = 'store_true',
        default = False,
        help = "Optional: If -m added, only create meta files (fast).")
    
    arguments.add_argument("-c", "--only_clinical_files",
        required = False,
        action = 'store_true',
        default = False,
        help = "Optional: If -c added, only clinical data files will be transformed.")

    args = parser.parse_args()
    main(args.input_dir, args.output_dir, args.only_meta_files, args.only_clinical_files)
