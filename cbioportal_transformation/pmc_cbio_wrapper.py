#!/usr/bin/python2.7

### Code to transform PMC processed data to cBioPortal staging files
### Author: Sander Tan, The Hyve

import sys
sys.dont_write_bytecode = True

import os
import argparse
import pmc_cbio_transform_clinical
import pmc_cbio_create_metafile
from shutil import copyfile


### Define study properties
STUDY_ID = 'pmc_test'
NAME = "PMC - Study name"
NAME_SHORT = "PMC - Study name"
DESCRIPTION = 'Transformed from .... to cBioPortal format.'
TYPE_OF_CANCER = 'mixed'

def transform_study(input_dir, output_dir, only_meta_files, only_clinical_files):
    
    ### Select study files
    study_files = []
    for study_file in os.listdir(input_dir):
        if study_file[0] != '.':
            study_files.append(study_file)
            
    ### Transform data files
    for study_file in study_files:
        file_type = study_file.split('.')[0]
        study_file_location = os.path.join(input_dir, study_file)

        ### Clinical data
        if study_file.split('_')[0] == 'clinical':
            print 'Transforming clinical patient data: %s' % study_file
            
            # TODO Extract patients, add to patient file
            # TODO Extract samples, add to sample file
            pmc_cbio_transform_clinical.transform_clinical_data(clinical_inputfile=study_file_location, output_dir= output_dir, clinical_type='patient', study_id = STUDY_ID)

        ### CNA Segment data
        elif study_file.split('.')[-1] == 'seg':
            # TODO: Concatenate all these files into 1 file per study

            ### Replace header 
            with open(study_file_location) as segment_file:
                segment_lines = segment_file.readlines()
                segment_lines[0] = 'Sample	Chromosome	Start	End	Num_Probes	Segment_Mean'
            with open(study_file_location, 'w') as segment_file:
                segment_file.writelines(segment_lines)

            ### Create meta file
            meta_content = pmc_cbio_create_metafile.create_meta_content(cancer_study_identifier = STUDY_ID, genetic_alteration_type = 'COPY_NUMBER_ALTERATION', datatype = 'SEG', reference_genome_id = 'hg38', description = 'Segment data', data_filename = study_file)
            meta_filename = os.path.join(output_dir, 'meta_cna_segment.txt')
            pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

        ### Mutation data
        elif study_file.split('.')[-1] == 'maf':

            # TODO: Concatenate all these files into 1 file per study
            data_filename = os.path.join(output_dir, study_file)
            copyfile(study_file_location, data_filename)

            ### Create meta file
            meta_content = pmc_cbio_create_metafile.create_meta_content(cancer_study_identifier = STUDY_ID, genetic_alteration_type = 'MUTATION_EXTENDED', datatype = 'MAF', stable_id = 'mutations', show_profile_in_analysis_tab = 'true', profile_name = 'Mutations', profile_description = 'Mutation data', data_filename = study_file, variant_classification_filter = '')
            meta_filename = os.path.join(output_dir, 'meta_mutations.txt')
            pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

        ### CNA Continuous
        elif 'data_by_genes' in study_file:
            # TODO: Merge all these files into 1 file per study
            data_filename = os.path.join(output_dir, study_file)
            copyfile(study_file_location, data_filename)
            
            ### Create meta file
            meta_content = pmc_cbio_create_metafile.create_meta_content(cancer_study_identifier = STUDY_ID, genetic_alteration_type = 'COPY_NUMBER_ALTERATION', datatype = 'CONTINUOUS', stable_id = 'linear_CNA', show_profile_in_analysis_tab = 'false', profile_name = 'Copy-number alteration values', profile_description = 'Continuous copy-number alteration values for each gene.', data_filename = study_file)
            meta_filename = os.path.join(output_dir, 'meta_cna_continuous.txt')
            pmc_cbio_create_metafile.write_file(meta_content, meta_filename)

        ### CNA Continuous
        elif 'thresholded.by_genes' in study_file:
            # TODO: Merge all these files into 1 file per study
            data_filename = os.path.join(output_dir, study_file)
            copyfile(study_file_location, data_filename)

            ### Create meta file
            meta_content = pmc_cbio_create_metafile.create_meta_content(cancer_study_identifier = STUDY_ID, genetic_alteration_type = 'COPY_NUMBER_ALTERATION', datatype = 'DISCRETE', stable_id = 'gistic', show_profile_in_analysis_tab = 'true', profile_name = 'Putative copy-number alterations from GISTIC', profile_description = 'Putative copy-number alteration values for each gene from GISTIC 2.0. Values: -2 = homozygous deletion; -1 = hemizygous deletion; 0 = neutral / no change; 1 = gain; 2 = high level amplification.', data_filename = study_file)
            meta_filename = os.path.join(output_dir, 'meta_cna_discrete.txt')
            pmc_cbio_create_metafile.write_file(meta_content, meta_filename)


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
