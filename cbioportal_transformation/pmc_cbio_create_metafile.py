#!/usr/bin/env python3

# Code to create meta files
# Author: Sander Tan, The Hyve

import sys
sys.dont_write_bytecode = True


def create_meta_content(file_name,
                        cancer_study_identifier = None,
                        genetic_alteration_type = None,
                        datatype = None,
                        data_filename = None,
                        stable_id = None,
                        profile_name = None,
                        profile_description = None,
                        show_profile_in_analysis_tab = None,
                        type_of_cancer = None,
                        name = None,
                        short_name = None,
                        description = None,
                        add_global_case_list = None,
                        variant_classification_filter = None,
                        pmid = None,
                        reference_genome_id = None,
                        swissprot_identifier = None
                        ):

    # Required properties
    meta_content = []
    if cancer_study_identifier is not None:
        meta_content.append('cancer_study_identifier: %s' % cancer_study_identifier)

    # Properties that are required for most data types
    if genetic_alteration_type is not None:
        meta_content.append('genetic_alteration_type: %s' % genetic_alteration_type)
        meta_content.append('datatype: %s' % datatype)
        meta_content.append('data_filename: %s' % data_filename)

    # Properties that are required for mutation, gene expression and protein enrichment profiles
    if stable_id is not None:
        meta_content.append('stable_id: %s' % stable_id)
        meta_content.append('profile_name: %s' % profile_name)
        meta_content.append('profile_description: %s' % profile_description)
        meta_content.append('show_profile_in_analysis_tab: %s' % show_profile_in_analysis_tab)

    # Properties that are required for meta_study
    if name is not None:
        meta_content.append('type_of_cancer: %s' % type_of_cancer)
        meta_content.append('name: %s' % name)
        meta_content.append('short_name: %s' % short_name)
        meta_content.append('description: %s' % description)
        meta_content.append('add_global_case_list: %s' % add_global_case_list)

    if variant_classification_filter is not None:
        meta_content.append('variant_classification_filter: %s' % variant_classification_filter)

    if swissprot_identifier is not None:
        meta_content.append('swissprot_identifier: %s' % swissprot_identifier)

    if pmid is not None:
        meta_content.append('pmid: %s' % pmid)

    if reference_genome_id is not None:
        meta_content.append('reference_genome_id: %s' % reference_genome_id)
        meta_content.append('description: %s' % description)

    # Write file
    with open(file_name, 'w') as meta_output_file:
        meta_output_file.write('\n'.join(meta_content) + '\n')
    return
