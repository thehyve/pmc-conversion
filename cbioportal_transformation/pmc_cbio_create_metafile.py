#!/usr/bin/env python2.7

### Code to create meta files
### Author: Sander Tan, The Hyve

import sys
sys.dont_write_bytecode = True

def write_file(content, filename):
    output_file = open(filename, 'w')
    output_file.write(content)
    output_file.close()

def create_meta_content(cancer_study_identifier = None,
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
                        reference_genome_id = None
                        ):

    ### Required properties
    meta_content = ''
    if cancer_study_identifier is not None:
        meta_content = 'cancer_study_identifier: %s\n' % cancer_study_identifier

    ### Properties that are required for most data types
    if genetic_alteration_type is not None:
        meta_content = meta_content + 'genetic_alteration_type: %s\n' % genetic_alteration_type
        meta_content = meta_content + 'datatype: %s\n' % datatype
        meta_content = meta_content + 'data_filename: %s\n' % data_filename

    ### Properties that are required for mutation, gene expression and protein enrichment profiles
    if stable_id is not None:
        meta_content = meta_content + 'stable_id: %s\n' % stable_id
        meta_content = meta_content + 'profile_name: %s\n' % profile_name
        meta_content = meta_content + 'profile_description: %s\n' % profile_description
        meta_content = meta_content + 'show_profile_in_analysis_tab: %s\n' % show_profile_in_analysis_tab

    ### Properties that are required for meta_study
    if name is not None:
        meta_content = meta_content + 'type_of_cancer: %s\n' % type_of_cancer
        meta_content = meta_content + 'name: %s\n' % name
        meta_content = meta_content + 'short_name: %s\n' % short_name
        meta_content = meta_content + 'description: %s\n' % description
        meta_content = meta_content + 'add_global_case_list: %s\n' % add_global_case_list

    if variant_classification_filter is not None:
        meta_content = meta_content + 'variant_classification_filter:%s\n' % variant_classification_filter

    if pmid is not None:
        meta_content = meta_content + 'pmid: %s\n' % pmid
        
    if reference_genome_id is not None:
        meta_content = meta_content + 'reference_genome_id: %s\n' % reference_genome_id
        meta_content = meta_content + 'description: %s\n' % description
    return meta_content
