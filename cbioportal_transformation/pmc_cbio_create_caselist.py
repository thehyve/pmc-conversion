#!/usr/bin/python2.7

### Code to create case lists
### Author: Sander Tan, The Hyve

import os

def create_caselist(output_dir,
                    file_name,
                    cancer_study_identifier = None,
                    stable_id = None,
                    case_list_name = None,
                    case_list_description = None,
                    case_list_category = None,
                    case_list_ids = None
                    ):

    ### Define output directory
    case_list_dir = os.path.join(output_dir, 'case_lists')
    if not os.path.exists(case_list_dir):
        os.mkdir(case_list_dir)

    ### Create contents
    caselist_content = 'cancer_study_identifier: %s\n' % cancer_study_identifier
    caselist_content = caselist_content + 'stable_id: %s\n' % stable_id
    caselist_content = caselist_content + 'case_list_name: %s\n' % case_list_name
    caselist_content = caselist_content + 'case_list_description: %s\n' % case_list_description
    caselist_content = caselist_content + 'case_list_category: %s\n' % case_list_category
    caselist_content = caselist_content + 'case_list_ids: %s\n' % case_list_ids

    ### Write file
    case_output_file = open(os.path.join(case_list_dir, file_name), 'w')
    case_output_file.write(caselist_content)
    case_output_file.close()
    return
