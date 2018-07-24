#!/usr/bin/env python3

### Code to create case lists
### Author: Sander Tan, The Hyve

import os
import logging

logger = logging.getLogger(__name__)
logger.name = logger.name.rsplit('.',1)[1]

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
    caselist_content = []
    caselist_content.append('cancer_study_identifier: %s' % cancer_study_identifier)
    caselist_content.append('stable_id: %s' % stable_id)
    caselist_content.append('case_list_name: %s' % case_list_name)
    caselist_content.append('case_list_description: %s' % case_list_description)
    caselist_content.append('case_list_category: %s' % case_list_category)
    caselist_content.append('case_list_ids: %s' % case_list_ids)

    ### Write file
    logger.debug('Writing cBioPortal caselist to {} for study {}'.\
                 format(file_name, cancer_study_identifier))
    with open(os.path.join(case_list_dir, file_name), 'w') as caselist_output_file:
        caselist_output_file.write('\n'.join(caselist_content) + '\n')
    return
