import json
import sys

'''
    Tries to guess and fill in subject_dimension for each column declaration in the blueprint json.
    Throws exception otherwise.
    WARNING: Human review is required afterwards!
    EXAMPLE: cat ../../config/blueprint.json | python fill_subject_dimensions.py > blueprint.json
'''


def _unify_path(path):
    return path.replace('_', ' ').lower()


blueprint_json_txt = '\n'.join(sys.stdin.readlines())
blueprint_json = json.loads(blueprint_json_txt)
for column, declarations in blueprint_json.items():
    if 'label' in declarations and declarations['label'] in ['MODIFIER', 'OMIT', 'SUBJ_ID']:
        continue
    if 'path' not in declarations:
        print("{} column declaration does not have path. Skip it.".format(column), file=sys.stderr)
        continue
    path = _unify_path(declarations['path'])
    if 'patient information' in path or path.endswith('study information'):
        subject_dimension = 'patient'
    elif 'diagnosis information' in path:
        subject_dimension = 'Diagnosis ID'
    elif 'biosource information' in path:
        subject_dimension = 'Biosource ID'
    elif 'biomaterial information' in path:
        subject_dimension = 'Biomaterial ID'
    else:
        print("Failed to categorise {} column by its path.".format(column), file=sys.stderr)
        continue

    if 'metadata_tags' not in declarations:
        declarations['metadata_tags'] = {}
    if 'subject_dimension' not in declarations['metadata_tags']:
        declarations['metadata_tags']['subject_dimension'] = subject_dimension
json.dump(blueprint_json, sys.stdout, indent=4)
