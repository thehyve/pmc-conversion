dimensions = {'patient', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id'}


def collect_tree_node_dimension_violations(blueprint):
    for column, declarations in blueprint.items():
        if _no_dimension_field(declarations):
            yield f"{column}: No dimension metadata tag specified."
        elif _get_dimension(declarations) not in dimensions:
            yield f"{column}: \"{_get_dimension(declarations)}\" dimension is not recognised."


def _no_dimension_field(column_declarations):
    return 'metadata_tags' not in column_declarations or 'dimension' not in column_declarations['metadata_tags']


def _get_dimension(column_declarations):
    return column_declarations['metadata_tags']['dimension']
