dimensions = {'patient', 'diagnosis id', 'biosource id', 'biomaterial id'}


def collect_tree_node_dimension_violations(blueprint):
    violations = []
    for column, declarations in blueprint.items():
        if _no_dimension_field(declarations):
            violations.append(f"{column}: No dimension metadata tag specified.")
        elif _get_dimension(declarations).lower() not in dimensions:
            violations.append(f"{column}: \"{_get_dimension(declarations)}\" dimension is not recognised.")
    return violations


def _no_dimension_field(column_declarations):
    return 'metadata_tags' not in column_declarations or 'dimension' not in column_declarations['metadata_tags']


def _get_dimension(column_declarations):
    return column_declarations['metadata_tags']['dimension']
