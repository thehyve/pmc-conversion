def collect_tree_node_dimension_violations(blueprint):
    return [ f"{column}: No dimension metadata tag specified." for column, declarations in blueprint.items() if _no_dimension_field(declarations) ]


def _no_dimension_field(column_declarations):
    return 'metadata_tags' not in column_declarations or 'dimension' not in column_declarations['metadata_tags']
