class BlueprintValidations:
    def __init__(self, modifier_dimensions=set()):
        self.dimensions = {'patient'} | modifier_dimensions

    def collect_tree_node_dimension_violations(self, blueprint):
        for column, declarations in blueprint.items():
            if self._no_dimension_field(declarations):
                yield f"{column}: No dimension metadata tag specified."
            elif self._get_dimension(declarations) not in self.dimensions:
                yield f"{column}: \"{self._get_dimension(declarations)}\" dimension is not recognised."

    def _no_dimension_field(self, column_declarations):
        return 'metadata_tags' not in column_declarations or 'dimension' not in column_declarations['metadata_tags']

    def _get_dimension(self, column_declarations):
        return column_declarations['metadata_tags']['dimension']
