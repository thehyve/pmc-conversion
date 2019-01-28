#!/bin/bash
set -e
set -x

mkdir -p out

export PYTHONPATH=.:$PYTHONPATH
python scripts/csr_transformations.py \
	--input_dir test_data/default_data \
	--output_dir out \
	--config_dir config \
	--data_model data_model.json \
	--column_priority column_priority.json \
	--file_headers file_headers.json \
	--columns_to_csr columns_to_csr.json \
	--output_filename csr_transformation_data.tsv \
	--output_study_filename study_registry.tsv \
	--logging_config logging.cfg

python scripts/transmart_transformation.py \
	--csr_data_file out/csr_transformation_data.tsv \
	--study_registry_data_file out/study_registry.tsv \
	--output_dir out \
	--config_dir config \
	--blueprint blueprint.json \
	--modifiers modifiers.txt \
	--study_id CSR \
	--top_node "\\Public Studies\\CSR\\" \
	--security_required N
