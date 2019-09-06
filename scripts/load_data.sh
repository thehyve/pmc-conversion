#!/bin/bash
set -e
set -x

SCRIPT_DIR=$(dirname $0)
cd ${SCRIPT_DIR}
./load_transmart_data.sh
./load_cbioportal_data.sh