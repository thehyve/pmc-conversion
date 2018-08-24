#!/bin/bash
set -e
set -x

SCRIPT_DIR=$(dirname $0)
cd ${SCRIPT_DIR}
python -m luigi --module luigi-pipeline LoadDataFromNewFilesTask