#!/bin/bash
set -e
set -x

SCRIPT_DIR=$(dirname $0)
cd ${SCRIPT_DIR}
./remove_done_files.sh
python -m luigi --module luigi-pipeline TransmartDataLoader
python -m luigi --module luigi-pipeline TransmartApiTask
./remove_done_files.sh