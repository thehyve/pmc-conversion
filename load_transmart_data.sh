#!/bin/bash
set -e
set -x

SCRIPT_DIR=$(dirname $0)
cd ${SCRIPT_DIR}
./remove_done_files.sh
python -m luigi --module scripts LoadTransmartStudy
./remove_done_files.sh