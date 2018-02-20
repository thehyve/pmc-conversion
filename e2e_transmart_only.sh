#!/bin/bash
set -e
set -x

SCRIPT_DIR=$(dirname $0)
cd ${SCRIPT_DIR}
python3 -m luigi --module scripts e2e_LoadDataFromNewFilesTaskTransmartOnly