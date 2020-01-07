#!/bin/bash
set -e
set -x

SCRIPT_DIR=$(dirname "$0")
cd "${SCRIPT_DIR}/.."
if python -m luigi --module luigi-pipeline CbioportalDataLoading; then
    echo "Loading to cBioPortal finished successfully"
else
    echo "Error"
    exit 1
fi
