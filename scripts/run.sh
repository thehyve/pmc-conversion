#!/bin/bash
set -e
set -x

SCRIPT_DIR=$(dirname "$0")
cd "${SCRIPT_DIR}/.."
if python -m luigi --module luigi-pipeline LoadDataFromNewFilesTask; then
    echo "Pipeline finished successfully"
else
    python scripts/email_client.py --config email_config.cfg
fi
