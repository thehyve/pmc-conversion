#!/bin/bash
set -e
set -x

$(dirname "$0")/remove_done_files.sh
python3 -m luigi --module luigi-pipeline LoadDataFromNewFilesTask
