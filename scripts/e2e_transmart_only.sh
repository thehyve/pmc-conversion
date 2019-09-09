#!/bin/bash
set -e
set -x

python3 -m luigi --module luigi-pipeline e2e_LoadDataFromNewFilesTaskTransmartOnly