#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Use: $0 <commit sha1>"
    exit 1
fi

COMMIT_HEX_SHA=$1

python -m luigi --module scripts LoadDataFromHistoryTask --GitVersionTask-commit-hexsha $COMMIT_HEX_SHA