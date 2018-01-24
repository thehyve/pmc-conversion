#!/bin/bash
set -e

./remove_done_files.sh
python -m luigi --module scripts LoadTransmartDataFromHistoryTask
./remove_done_files.sh