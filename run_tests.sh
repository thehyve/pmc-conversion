
export PYTHONPATH=./tests/:./scripts/:./test_data/:$PYTHONPATH

test_folder="./tests/"
coverage run -m unittest discover -s $test_folder -p test_*.py
