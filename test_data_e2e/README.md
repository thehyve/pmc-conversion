# E2E TESTDATA

This folder is complete with utilities to help create CSR-compatible test datasets for E2E testing.
Test data updates should coordinate with 
[csr2transmart](https://github.com/thehyve/python_csr2transmart) developments.


### Folder structure

`current/` contains the latest test dataset compatible with `csr2transmart`. Inside this folder, 
you will find:
  - `config/` for configuration files (ontology and sources)
  - `dropzone/` for the actual data

Inside `dropzone`, you should have at least two test dataset versions (e.g. `full_dataset` and 
`alternative`). This allows to quickly switch between them, which is handy to trigger a new ETL 
pipeline run.

 `xx_achive/` can be used to store old datasets for future reference.


### How to update E2E test data

1. Create a Python virtual environment (make sure the Python version is compatible with the 
   `csr2transmart` version you want to use).


2. Install the desired version of `csr2transmart` from PyPI, e.g.:
   ```
   pip install csr2transmart==0.1.0
   ```
   Alternatively, install a development version from the `python_csr2transmart` repo by pointing 
   to a specific tag or branch, e.g.:
   ```
   pip install git+https://github.com/thehyve/python_csr2transmart.git@my-dev-branch
   ```


3. Create a new branch and start updating your test data and configuration files. 


4. Check that all of your new dataset versions can be parsed by `sources2csr` and 
   `csr2transmart` by running (e.g. from inside `current/`):
   ```
   sources2csr dropzone/full_dataset /tmp/csr_test config
   csr2transmart /tmp/csr_test /tmp/transmart_test config
   ```


5. Generate sha1sum files for all test data by moving to the root folder and running:
   ```
   python -m test_data_e2e.generate_sha1sum_files
   ```
   This automatically traverses the whole `test_data_e2e/current/dropzone` folder, no need to 
   provide the path.
