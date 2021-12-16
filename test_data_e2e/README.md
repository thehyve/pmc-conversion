# PMC E2E TESTDATA

A repository complete with utilities to help create CSR-compatible test datasets for E2E testing.


### How to use this repo

1. Update `requirements.txt` to point to the latest release of `python_csr2transmart` you would 
   like your test data to be compatible with (default `master` branch).


2. Create a Python virtual environment (check the latest Python version supported by 
   `python_csr2transmart`) and install the requirements:
    ```
      pip install -r requirements.txt
    ```


3. Create a new branch and start working on a new version of `test_data/`, and corresponding 
   changes to the configuration files in `config/`. We recommend using two sub-folders, one for the 
   complete dataset (e.g. `full_dataset/`) and one with an alternative version, obtained for 
   example by removing or swapping the gender of one patient (e.g. `alternative/`).


4. Check that the dataset can be parsed by `sources2csr` and `csr2transmart` by running:
   ```
   python validate_data.py <path_to_data>
   ```
   where the provided path should point to a specific test data subfolder (e.g. `full_dataset/`).
   The output will be written to the (git-ignored) `validation_results/` folder, should you need 
   to inspect it.


5. Generate sha1sum files for all test data by running:
   ```
   python generate_sha1sum.py
   ```
   This automatically traverses the whole `test_data/` folder, no need to provide the path.
