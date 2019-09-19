# PMC conversion

[![Build status](https://travis-ci.org/thehyve/pmc-conversion.svg?branch=master)](https://travis-ci.org/thehyve/pmc-conversion/branches)
[![codecov](https://codecov.io/gh/thehyve/pmc-conversion/branch/master/graph/badge.svg)](https://codecov.io/gh)
[![license](https://img.shields.io/github/license/thehyve/pmc-conversion.svg?style=flat-square)](https://github.com//thehyve/pmc-conversion/blob/master/LICENSE)

Data transformation and loading pipeline. It uses [Luigi](https://github.com/spotify/luigi) Python package for jobs handling 
and [python_csr2transmart](https://github.com/thehyve/python_csr2transmart) package for transformation of Central Subject Registry data. 

It loads data to [tranSMART](https://github.com/thehyve/transmart-core) platform using [transmart-copy](https://github.com/thehyve/transmart-core/tree/dev/transmart-copy) tool 
and to [cBioPortal](https://github.com/cBioPortal/cbioportal) using [cbioportalImporter.py](https://docs.cbioportal.org/5.1-data-loading/data-loading/data-loading-for-developers) script.

## Configuration

There are two types of configuration files: 
- pipeline configuration: [luigi.cfg](#luigi-configuration) and [email_config.cfg](#email-configuration) files to be located in the repository root,
- [transformation configuration](https://github.com/thehyve/python_csr2transmart#usage) files, 
located in `transformation_config_dir` (defined in [luigi.cfg](#luigi-configuration)).

### Luigi configuration

Luigi configuration can be created by coping the `luigi.cfg-sample`:

``` bash
cp luigi.cfg-sample luigi.cfg
```

Config options overview:

| Variable                  | Section                  | Default value                | Description                                                                                                                                                         |
|---------------------------|--------------------------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| logging_conf              | core                     | logging.cfg                  | Name of logging configuration file.                                                                                                                                 |
| db_connection             | task_history             | sqlite://pmc-luigi-db.sqlite | Database to store task history.                                                                                                                                     |
| record_taks_history       | scheduler                | True                         | Store task history True or False. Requires db_connection under task_history to be set if True                                                                       |
| state_path                | scheduler                | luigi-state.pickle           | Path to save a pickle file with the current state of the pipeline                                                                                                   |
| remove_delay              | scheduler                | 86400 (1 day)                | Set how long tasks should be remembered in task dashboard. Value in seconds.                                                                                        |
| drop_dir                  | GlobalConfig             | /home/drop/drop_zone         | Path to drop zone directory.                                                                                                                                        |
| data_repo_dir             | GlobalConfig             | /home/pmc/data               | Path to the git folder to store input and staging data.  By default the pipeline creates the input, staging, load logs, intermediate file dir inside the repo_root. |
| working_dir               | GlobalConfig             | /home/pmc/working_dir        | Directory used as working directory. Similar to /tmp.                                                                                                               |
| transformation_config_dir | GlobalConfig             | /home/pmc/config             | Directory with the configuration files required for transformation.                                                                                                 |
| load_logs_dir_name        | GlobalConfig             | load_logs                    | Directory name to store loading logs.                                                                                                                               |
| transmart_copy_jar        | GlobalConfig             | /home/pmc/libs               | Location to transmart-copy jar file to use for data loading to tranSMART.                                                                                           |
| study_id                  | GlobalConfig             | CSR_STUDY                    | Study ID of the study used in tranSMART.                                                                                                                            |
| top_node                  | GlobalConfig             | \Central Subject Registry\   | Name of the top ontology tree node to display in tranSMART.                                                                                                         |
| PGHOST                    | GlobalConfig             | localhost                    | tranSMART database host.                                                                                                                                            |
| PGPORT                    | GlobalConfig             | 5432                         | tranSMART database port.                                                                                                                                            |
| PGDATABASE                | GlobalConfig             | transmart                    | tranSMART database name.                                                                                                                                            |
| PGUSER                    | GlobalConfig             | tm_cz                        | User to use for loading data to tranSMART.                                                                                                                          |
| PGPASSWORD                | GlobalConfig             | tm_cz                        | User password.                                                                                                                                                      |
| transmart_loader          | resources                | 1                            | Amount of workers luigi has access to.                                                                                                                              |
| keycloak_url              | TransmartApiTask         |                              | URL to Keycloak instance used to get access to tranSMART, e.g. https://keycloak.example.com/auth/realms/transmart-dev                                               |
| transmart_url             | TransmartApiTask         |                              | URL to tranSMART API V2.                                                                                                                                            |
| gb_backend_url            | TransmartApiTask         |                              | URL to Glowing Bear Backend API.                                                                                                                                    |
| client_id                 | TransmartApiTask         |                              | Keycloak client ID.                                                                                                                                                 |
| offline_token             | TransmartApiTask         |                              | Offline token used to request an access token in order to communicate with Gb Backend and tranSMART REST APIs.                                                      |
| docker_image              | CbioportalDataValidation |                              | Name of docker image to use during cBioPortal data validation.                                                                                                      |
| docker_image              | CbioportalDataLoading    |                              | Name of docker image to use during cBioPortal data loading.                                                                                                         |
| server_name               | CbioportalDataLoading    |                              | Name of the the cBioPortal server.                                                                                                                                  |
| PGADMINUSER               | E2eTest                  | postgres                     | Test database user.                                                                                                                                                 |
| PGADMINPASSWORD           | E2eTest                  | password                     | Test database user password.                                                                                                                                        |
| parent_drop_dir           | E2eTest                  |                              | Test data dropzone directory.                                                                                                                                       |

#### Offline token

The application requires an offline token to exchange it for an access token to communicate with tranSMART and GB Backend.

Below is `curl` command to generate an offline token for `USERNAME` user.
To get the token the user needs to have the role mapping for the realm-level: `"offline_access"`.
Before using the command you have to substitute words in uppercase with proper ones.

```bash
    curl \
      -d 'client_id=CLIENT_ID' \
      -d 'username=USERNAME' \
      -d 'password=PASSWORD' \
      -d 'grant_type=password' \
      -d 'scope=offline_access' \
      'https://<KEYCLOAK_URL>/protocol/openid-connect/token'
```

The value of the `refresh_token` field in the response is the offline token.

### Email configuration

Email configuration can be created by coping the `email_config.cfg-sample`:

``` bash
cp email_config.cfg-sample email_config.cfg
```

Config options overview:


| Variable | Section | Default value               | Description                                                   |
|----------|---------|-----------------------------|---------------------------------------------------------------|
| log_file | global  | python.log                  | Logging file name.                                            |
| receiver | email   |                             | Email address of the receiver, can be a comma separated list. |
| sender   | email   |                             | Email address of the sender.                                  |
| prefix   | email   | [CSR Data Loading Pipeline] | Prefix for subject line of the error email.                   |
| port     | smtp    | 587                         | Port to use for sending emails.                               |
| username | smtp    |                             | Username for email client, when not needed can be left empty. |
| password | smtp    |                             | Password for email client.                                    |
| host     | smtp    | smtp.gmail.com              | Host of the email client.                                     |


## Input data

In the drop directory (`drop_dir` in luigi.cfg) the clinical data files should be provided as well as a folder called NGS with all omcs data. 
Each file in the drop directory has to be accompanied by a sha1 checksum file.

File naming convention:
- File name: `<filename>.<extension>`
- Sha1 file: `<filename>.<extension>.sha1`

E.g. data.txt has to have data.txt.sha1 next to it with sha1 hash of the data file. 
As sha1 hashes 40 characters long the rest of the file gets ignored:

`1625be750dab24057c4c82d62d27298236ebb04c diagnosis.txt`

For more information, see the [CSR data model](https://github.com/thehyve/python_csr2transmart#data-model) description 
and an example of [input data files](./test_data/dropzone/E2E_TEST_DATA).

## Usage

1. Make sure you have `luigi.cfg` and `email_config.cfg` properly configured (see [configuration section](#configuration)) 
and the input data is in the proper directory (see [input data section](#input-data)).

2. Install dependencies:

    Pipeline requires Python >= 3.6.

    ``` bash
    python -m pip install -r requirements/requirements.txt
    ```

3. Start luigi daemon:

    ``` bash
    luigid
    ```

4. Start the full pipeline:

    ``` bash
    ./scripts/run.sh
    ```



### Other available scripts

To load data to transmart only:

``` bash
./scripts/load_transmart_data.sh
```

To load data to cbioportal only:

``` bash
./scripts/load_cbioportal_data.sh
```

To load data to both systems:

``` bash
./scripts/load_data.sh
```

The pipeline creates files that start with `.done-*`.
These files created for each successfully finished task of the pipeline.
To force execution of tasks again you need to remove these files:
``` bash
 ./scripts/remove_done_files.sh
```


## Test

To run e2e tests:
``` bash
./scripts/e2e_transmart_only.sh
```

To run other tests:
``` bash
./scripts/run_tests.sh
```

## License

Copyright (c) 2018, 2019 The Hyve B.V.

The PMC conversion pipeline is licensed under the MIT License. See the file [LICENSE](LICENSE).
