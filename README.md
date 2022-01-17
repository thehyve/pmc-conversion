# PMC conversion

[![Build status](https://travis-ci.org/thehyve/pmc-conversion.svg?branch=master)](https://travis-ci.org/thehyve/pmc-conversion/branches)
[![codecov](https://codecov.io/gh/thehyve/pmc-conversion/branch/master/graph/badge.svg)](https://codecov.io/gh)
[![license](https://img.shields.io/github/license/thehyve/pmc-conversion.svg?style=flat-square)](https://github.com//thehyve/pmc-conversion/blob/master/LICENSE)

Data transformation and loading pipeline. It uses [Luigi](https://github.com/spotify/luigi) Python package for jobs handling 
and [python_csr2transmart](https://github.com/thehyve/python_csr2transmart) package for transformation of Central Subject Registry data. 

It loads data to [tranSMART](https://github.com/thehyve/transmart-core) platform using [transmart-copy](https://github.com/thehyve/transmart-core/tree/dev/transmart-copy) tool.

For a production deployment instructions, start with the [deployment](#deployment) section.

## Configuration

There are two types of configuration files: 
- pipeline configuration: [luigi.cfg](#luigi-configuration) and [email_config.cfg](#email-configuration) files to be located in the repository root,
- [transformation configuration](#transformation-configuration) files, 
located in `transformation_config_dir` (defined in [luigi.cfg](#luigi-configuration)).

### Luigi configuration

Luigi configuration can be created by coping the `luigi.cfg-sample`:

``` bash
cp luigi.cfg-sample luigi.cfg
```

Config options overview:

| Variable                  | Section                  | Default value                | Description                                                                                                                                                         |
|---------------------------|--------------------------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| logging_conf_file         | core                     | logging.cfg                  | Name of logging configuration file.                                                                                                                                 |
| db_connection             | task_history             | sqlite://pmc-luigi-db.sqlite | Database to store task history.                                                                                                                                     |
| record_taks_history       | scheduler                | True                         | Store task history True or False. Requires db_connection under task_history to be set if True                                                                       |
| state_path                | scheduler                | luigi-state.pickle           | Path to save a pickle file with the current state of the pipeline                                                                                                   |
| remove_delay              | scheduler                | 86400 (1 day)                | Set how long tasks should be remembered in task dashboard. Value in seconds.                                                                                        |
| drop_dir                  | GlobalConfig             | /home/drop/drop_zone         | Path to drop zone directory.                                                                                                                                        |
| data_repo_dir             | GlobalConfig             | /home/pmc/data               | Path to the git folder to store input and staging data.  By default the pipeline creates the input, staging, load logs, intermediate file dir inside the repo_root. |
| working_dir               | GlobalConfig             | /home/pmc/working_dir        | Directory used as working directory. Similar to /tmp.                                                                                                               |
| transformation_config_dir | GlobalConfig             | /home/pmc/config             | Directory with the configuration files required for transformation.                                                                                                 |
| load_logs_dir_name        | GlobalConfig             | load_logs                    | Directory name to store loading logs.                                                                                                                               |
| transmart_copy_jar        | GlobalConfig             | /home/pmc/libs/transmart-copy.jar | Location to transmart-copy jar file to use for data loading to tranSMART.                                                                                           |
| study_id                  | GlobalConfig             | CSR_STUDY                    | Study ID of the study used in tranSMART.                                                                                                                            |
| top_node                  | GlobalConfig             | \Central Subject Registry\   | Name of the top ontology tree node to display in tranSMART.                                                                                                         |
| PGHOST                    | GlobalConfig             | localhost                    | tranSMART database host.                                                                                                                                            |
| PGPORT                    | GlobalConfig             | 5432                         | tranSMART database port.                                                                                                                                            |
| PGDATABASE                | GlobalConfig             | transmart                    | tranSMART database name.                                                                                                                                            |
| PGUSER                    | GlobalConfig             | biomart_user                 | User to use for loading data to tranSMART.                                                                                                                          |
| PGPASSWORD                | GlobalConfig             | biomart_user                 | User password.                                                                                                                                                      |
| transmart_loader          | resources                | 1                            | Amount of workers luigi has access to.                                                                                                                              |
| keycloak_url              | TransmartApiTask         | https://keycloak.example.com/realms/example | URL to Keycloak instance used to get access to tranSMART, e.g. https://keycloak.example.com/realms/transmart-dev                                               |
| transmart_url             | TransmartApiTask         | http://localhost:8081        | URL to tranSMART API V2.                                                                                                                                            |
| gb_backend_url            | TransmartApiTask         | http://localhost:8083        | URL to Glowing Bear Backend API.                                                                                                                                    |
| client_id                 | TransmartApiTask         | transmart-client             | Keycloak client ID.                                                                                                                                                 |
| offline_token             | TransmartApiTask         |                              | Offline token used to request an access token in order to communicate with Gb Backend and tranSMART REST APIs.                                                      | |

#### Offline token

The application requires an offline token to exchange it for an access token to communicate with tranSMART and GB Backend.
To get the token a user needs to have the role mapping for the realm-level: `"offline_access"`.

To create such a user in Keycloak:
- Login to Keycloak admin console.
- Select the proper realm, e.g. `example`.
- Go to `Users`.
- Click `Add user`, enter username `pmc-pipeline` and click `Save`.
- Select the `Credentials` tab, enter a strong password and click `Reset Password`.
- Go to `Role Mappings` tab.
    - Ensure that the `offline_access` realm role is assigned.
    - Select `transmart-client` in the `Client Roles` dropdown and assure the `ROLE_ADMIN` role is assigned.

Below is `curl` command to generate an offline token for `pmc-pipeline` user.

```bash
KEYCLOAK_CLIENT_ID=transmart-client
USERNAME=pmc-pipeline
PASSWORD=choose-a-strong-system-password  # CHANGE ME
KEYCLOAK_SERVER_URL=https://keycloak.example.com # CHANGE ME
KEYCLOAK_REALM=example # CHANGE ME

curl -f --no-progress-meter \
  -d "client_id=${KEYCLOAK_CLIENT_ID}" \
  -d "username=${USERNAME}" \
  -d "password=${PASSWORD}" \
  -d 'grant_type=password' \
  -d 'scope=offline_access' \
  "${KEYCLOAK_SERVER_URL}/realms/${KEYCLOAK_REALM}/protocol/openid-connect/token" | jq -r '.refresh_token'
```
The value of the `refresh_token` field in the response is the offline token.

### Email configuration

Email configuration can be created by coping the `email_config.cfg-sample`:

``` bash
cp email_config.cfg-sample email_config.cfg
```

Config options overview:


| Variable | Section | Default value                  | Description                                                   |
|----------|---------|--------------------------------|---------------------------------------------------------------|
| log_file | global  | /home/pmc/pmc-conversion/python.log | Logging file name.                                       |
| receiver | email   |                                | Email address of the receiver, can be a comma separated list. |
| sender   | email   | pmc-notifications@example.com  | Email address of the sender.                                  |
| prefix   | email   | [CSR Data Loading Pipeline]    | Prefix for subject line of the error email.                   |
| port     | smtp    | 587                            | Port to use for sending emails.                               |
| username | smtp    | pmc-notifications@example.com  | Username for email client, when not needed can be left empty. |
| password | smtp    |                                | Password for email client.                                    |
| host     | smtp    | smtp.gmail.com                 | Host of the email client.                                     |

### Transformation configuration

Configuration files for TranSMART must be placed in `transformation_config_dir`. 
Specifically, `sources_config.json` and `ontology_config.json`, described in [python_csr2transmart](https://github.com/thehyve/python_csr2transmart#usage).

The files reference the input data and need to be customized accordingly.
 
Sample configuration files are provided in [test_data/test_data_NGS/config](https://github.com/thehyve/pmc-conversion/tree/master/test_data/test_data_NGS/config).

## Input data

In the drop directory (`drop_dir` in luigi.cfg) the clinical data files should be provided as well as a folder called NGS with all omics data. 
Each file in the drop directory has to be accompanied by a sha1 checksum file.

File naming convention:
- File name: `<filename>.<extension>`
- Sha1 file: `<filename>.<extension>.sha1`

E.g. data.txt has to have data.txt.sha1 next to it with sha1 hash of the data file. 
As sha1 hashes 40 characters long the rest of the file gets ignored:

`1625be750dab24057c4c82d62d27298236ebb04c diagnosis.txt`

For more information, see the [CSR data model](https://github.com/thehyve/python_csr2transmart#data-model) description 
and an example of [input data files](test_data_e2e/current/dropzone).

## Usage

1. Make sure you have `luigi.cfg` and `email_config.cfg` properly configured (see [configuration section](#configuration)) 
and the input data is in the proper directory (see [input data section](#input-data)).

2. Install dependencies:

    Pipeline requires Python >= 3.7.

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

### Pipeline tasks overview

When starting the full pipeline, it executes the following tasks:

1. Checks if new input data was provided.
   Files from the ``drop_dir`` get shasum calculated and checked with provided shasum.
   If the shasum is correct, it synchronizes drop zone with the input data directory.
   Else, it return an error with the file that has an incorrect shasum.
   The new input data files are backed-up using git repository.

2. Reads from source files and produces tab delimited CSR files.

3. Reads CSR files and transforms the data to the TranSMART data model,
   creating files that can be imported to TranSMART using transmart-copy.
   The files are added to the git repository.

4. Loads the files using transmart-copy. It tries to delete the existing data
   and load the new staging files. If it fails, nothing happens to the existing data in the database.

5. Calls after_data_loading_update tranSMART API call to clear and rebuild the application cache.
   tranSMART loading log is committed using git.

6. In case not all the tasks are completed successfully, an email will be sent to the configured receivers,
   containing the full error report.

### Other available scripts

To load data to TranSMART:

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


### E2e tests

The `e2e_transmart_only` test will run all the pipeline tasks.
When running the test, data from `drop_dir` directory configured in `luigi.cfg`
will be transformed and loaded to the currently configured tranSMART database.
This will also trigger the after_data_loading_update tranSMART API call.

NOTE! Do not run this on production.


To run the e2e test:
``` bash
./scripts/e2e_transmart_only.sh
```

### Other tests

To run other tests:
``` bash
./scripts/run_tests.sh
```

## Deployment

Instructions on how to set up the pipeline on a production environment.

### Dependencies

- Python >= 3.7,
- Package `python3.7-venv` (or higher version, depending on the version of Python) installed, 
- Git,
- An SMTP server, listening on port 25.

### Installation steps

#### Create required users

Create users `pmc` and `drop` with home directories:

```shell
sudo useradd -m -s /bin/bash pmc
sudo useradd -m -s /bin/bash drop
```

Add `pmc` user to `drop` user group:
```shell
sudo usermod -a -G drop pmc
```

If there is a list of users who should be able to log in as `drop` and/or `pmc` user through SSH,
add /.ssh directories inside the newly created home directories of these users and put the list of SSH-RSA keys
inside `authorized_keys` files (`/home/pmc/.ssh/authorized_keys` and `/home/drop/.ssh/authorized_keys`).

#### Create required directories

The following directories should be created for `pmc` user:
- /home/pmc/data
- /home/pmc/working_dir
- /home/pmc/config
- /home/pmc/libs

```shell
sudo -iu pmc
cd /home/pmc
mkdir data working_dir config libs
```

and for `drop` user:
- /home/drop/drop_zone

```shell
sudo -iu drop
cd /home/drop
mkdir drop_zone
```

The `drop_zone` directory is where the pipeline checks for new data by default. It can be configured to be
a symbolic link to the actual data directory, where the source data is delivered.

To create a symlink, run:

```shell
ln -sfn /home/drop/sample_test_data_folder/sample_dataset /home/drop/drop_zone
```

#### Prepare the repositories

Clone the indicated pipeline repository tag into the `pmc` user home directory.

```shell
sudo -iu pmc
cd /home/pmc/
git clone https://github.com/thehyve/pmc-conversion.git --branch <tag_name> --single-branch
```

Initialize git repositories inside `/home/pmc/data` and `/home/pmc/config` directories:
```shell
cd /home/pmc/data
git init
cd /home/pmc/config
git init
```

#### Prepare external tools

Download the latest version of the transmart-copy.jar from the Nexus repository of The Hyve to `/home/pmc/libs:
```shell
curl -f -L https://repo.thehyve.nl/service/local/repositories/releases/content/org/transmartproject/transmart-copy/17.2.8/transmart-copy-17.2.8.jar -o /home/pmc/libs/transmart-copy.jar
```

Then put the transmart-copy.jar inside `/home/pmc/libs`.

#### Prepare the configuration

Prepare the [luigi.cfg](#luigi-configuration) and [email_config.cfg](#email-configuration)
and put them into the `/home/pmc/pmc-conversion/` directory.


#### Create a Python3 virtualenv

```shell
sudo -iu pmc
cd /home/pmc
python3 -m venv venv
source venv/bin/activate
pip install -r /home/pmc/pmc-conversion/requirements/requirements.txt
```

#### Configure a Luigi daemon service

Add a new systemd service:
```shell
sudo vi /etc/systemd/system/luigi.service
```

and add the following content:
```
[Unit]
Description=PMC Luigi daemon service

[Service]
ExecStart=/home/pmc/venv/bin/luigid
User=pmc
WorkingDirectory=/home/pmc/pmc-conversion
Restart=always

[Install]
WantedBy=multi-user.target
```
Save and close the file.

Then start the service:
```shell
sudo systemctl start luigi
```

and automatically get it to start on boot:
```shell
sudo systemctl enable luigi
```

### Test the pipeline

Test if the pipeline works correctly by manually triggering the data upload as a `pmc` user:

```shell
sudo -iu pmc
source /home/pmc/venv/bin/activate && cd /home/pmc/pmc-conversion && /home/pmc/pmc-conversion/scripts/run.sh
```

#### Create a cron job

If the pipeline should be run periodically, e.g. daily at 2:02, install a cron job for the `pmc` user as follows:

```shell
sudo -iu pmc
crontab -e
```
Then add an entry for running the pipeline as follows:
```
# Example of job definition:
# .---------------- minute (0 - 59)
# |  .------------- hour (0 - 23)
# |  |  .---------- day of month (1 - 31)
# |  |  |  .------- month (1 - 12) OR jan,feb,mar,apr ...
# |  |  |  |  .---- day of week (0 - 6) (Sunday=0 or 7) OR sun,mon,tue,wed,thu,fri,sat
# |  |  |  |  |
# *  *  *  *  * user-name  command to be executed

2 2 * * * . /home/pmc/venv/bin/activate && cd /home/pmc/pmc-conversion && /home/pmc/pmc-conversion/scripts/run.sh
```

Save and close the file.

#### Grant sudo access to the pmc user for restarting TranSMART

As a sudo user run:
```bash
vi /etc/sudoers.d/pmc_restart_transmart
```

and add the following content:
```
pmc ALL=(ALL) NOPASSWD: /bin/systemctl restart transmart-server.service
```

Save and close the file.

## License

Copyright (c) 2018, 2019 The Hyve B.V.

The PMC conversion pipeline is licensed under the MIT License. See the file [LICENSE](LICENSE).
