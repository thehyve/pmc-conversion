# PMC conversion

This conversion pipeline requires Python 3.6.

## Installation

``` bash
$ python -m pip install -r requirements/requirements.txt
```

## Keycloak settings

### Identity server

Keycloak settings have to be set in luigi.cfg file (TransmartApiTask section), so the pipeline can access GB Backend 
and tranSMART REST APIs.

| key | description |
|-----------|-------------|
| `keycloak_url` | keycloak url that used. e.g. `https://keycloak.example.com/auth/realms/transmart-dev` |
| `client_id` | keycloak client id. |
| `offline_token` | keycloak offline token. |


### Offline token

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

## Usage

First make sure to copy luigi.cfg-sample and update it.

``` bash
$ cp luigi.cfg-sample luigi.cfg

```

Start luigi daemon, the pipeline workers will communicate with the daemon for work:

``` bash
$ luigid

```

To start the full pipeline:

``` bash
$ ./run.sh

```

## Fixing manually when something went wrong

When pipeline fails on loading or you want to load data from the history you need to run the data load step only.

To load data to transmart only:

``` bash
$ ./load_transmart_data.sh

```

To load data to cbioportal only:

``` bash
$ ./load_cbioportal_data.sh

```

To load data to both systems:

``` bash
$ ./load_data.sh

```

The pipeline creates files that start with `.done-*`.
These files created for each successfully finished task of the pipeline.
To force execution of tasks again you need to remove these files:
``` bash
$ ./remove_done_files.sh
```

That's what happens as part of the script when you run load data task only.

The typical workflow to load version from history might look like this:
``` bash
$ cd <data repo>
$ git checkout <hash of the commit which data versioin we want to load>
$ cd <this directory, with the bash scripts>
$ ./load_data.sh
$ cd <data repo>
# to clean repository from data loading logs
$ git checkout .
# return back recent version
$ git checkout master
```

Please note that above example won't leave any sign in the git history of the fact of data load.


## License

Copyright (c) 2018, 2019 The Hyve B.V.

The PMC conversion pipeline is licensed under the MIT License. See the file `<LICENSE>`_.
