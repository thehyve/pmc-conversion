# PMC conversion

This conversion pipeline uses features of
- Python 3.6+
- luigi >= 2.7

Likely later:
- _pandas_ >= 0.21.0.
- tmtk >= 0.4.0

## Installation

``` bash
$ python -m pip install -r requirements.txt
```

## File system requirements

```
<data_root>
    │
    └───...
    │       ...
    │       ...
    │
    └───...
    │       ...
    │       ...
    │
    └───...
        │
        └───...
        │       ...
        │       ...
        │
        └───...
        │       ...
        │       ...
        ...
```

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
