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

### Checksum files: *.sha1
All data files (AccessDB, SAV, Pedigree) should have a _sha1_ file in the same 
directory. It is used for checking datafile health and whether the transformations 
should be run.

This file should have the data file name appended with ‘.sha1’. The sha1 
file should start with the 40 hex character long sha1 hash for the specific 
data file. It does not matter if there is any other information in this 
hash file (e.g. filename). 

## Usage

First make sure to copy luigi.cfg-sample and update it.

``` bash
$ cp luigi.cfg-sample luigi.cfg

```

Start luigi daemon, the pipeline workers will communicate with the daemon for work:

``` bash
$ luigid

```

To start the pipeline run point to the _scripts_ module and start :

``` bash
$ python -m luigi --module scripts StartAll --scheduler-url http://localhost:8082

```

The pipeline creates files with identifiers in several subdirectories. These files 
all start with `.done-*` and if you remove them the pipeline will rerun all tasks. E.g:

``` bash
$ find . -iname ".done-*" -exec rm {} +
```
