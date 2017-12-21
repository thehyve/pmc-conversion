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

To start the pipeline run point to the _scripts_ module and start :

``` bash
$ python -m luigi --module scripts DataLoader

```
