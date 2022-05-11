# OpenAlex-RAW
This is a python module to process the OpenAlex dataset from the snapshot raw files available from the [OpenAlex website](https://www.openalex.org).

## Installation
To use the package you need to have a python (`>=3.7`) environment installed in your system. The package can be installed via `pip` or by downloading the source code from this repository.

### Downloading the OpenAlex snapshot
If you did not already download the snapshot, you can follow the instalation instructions from the OpenAlex website in [Download OpenAlex Snapshot to your machine](https://docs.openalex.org/download-snapshot/download-to-your-machine). Here we provide a summary of the steps to download the dataset. Please, check the OpenAlex website for the most up to date instructions.

First, install the `aws cli` tool by following the instructions on the [AWS-cli website](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

Next, use the following command to download the snapshot:
```bash
aws s3 sync 's3://openalex' 'openalex-snapshot' --no-sign-request 
```

A new folder named `openalex-snapshot` will be created in your current working directory containing the dataset. Note that this process can take a long time as the dataset is over 300GB.

### Installing the OpenAlex-RAW package
The package can be installed via pip by running the following command:

```bash
pip install openalex-raw
```

All the required packages are installed automatically.

You can also download the source code from this repository and install it manually. This can be done using `git`:

```bash
git clone https://github.com/filipinascimento/openalex-raw
```

Next, you need to install the package using `pip` or `setup.py`:

```bash
pip install -e ./openalex-raw
```
or

```bash
cd openalex-raw
python setup.py install
```

## Usage RAW access
To go over all the entries of a certain type in the dataset, you can use the following code:

```python
from pathlib import Path

# tqdm is used to print a nice progress bar
# install it using `pip install tqdm`
from tqdm.auto import tqdm

import openalexraw as oaraw

# Path to the OpenAlex snapshot
openAlexPath = Path("<Location of the OpenAlex Snapshot>")

# Initializing the OpenAlex object with the OpenAlex snapshot path
oa = oaraw.OpenAlex(
    openAlexPath = openAlexPath
)

# Which entity to process
# "works" | "authors" | "institutions" | "venues" | "concepts"
entityType = "works"

# Getting the number of entries
entitiesCount = oa.getRawEntityCount(entityType)

# Iterating over all the entities of a certain type
for entity in tqdm(oa.rawEntities(entityType),total=entitiesCount):
    openAlexID = entity["id"]
    # do something with the entity
```

On fast storage, it may take a couple of hours to iterate over all the entities for `works` or `authors` types. For `institutions` and `venues`, and `concepts` types, it may take just a few minutes.


## Generating Schema and Report
Schemas and reports for each entity type can be found respectively in the folders `Schema` and `Reports` of this repository. Schema files are in machine-readable JSON format and contain all the fields and non-null counts, nested structures and lists are included. The reports show the number and percentage of the coverage of the fields in the dataset. Both Schema and Report files are named according to the OpenAlex entity type. Schema files also include the most common values (samples) for each field. Two schema files are provided: one with samples (e.g., `Schema/schema_works_samples.json`) and another without (e.g., `Schema/schema_works.json`).

To generate/update all the reports and schema, check the file `Examples/create_report.py`. Building the report can take a long time. You can use the provided schema files when generating `dbgz` archives.

## Coming soon
 - Random access based on the OpenAlex ID via `dbgz`.
 - Better documentation for Schema/Report generators.


## Full API documentation
The following is the documentation of the package's API.

### <kbd>class</kbd> `OpenAlex`
```python
    OpenAlex(
        openAlexPath,
        verbose = False
        ):
```
Class to access the OpenAlex data snapshots.
  * `openAlexPath` : `str` or `pathlib.Path`  
    The path to the OpenAlex directory. (default: current working directory)
  * `verbose` : `bool`  
    If True, print out more information. (default: False)

Returns 
  * `OpenAlex` object 
    The OpenAlex instance that can be used to access the dataset.


### <kbd>method</kbd> `getRawEntityCount`
```python
    OpenAlex.getRawEntityCount(entityType):
```
Get the number of raw entities of the given entity type.
  * `entityType` : `str` 
    Entity type can be `"authors"`, `"concepts"`, `"institutions"`, `"venues"` or `"works"`.

Returns 
  * `int` 
    The number of entities for the provided `entityType`.


### <kbd>method</kbd> `rawEntities`
```python
    OpenAlex.rawEntities(entityType):
```
Iterate over the entities of the selected type directly from the raw snapshot.
  * `entityType` : `str` 
    Entity type can be `"authors"`, `"concepts"`, `"institutions"`, `"venues"` or `"works"`.

Returns 
  * `iterable` 
    An iterable collection of entities of the provided `entityType`.

