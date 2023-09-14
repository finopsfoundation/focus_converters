# focus_converter_base

This project impelemnts a set of common functions that can be used to convert provider specific
cost data into FOCUS specification cost dataset.

It implements a variety of transform functions that take advantage of [polars](https://github.com/pola-rs/polars)
lazy load API(s) allowing up to compute large datasets without fitting everything into memory.

Another important technology here is the pyarrow's dataset scanner that make it possible to read
data into smaller chunks that can be processed out of memory.

Also, the reference implementation uses an abstraction layer called fsspec, which abstracts
reading writing from object stores like S3, GCS etc.

## Prerequisites

- Python 3.8 or later
- Poetry

## Installation

### Step 1: Install Poetry

Poetry is a tool for dependency management and packaging in Python. To install Poetry, open your terminal and run the
following command:

```sh
curl -sSL https://install.python-poetry.org | python3 -
```

Refer to the [official Poetry documentation](https://python-poetry.org/docs/) for more installation options and details.

### Step 2: Clone the Project Repository

Clone the project repository to your local machine using the following command:

```sh
git clone https://github.com/finopsfoundation/focus_converters.git
```

### Step 3: Navigate to the Project Directory

Change your current directory to the project's directory:

```sh
cd focus_converter_base
```

### Step 4: Install Project Dependencies

Install the project's dependencies using Poetry:

```sh
poetry install --only main --no-root
```

or for installing all `dev` dependencies

```sh
poetry install --only main --no-root
```

## Setting Up the Development Environment

Once you have installed the necessary dependencies, you can set up the development environment using the following
steps:

### Step 1: Activate the Poetry Environment

Activate the Poetry virtual environment:

```sh
poetry shell
```

### Step 2: List available providers

```bash
python -m focus_converter.main list-providers
```

### Step 3: Generate test aws data

```bash
python -m tests.data_generators.main  --provider aws --num-rows 1000000 --destination-path samples/test_mil
```

### Step 4: Convert data

```bash
python -m focus_converter.main convert --provider aws --data-path samples/test_mil/ --data-format parquet --parquet-data-format dataset --export-path samples/output/
```

NOTE: Same can be done for other providers by changing provider from step 2.

## License

This project is licensed under the terms of the MIT license. See the LICENSE file for details.
