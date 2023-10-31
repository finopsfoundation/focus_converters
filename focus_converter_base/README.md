# focus_converter_base

This project impelemnts a set of common functions that can be used to convert provider specific
cost data into FOCUS specification cost dataset.

It implements a variety of transform functions that take advantage of [polars](https://github.com/pola-rs/polars)
lazy load API(s) allowing up to compute large datasets without fitting everything into memory.

Another important technology here is the pyarrow's dataset scanner that make it possible to read
data into smaller chunks that can be processed out of memory.

Also, the reference implementation uses an abstraction layer called fsspec, which abstracts
reading writing from object stores like S3, GCS etc.

## Installation

### With pip for modifying conversion plans|configs (recommended)

[Instructions for installing with pip.](docs/installation/with_pip.md)

### With poetry for modifying conversion functions

[Instructions for installing with poetry.](docs/installation/with_poetry.md)

## Usage

### Step 1: List available providers

```bash
python -m focus_converter.main list-providers
```

### Step 2: Generate test aws data

```bash
python -m tests.data_generators.main  --provider aws --num-rows 1000000 --destination-path samples/test_mil
```

### Step 3: Convert data

```bash
python -m focus_converter.main convert --provider aws --data-path samples/test_mil/ --data-format parquet --parquet-data-format dataset --export-path samples/output/
```

NOTE: Same can be done for other providers by changing provider from step 2.

## License

This project is licensed under the terms of the MIT license. See the LICENSE file for details.
