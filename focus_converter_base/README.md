# focus_converter_base

This project impelemnts a set of common functions that can be used to convert provider specific
cost data into FOCUS specification cost dataset.

It implements a variety of transform functions that take advantage of [polars](https://github.com/pola-rs/polars)
lazy load API(s) allowing up to compute large datasets without fitting everything into memory.

Another important technology here is the pyarrow's dataset scanner that make it possible to read
data into smaller chunks that can be processed out of memory.

Also, the reference implementation uses an abstraction layer called fsspec, which abstracts
reading writing from object stores like S3, GCS etc.

## Installation, etc.

For general installation instructions and development guidance, please see the parent [README.md].

[README.md]: https://github.com/finopsfoundation/focus-spec-converter/tree/master/README.md