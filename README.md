# FOCUS Converter

The FOCUS Converter is a command-line utility to convert billing data files from popular public cloud providers,
such as **Amazon Web Services**, **Microsoft Azure**, **Google Cloud** and **Oracle Cloud**, into the common
schema known as FOCUS. You can read the specification at [FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec].

The converter is optimized for:

* Ability to act on the large files and wire formats provided by cloud providers.
* Comprehensibility of the conversion process which encodes _an_ understanding of the specification as-written.
* Best-effort conversion where the appropriate data for FOCUS does not exist in the provider's data file.
* Modularity so that new types of billing data can be supported.

## Currently Supported Cloud Providers

* [Amazon Web Services]
* [Google Cloud]
* [Microsoft Azure]
* [Oracle Cloud]

Want to add your own? See [CONTRIBUTING.md]

## Installation

The FOCUS converter supports Python 3.9 and above. If you meet these requirements, you can install with pip:

```sh
pip install focus_converter
```

After this, you will have a script called `focus-converter` in your path.

## Example Usage

```bash
focus-converter convert --provider aws --data-path path/to/aws/parquet/cur/ --data-format parquet --parquet-data-format dataset --export-path /tmp/output/
```

Use `focus-converter list-providers` to see the other providers that are supported.

## Development setup

1. Clone this repository.
2. [Install Poetry] if you don't have it.
3. [Install libmagic] if you don't have it.
4. Run the following shell snippet:

```sh
cd focus_converter_base/
poetry install --only main --no-root
```


Before using `python -m focus_converter.main` as a substitute for the pre-installed `focus-converter` script and testing repository changes, ensure to run the `poetry shell` command to set up the environment correctly.

## License

This project is licensed under the terms of the MIT license.

## Contributing

We're excited to work together. Please see [CONTRIBUTING.md] for information on how to get started.

[CONTRIBUTING.md]: CONTRIBUTING.md
[Install Poetry]: https://python-poetry.org/docs/#installation
[Install libmagic]: https://formulae.brew.sh/formula/libmagic
[FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec]: https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec
[Amazon Web Services]: https://github.com/finopsfoundation/focus_converters/tree/master/focus_converter_base/conversion_configs/aws
[Google Cloud]: https://github.com/finopsfoundation/focus_converters/tree/master/focus_converter_base/conversion_configs/gcp
[Microsoft Azure]: https://github.com/finopsfoundation/focus_converters/tree/master/focus_converter_base/conversion_configs/azure
[Oracle Cloud]: https://github.com/finopsfoundation/focus_converters/tree/master/focus_converter_base/conversion_configs/oci
