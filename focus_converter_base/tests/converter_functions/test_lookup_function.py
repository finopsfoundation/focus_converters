import os
import tempfile
from unittest import TestCase
from uuid import uuid4

import pandas as pd
import polars as pl
from jinja2 import Template
from pydantic import ValidationError

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.conversion_functions.lookup_function import LookupFunction
from focus_converter.converter import FocusConverter

VALUE_LOOKUP_SAMPLE_TEMPLATE_YAML_JINJA = """
plan_name: sample
priority: 1
column: {{ random_column_alias }}
conversion_type: lookup
focus_column: Region
conversion_args:
    reference_dataset_path: {{ test_reference_dataset_path }}
    source_value: {{ source_value }}
    destination_value: {{ destination_value }}
"""


VALUE_LOOKUP_SAMPLE_TEMPLATE_MISSING_VALUE_YAML = """
plan_name: sample
priority: 1
column: test_column
conversion_type: lookup
focus_column: Region
"""

VALUE_MAPPING_SAMPLE_TEMPLATE_YAML = Template(VALUE_LOOKUP_SAMPLE_TEMPLATE_YAML_JINJA)


# noinspection DuplicatedCode
class TestMappingFunction(TestCase):
    def test_map_not_defined(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sample_file_path = os.path.join(temp_dir, "D001_S001.yaml")

            with open(sample_file_path, "w") as fd:
                fd.write(VALUE_LOOKUP_SAMPLE_TEMPLATE_MISSING_VALUE_YAML)

            with self.assertRaises(ValidationError) as cm:
                ConversionPlan.load_yaml(sample_file_path)
            self.assertEqual(len(cm.exception.errors()), 1)
            self.assertEqual(cm.exception.errors()[0]["loc"], ("conversion_args",))

    def test_lookup_value_with_bad_reference_data_path(self):
        random_column_alias = str(uuid4())
        generated_yaml = VALUE_MAPPING_SAMPLE_TEMPLATE_YAML.render(
            random_column_alias=random_column_alias,
            test_reference_dataset_path="nonexistent_path",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            sample_file_path = os.path.join(temp_dir, "D001_S001.yaml")

            with open(sample_file_path, "w") as fd:
                fd.write(generated_yaml)

            with self.assertRaises(ValidationError) as cm:
                ConversionPlan.load_yaml(sample_file_path)
            self.assertEqual(len(cm.exception.errors()), 1)
            self.assertEqual(cm.exception.errors()[0]["loc"], ("conversion_args",))

    def test_lookup_value(self):
        random_column_alias = str(uuid4())
        random_focus_colum = str(uuid4())

        source_column_alias = str(uuid4())
        destination_column_alias = str(uuid4())

        random_mapping_df = pd.DataFrame(
            [
                {
                    source_column_alias: "1",
                    destination_column_alias: "1_mapped",
                    "ignore_column": "-",
                },
                {
                    source_column_alias: "2",
                    destination_column_alias: "2_mapped",
                    "ignore_column": "-",
                },
                {
                    source_column_alias: "3",
                    destination_column_alias: "3_mapped",
                    "ignore_column": "-",
                },
                {
                    source_column_alias: "4",
                    destination_column_alias: "4_mapped",
                    "ignore_column": "-",
                },
            ]
        )
        with tempfile.NamedTemporaryFile(suffix=".csv") as mapping_csv:
            random_mapping_df.to_csv(mapping_csv.name)

            generated_yaml = VALUE_MAPPING_SAMPLE_TEMPLATE_YAML.render(
                random_column_alias=random_column_alias,
                test_reference_dataset_path=mapping_csv.name,
                source_value=source_column_alias,
                destination_value=destination_column_alias,
            )

            df = pd.DataFrame(
                [
                    {"index_value": "1", random_column_alias: "1"},
                    {"index_value": "2", random_column_alias: "2"},
                    {"index_value": "3", random_column_alias: "3"},
                    {"index_value": "4", random_column_alias: "4"},
                    {"index_value": "5", random_column_alias: "5"},
                ]
            )
            pl_df = pl.from_dataframe(df).lazy()

            with tempfile.TemporaryDirectory() as temp_dir:
                sample_file_path = os.path.join(temp_dir, "D001_S001.yaml")

                with open(sample_file_path, "w") as fd:
                    fd.write(generated_yaml)

                conversion_plan = ConversionPlan.load_yaml(sample_file_path)
                conversion_lookup_args = LookupFunction.map_values_using_lookup(
                    plan=conversion_plan, column_alias=random_focus_colum
                )

                modified_pl_df = FocusConverter.__apply_lookup_reference_plans__(
                    lf=pl_df, lookup_args=[conversion_lookup_args]
                ).collect()
                self.assertIn(random_column_alias, modified_pl_df.columns)
                self.assertIn(random_focus_colum, modified_pl_df.columns)
                self.assertIn("index_value", modified_pl_df.columns)
                self.assertEqual(len(modified_pl_df.columns), 3)

                for index_value, _, mapped_value in modified_pl_df.iter_rows():
                    if index_value == "1":
                        self.assertEqual(mapped_value, "1_mapped")
                    elif index_value == "2":
                        self.assertEqual(mapped_value, "2_mapped")
                    elif index_value == "3":
                        self.assertEqual(mapped_value, "3_mapped")
                    elif index_value == "4":
                        self.assertEqual(mapped_value, "4_mapped")
                    elif index_value == "5":
                        self.assertIsNone(mapped_value)
                    else:
                        raise self.failureException(
                            f"Invalid value, map function not mapped, key: {index_value}, value: {mapped_value}"
                        )
