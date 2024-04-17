import os
import tempfile
from typing import Optional, Type
from uuid import uuid4

import pandas as pd
import polars as pl
import pyarrow.dataset as ds
import pytest
import yaml
from pydantic import BaseModel, ValidationError

from focus_converter.configs.base_config import ConversionPlan
from focus_converter.converter import FocusConverter
from focus_converter.data_loaders.data_loader import DataFormats, ParquetDataFormat
from focus_converter.models import focus_conversion, BaseFOCUSConverterPlugin
from focus_converter.models.focus_column_names import FocusColumnNames
from focus_converter.utils.export_conversion_rules import (
    generate_conversion_rules_per_provider,
    ReportFormats,
)


@focus_conversion(plan_name="test-plan-universal")
class CustomConversionPlugin(BaseFOCUSConverterPlugin):
    @classmethod
    def get_arguments_model(cls) -> Optional[Type[BaseModel]]:
        class CustomModel(BaseModel):
            arg1: str

        return CustomModel

    @classmethod
    def conversion_plan_hook(cls, plan, column_validator) -> pl.Expr:
        return pl.col(plan.column).str.replace("_", "Provider")


SAMPLE_PLAN_PER_PROVIDER = ConversionPlan(
    column="test_column",
    config_file_name="D001_S001.yaml",
    plan_name="test-plan",
    dimension_id=1,
    priority=0,
    conversion_type="test-plan-universal",
    focus_column=FocusColumnNames.PROVIDER,
    conversion_args={"arg1": "test"},
)


@pytest.fixture(scope="class")
def sample_focus_converter_custom_plugin():
    focus_converter = FocusConverter(column_prefix=None)
    with tempfile.TemporaryDirectory() as temp_dir:
        os.mkdir(f"{temp_dir}/test-provider")

        config_path = f"{temp_dir}/test-provider/test_column_S001.yaml"
        with open(config_path, "w") as f:
            yaml.dump(SAMPLE_PLAN_PER_PROVIDER.model_dump(mode="json"), f)

        focus_converter.load_provider_conversion_configs(
            conversion_configs_path=temp_dir
        )
        focus_converter.prepare_horizontal_conversion_plan(provider="test-provider")
    return focus_converter


class TestBaseConversionPlugin:
    def test_conversion_config_validation(self):
        focus_converter = FocusConverter(column_prefix=None)
        with tempfile.TemporaryDirectory() as temp_dir:
            os.mkdir(f"{temp_dir}/test-provider")

            config_path = f"{temp_dir}/test-provider/{uuid4()}_S001.yaml"
            with open(config_path, "w") as f:
                obj = SAMPLE_PLAN_PER_PROVIDER.model_dump(mode="json")
                obj.pop("conversion_args")
                yaml.dump(obj, f)

            with pytest.raises(ValidationError) as cm:
                focus_converter.load_provider_conversion_configs(
                    conversion_configs_path=temp_dir
                )
            assert (
                "Input should be a valid dictionary or instance of CustomModel"
                in str(cm.value)
            )

    def test_base_conversion_plugin(
        self, sample_focus_converter_custom_plugin: FocusConverter
    ):
        df = pd.DataFrame(
            [
                {"a": 1, "test_column": "_ 2"},
                {"a": 1, "test_column": "_ 3"},
                {"a": 1, "test_column": "_ 4"},
                {"a": 1, "test_column": "_ 5"},
                {"a": 1, "test_column": None},
            ]
        )
        pl_df = pl.from_dataframe(df).lazy()
        assert pl_df.dtypes[1] == pl.Utf8

        with tempfile.TemporaryDirectory() as temp_dir:
            input_file = f"{temp_dir}/test.parquet"
            output_dir = f"{temp_dir}/test_output.parquet"
            df.to_parquet(input_file)

            sample_focus_converter_custom_plugin.load_data(
                data_path=input_file,
                data_format=DataFormats.PARQUET,
                parquet_data_format=ParquetDataFormat.FILE,
            )
            sample_focus_converter_custom_plugin.configure_data_export(
                export_path=output_dir, export_include_source_columns=True
            )
            sample_focus_converter_custom_plugin.convert()

            dataset = ds.dataset(output_dir)
            modified_pl_df = pl.scan_pyarrow_dataset(dataset).collect()
            assert list(modified_pl_df["Provider"]) == [
                "Provider 2",
                "Provider 3",
                "Provider 4",
                "Provider 5",
                None,
            ]

    def test_graph_uml_generator(
        self, sample_focus_converter_custom_plugin: FocusConverter
    ):
        uml = (
            sample_focus_converter_custom_plugin.__column_validator__.generate_mermaid_uml()
        )
        assert (
            uml
            == "graph LR;\n\tSOURCE --> test_column\n\ttest_column -- test-plan-universal:test_column_S001.yaml --> Provider\n\tProvider --> FOCUS_DATASET\n"
        )

    def test_export_rule_conversion(
        self, sample_focus_converter_custom_plugin: FocusConverter
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            generate_conversion_rules_per_provider(
                converter=sample_focus_converter_custom_plugin,
                provider="test-provider",
                plans=[SAMPLE_PLAN_PER_PROVIDER],
                output_dir=temp_dir,
                output_format=ReportFormats.CSV,
            )

            with open(f"{temp_dir}/test-provider.csv") as f:
                content = f.read()
                assert "test-plan-universal" in content

            assert (
                "Provider,1,test_column,Not Defined,test-plan-universal,{'arg1': 'test'}"
                in content
            )
