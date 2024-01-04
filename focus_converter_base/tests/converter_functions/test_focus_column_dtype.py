from unittest import TestCase

from focus_converter.models.focus_column_names import (
    FocusColumnNames,
    get_dtype_for_focus_column_name,
)


class TestFocusColumnDtype(TestCase):
    def test_output_type_for_all_configured_columns(self):
        """
        Test that the output type for all configured columns is correct
        """

        for column_name in FocusColumnNames:
            dtype = get_dtype_for_focus_column_name(column_name)
            self.assertIsNotNone(dtype, f"Column {column_name} has no dtype")
