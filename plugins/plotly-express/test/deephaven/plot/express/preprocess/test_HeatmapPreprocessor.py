import unittest

import numpy as np
import pandas as pd

from ..BaseTest import BaseTestCase, remap_types


class HeatmapPreprocessorTestCase(BaseTestCase):
    def setUp(self) -> None:
        from deephaven import new_table, merge
        from deephaven.column import int_col

        self.source = new_table(
            [
                int_col("X", [0, 4, 0, 4]),
                int_col("Y", [0, 0, 4, 4]),
                int_col("Z", [1, 1, 2, 2]),
            ]
        )

        # Variance and standard deviation require at least two rows to be non-null, so stack them
        # for a resulting var and std of 0 in all grid cells
        self.var_source = merge([self.source, self.source])

    def tables_equal(self, args, expected_df, t=None, post_process=None) -> None:
        """
        Compare the expected dataframe to the actual dataframe generated by the preprocessor

        Args:
            args: The arguments to pass to the preprocessor
            expected_df: The expected dataframe
            t: The table to preprocess, defaults to self.source
        """
        from src.deephaven.plot.express.preprocess.HeatmapPreprocessor import (
            HeatmapPreprocessor,
        )
        import deephaven.pandas as dhpd

        if t is None:
            t = self.source

        args_copy = args.copy()

        heatmap_preprocessor = HeatmapPreprocessor(args_copy)

        new_table_gen = heatmap_preprocessor.preprocess_partitioned_tables([t])
        new_table, _ = next(new_table_gen)

        new_df = dhpd.to_pandas(new_table)

        if post_process is not None:
            post_process(new_df)

        self.assertTrue(expected_df.equals(new_df))

    def test_basic_preprocessor(self):
        args = {
            "x": "X",
            "y": "Y",
            "z": None,
            "histfunc": "count",
            "nbinsx": 2,
            "nbinsy": 2,
            "range_bins_x": None,
            "range_bins_y": None,
            "empty_bin_default": None,
            "table": self.source,
        }

        expected_df = pd.DataFrame(
            {
                "X": [1.0, 1.0, 3.0, 3.0],
                "Y": [1.0, 3.0, 1.0, 3.0],
                "count": [1, 1, 1, 1],
            }
        )
        remap_types(expected_df)

        self.tables_equal(args, expected_df)

    def test_basic_preprocessor_z(self):
        args = {
            "x": "X",
            "y": "Y",
            "z": "Z",
            "histfunc": "sum",
            "nbinsx": 2,
            "nbinsy": 2,
            "range_bins_x": None,
            "range_bins_y": None,
            "empty_bin_default": None,
            "table": self.source,
        }

        expected_df = pd.DataFrame(
            {"X": [1.0, 1.0, 3.0, 3.0], "Y": [1.0, 3.0, 1.0, 3.0], "sum": [1, 2, 1, 2]}
        )
        remap_types(expected_df)

        self.tables_equal(args, expected_df)

    def test_partial_range_preprocessor(self):
        args = {
            "x": "X",
            "y": "Y",
            "z": None,
            "histfunc": "count",
            "nbinsx": 2,
            "nbinsy": 2,
            "range_bins_x": [None, 6],
            "range_bins_y": None,
            "empty_bin_default": None,
            "table": self.source,
        }

        expected_df = pd.DataFrame(
            {
                "X": [1.5, 1.5, 4.5, 4.5],
                "Y": [1.0, 3.0, 1.0, 3.0],
                "count": [1, 1, 1, 1],
            }
        )
        remap_types(expected_df)

        self.tables_equal(args, expected_df)

    def test_full_preprocessor(self):
        args = {
            "x": "X",
            "y": "Y",
            "z": None,
            "histfunc": "count",
            "nbinsx": 2,
            "nbinsy": 3,
            "range_bins_x": [1, 5],
            "range_bins_y": [-1, 5],
            "empty_bin_default": None,
            "table": self.source,
        }

        expected_df = pd.DataFrame(
            {
                "X": [2.0, 2.0, 2.0, 4.0, 4.0, 4.0],
                "Y": [0.0, 2.0, 4.0, 0.0, 2.0, 4.0],
                "count": [0, 0, 0, 1, 0, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count"] = expected_df["count"].astype("Int64")

        self.tables_equal(args, expected_df)

    def test_full_preprocessor_z(self):
        args = {
            "x": "X",
            "y": "Y",
            "z": "Z",
            "histfunc": "avg",
            "nbinsx": 3,
            "nbinsy": 2,
            "range_bins_x": [-1, 5],
            "range_bins_y": [1, 5],
            "empty_bin_default": None,
            "table": self.source,
        }

        expected_df = pd.DataFrame(
            {
                "X": [0.0, 0.0, 2.0, 2.0, 4.0, 4.0],
                "Y": [2.0, 4.0, 2.0, 4.0, 2.0, 4.0],
                "avg": [pd.NA, 2.0, pd.NA, pd.NA, pd.NA, 2.0],
            }
        )
        remap_types(expected_df)
        expected_df["avg"] = expected_df["avg"].astype("Float64")

        self.tables_equal(args, expected_df)

    def test_preprocessor_aggs(self):
        args = {
            "x": "X",
            "y": "Y",
            "z": "Z",
            "histfunc": "abs_sum",
            "nbinsx": 2,
            "nbinsy": 2,
            "range_bins_x": None,
            "range_bins_y": None,
            "empty_bin_default": None,
            "table": self.source,
        }

        expected_df = pd.DataFrame(
            {
                "X": [1.0, 1.0, 3.0, 3.0],
                "Y": [1.0, 3.0, 1.0, 3.0],
                "abs_sum": [1, 2, 1, 2],
            }
        )
        remap_types(expected_df)
        expected_df["abs_sum"] = expected_df["abs_sum"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "avg"

        expected_df = pd.DataFrame(
            {"X": [1.0, 1.0, 3.0, 3.0], "Y": [1.0, 3.0, 1.0, 3.0], "avg": [1, 2, 1, 2]}
        )
        remap_types(expected_df)
        expected_df["avg"] = expected_df["avg"].astype("Float64")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "count"

        expected_df = pd.DataFrame(
            {
                "X": [1.0, 1.0, 3.0, 3.0],
                "Y": [1.0, 3.0, 1.0, 3.0],
                "count": [1, 1, 1, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count"] = expected_df["count"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "count_distinct"

        expected_df = pd.DataFrame(
            {
                "X": [1.0, 1.0, 3.0, 3.0],
                "Y": [1.0, 3.0, 1.0, 3.0],
                "count_distinct": [1, 1, 1, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count_distinct"] = expected_df["count_distinct"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "max"

        expected_df = pd.DataFrame(
            {"X": [1.0, 1.0, 3.0, 3.0], "Y": [1.0, 3.0, 1.0, 3.0], "max": [1, 2, 1, 2]}
        )
        remap_types(expected_df)
        expected_df["max"] = expected_df["max"].astype("Int32")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "median"

        expected_df = pd.DataFrame(
            {
                "X": [1.0, 1.0, 3.0, 3.0],
                "Y": [1.0, 3.0, 1.0, 3.0],
                "median": [1, 2, 1, 2],
            }
        )
        remap_types(expected_df)
        expected_df["median"] = expected_df["median"].astype("Float64")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "min"

        expected_df = pd.DataFrame(
            {"X": [1.0, 1.0, 3.0, 3.0], "Y": [1.0, 3.0, 1.0, 3.0], "min": [1, 2, 1, 2]}
        )
        remap_types(expected_df)
        expected_df["min"] = expected_df["min"].astype("Int32")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "std"

        expected_df = pd.DataFrame(
            {"X": [1.0, 1.0, 3.0, 3.0], "Y": [1.0, 3.0, 1.0, 3.0], "std": [0, 0, 0, 0]}
        )
        remap_types(expected_df)
        expected_df["std"] = expected_df["std"].astype("Float64")

        self.tables_equal(args, expected_df, self.var_source)

        args["histfunc"] = "sum"

        expected_df = pd.DataFrame(
            {"X": [1.0, 1.0, 3.0, 3.0], "Y": [1.0, 3.0, 1.0, 3.0], "sum": [1, 2, 1, 2]}
        )
        remap_types(expected_df)
        expected_df["sum"] = expected_df["sum"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "var"

        expected_df = pd.DataFrame(
            {"X": [1.0, 1.0, 3.0, 3.0], "Y": [1.0, 3.0, 1.0, 3.0], "var": [0, 0, 0, 0]}
        )
        remap_types(expected_df)
        expected_df["var"] = expected_df["var"].astype("Float64")

        self.tables_equal(args, expected_df, self.var_source)

    def test_histfunc_z_mismatch(self):
        from src.deephaven.plot.express.preprocess.HeatmapPreprocessor import (
            HeatmapPreprocessor,
        )

        args = {
            "x": "X",
            "y": "Y",
            "z": None,
            "histfunc": "sum",
            "nbinsx": 2,
            "nbinsy": 2,
            "range_bins_x": None,
            "range_bins_y": None,
            "empty_bin_default": None,
            "table": self.source,
        }

        heatmap_preprocessor = HeatmapPreprocessor(args)

        new_table_gen = heatmap_preprocessor.preprocess_partitioned_tables(
            [self.source]
        )

        self.assertRaises(ValueError, lambda: next(new_table_gen))

    def test_empty_bin_default(self):
        args = {
            "x": "X",
            "y": "Y",
            "z": "Z",
            "histfunc": "sum",
            "nbinsx": 4,
            "nbinsy": 2,
            "range_bins_x": None,
            "range_bins_y": None,
            "empty_bin_default": 0,
            "table": self.source,
        }

        # sum, 0 - default to 0
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "sum": [1, 2, 0, 0, 0, 0, 1, 2],
            }
        )
        remap_types(expected_df)
        expected_df["sum"] = expected_df["sum"].astype("Int64")
        self.tables_equal(args, expected_df)

        args["empty_bin_default"] = "NaN"

        # sum, "NaN" - no default
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "sum": [1, 2, pd.NA, pd.NA, pd.NA, pd.NA, 1, 2],
            }
        )
        remap_types(expected_df)
        expected_df["sum"] = expected_df["sum"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["empty_bin_default"] = None

        # sum, None - no default
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "sum": [1, 2, pd.NA, pd.NA, pd.NA, pd.NA, 1, 2],
            }
        )
        remap_types(expected_df)
        expected_df["sum"] = expected_df["sum"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "var"

        # var, None - no default
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "var": [np.nan, np.nan, pd.NA, pd.NA, pd.NA, pd.NA, np.nan, np.nan],
            }
        )
        remap_types(expected_df)

        expected_df["var"] = expected_df["var"].astype("Float64")

        def na_conversion(df):
            # convert to pandas.NA for whole df comparison,
            # but verify that the nans are there because they come from there being no data in the bin
            arr = df["var"].to_numpy()
            for i in [0, 1, 6, 7]:
                self.assertTrue(np.isnan(arr[i]))
                df.at[i, "var"] = pd.NA

        self.tables_equal(args, expected_df, post_process=na_conversion)

        args["empty_bin_default"] = "NaN"

        # var, "NaN" - no default
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "var": [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
            }
        )
        remap_types(expected_df)
        expected_df["var"] = expected_df["var"].astype("Float64")

        self.tables_equal(args, expected_df, post_process=na_conversion)

        args["empty_bin_default"] = 3

        # var, 1 - default to 1 if at least two data points
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "var": [pd.NA, pd.NA, 3, 3, 3, 3, pd.NA, pd.NA],
            }
        )
        remap_types(expected_df)
        expected_df["var"] = expected_df["var"].astype("Float64")

        self.tables_equal(args, expected_df, post_process=na_conversion)

        args["histfunc"] = "count"

        # count, 3 - default to 3
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "count": [1, 1, 3, 3, 3, 3, 1, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count"] = expected_df["count"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["empty_bin_default"] = None

        # count, None - default to 0
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "count": [1, 1, 0, 0, 0, 0, 1, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count"] = expected_df["count"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["empty_bin_default"] = "NaN"

        # count, "NaN" - no default
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "count": [1, 1, pd.NA, pd.NA, pd.NA, pd.NA, 1, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count"] = expected_df["count"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["histfunc"] = "count_distinct"

        # count_distinct, "NaN" - no default
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "count_distinct": [1, 1, pd.NA, pd.NA, pd.NA, pd.NA, 1, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count_distinct"] = expected_df["count_distinct"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["empty_bin_default"] = None

        # count_distinct, None - default to 0
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "count_distinct": [1, 1, 0, 0, 0, 0, 1, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count_distinct"] = expected_df["count_distinct"].astype("Int64")

        self.tables_equal(args, expected_df)

        args["empty_bin_default"] = 2

        # count_distinct, 2 - default to 2
        expected_df = pd.DataFrame(
            {
                "X": [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5],
                "Y": [1.0, 3.0, 1.0, 3.0, 1.0, 3.0, 1.0, 3.0],
                "count_distinct": [1, 1, 2, 2, 2, 2, 1, 1],
            }
        )
        remap_types(expected_df)
        expected_df["count_distinct"] = expected_df["count_distinct"].astype("Int64")

        self.tables_equal(args, expected_df)


if __name__ == "__main__":
    unittest.main()
