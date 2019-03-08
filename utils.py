import pandas as pd
import json
from pandas.io.json import json_normalize


def load_df(file_name):
    """Return a DataFrame of a json file from the directory data.

    Arguments:
        file_name {str} -- json file name without extension from the directory data
    """
    with open("data/{}.json".format(file_name)) as f:
        data = json.load(f)
    return json_normalize(data[file_name])


def merge(
    hierarchy_df,
    hierarchy_df_on,
    values_df,
    values_df_on,
    hierarchy_suffix="_x",
    values_suffix="_y",
    transformers={},
    drop_cols=[],
    rename_cols={},
):
    """
    Link a values table to its corresponding hierarchy table.

    Arguments:
        hierarchy_df {DataFrame} -- DataFrame containing the tree structure of the entity
        hierarchy_df_on {str} -- The column on which the hierarchy DataFrame will occurr
        values_df {DataFrame} -- DataFrame containing the values for entries in the hierarchy DataFrame
        values_df_on {str} -- The column on which the values DataFrame will occurr
        transformers {dict} -- A mapping of column names and a list functions to be applied to said column after the merger occurs
        drop_cols {list} -- A list of column names to be dropped after the merger occurs
        rename_cols {dict} -- A mapping of existing column names and their replacement values, renaming
                              occurs after the merger occurs but before transformers are applied
    """
    # TODO transformers by dtype
    suffixes = (hierarchy_suffix, values_suffix)
    merged = (
        pd.merge(
            left=hierarchy_df,
            left_on=hierarchy_df_on,
            right=values_df,
            right_on=values_df_on,
            suffixes=suffixes,
        )
        .rename(columns=rename_cols)
        .drop(columns=drop_cols)
    )
    for col_name, transformer_list in transformers.items():
        for transformer in transformer_list:
            merged.loc[:, col_name] = merged.loc[:, col_name].apply(transformer)
    return merged
