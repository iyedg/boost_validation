import pandas as pd
import json
from pathlib import Path


def generate_csv():
    """Create a csv file for each worksheet in saisie.xlsx.
    """
    saisie = pd.read_excel("data/saisie.xlsx", sheet_name=None)
    for sheet_name in saisie:
        try:
            saisie[sheet_name].dropna(how="all").to_csv(
                "./data/generated/{}.csv".format(sheet_name), index=False
            )
        except Exception as e:
            print(sheet_name)
            print(e)
            pass


def load_sheet(sheet_name):
    """Return a DataFrame from saisie.xlsx for the corresponding sheet name.

    This function checks if a csv file with the corresponding sheet name is already generated,
    if such a file exists return it as a DataFrame, else generate all csv files from saisie.xlsx
    then return the DataFrame.

    Arguments:
        sheet_name {string} -- Name of worksheet to be loaded from saisie.xlsx
    """
    csv_sheet_path = Path("data/generated/{}.csv".format(sheet_name))
    if csv_sheet_path.exists():
        return pd.read_csv(str(csv_sheet_path))
    else:
        generate_csv()
        return pd.read_csv(str(csv_sheet_path))


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


def top_level_budget(df, year, top_level_budget_string="ميزانية الوزارة"):
    """Return a list of organizations that to do not contain a top level budget type, if any.

    Arguments:
        year {int} -- validation is limited to the span of one year
    """
    year_df = df.pipe(lambda df: df.loc[df.year == year])
    unique_organizations = set(year_df.organization_name.unique())
    organization_w_top_level_budget = set(
        year_df.loc[
            year_df.budget_type_name == top_level_budget, "organization_name"
        ].unique()
    )
    if len(unique_organizations) >= len(organization_w_top_level_budget):
        return unique_organizations.difference(organization_w_top_level_budget)
    else:
        # !TODO: this is probably unnecessary but am too tired to think
        return organization_w_top_level_budget.difference(unique_organizations)
