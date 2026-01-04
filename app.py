# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "matplotlib==3.10.8",
#     "polars[pandas,pyarrow]==1.36.1",
#     "seaborn==0.13.2",
# ]
# ///

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full", app_title="CBI HBS Summary")


@app.cell
def _():
    import polars as pl
    import pandas as pd

    import marimo as mo
    return mo, pd, pl


@app.cell
def _(mo, pl):
    index = (
        pl.read_csv(str(mo.notebook_location() / "Data/index.csv"))
        .with_columns(
            pl.col("Table_Number").str.replace(r" (?<number>\d)$", r" ۰${number}")
        )
        .with_columns(
            pl.col("Available").replace_strict({1: "✅", 0: "❌"}, return_dtype=pl.String)
        )
    )
    return (index,)


@app.cell
def _(index, mo):
    years = index.get_column("Year").unique(maintain_order=False)
    year = mo.ui.dropdown(years, years[-1], label="Year", allow_select_none=True)
    return (year,)


@app.cell
def _(index, mo, pl, year):
    index_table = mo.ui.table(
        index.filter(
            pl.col("Year").eq(year.value) if year.value else True
        ),
        pagination=True,
        page_size=30,
        selection="single",
        show_data_types=False,
        show_column_summaries=False,
    )
    return (index_table,)


@app.cell
def _(index_table, mo, pl):
    def get_selected_table() -> pl.DataFrame | None:
        selected_row = index_table.value
        if len(selected_row) == 0:
            return
        selected_row = selected_row.row(0, named=True)
        if selected_row["Available"] != "✅":
            return
        file_name = selected_row["Table_Number"] + "-" + selected_row["Table_Name"]
        file_name = file_name.replace(" ", "_") + ".csv"
        file_path = str(mo.notebook_location()) + f"/Data/CSV_Files/{str(selected_row['Year'])}/{file_name}"
        return pl.read_csv(file_path)
    return (get_selected_table,)


@app.cell
def _(get_selected_table, mo):
    selected_table_df = get_selected_table()
    if selected_table_df is None:
        selected_table = None
    else:
        selected_table = mo.ui.dataframe(
            selected_table_df,
            page_size=20,
            # pagination=False,
            # show_data_types=False,
            # show_column_summaries=False,
        )
    return (selected_table,)


@app.cell
def _(index_table, mo, selected_table, year):
    raw_tables = mo.vstack(
        [
            year,
            mo.md("---"),
            mo.hstack(
                [
                    index_table,
                    selected_table,
                ],
                widths="equal",
            ),
        ],
    )
    return (raw_tables,)


@app.cell
def _(mo):
    cleaned_tables_names = [
        "annual_gross_expenditure_by_group",
        "household_size",
        "household_appliances_access",
    ]
    cleaned_table_name = mo.ui.dropdown(
        cleaned_tables_names,
        cleaned_tables_names[0],
        label="Table Name"
    )
    return (cleaned_table_name,)


@app.cell
def _(cleaned_table_name, mo, pl):
    cleaned_table_path = str(
        mo.notebook_location() /
        "Data/Cleaned_Tables" /
        f"{cleaned_table_name.value}.csv"
    )
    cleaned_table = pl.read_csv(cleaned_table_path)
    return (cleaned_table,)


@app.cell
def _(cleaned_table, cleaned_table_name, mo):
    cleaned_tables = mo.vstack(
        [
            cleaned_table_name,
            mo.md("---"),
            mo.ui.dataframe(cleaned_table, page_size=30),
        ]
    )
    return (cleaned_tables,)


@app.cell
def _(columns, mo):
    plot_table = mo.ui.dropdown(list(columns.keys()), "household_appliances_access", label="Table")
    return (plot_table,)


@app.cell
def _(columns, mo, plot_table):
    column = mo.ui.dropdown(columns[plot_table.value], columns[plot_table.value][0], label="Column")
    return (column,)


@app.cell
def _(mo):
    include_zero = mo.ui.checkbox(value=False, label="Include zero on Y-axis")
    return (include_zero,)


@app.cell
def _(mo):
    log_scale = mo.ui.checkbox(value=False, label="Log scale Y-axis")
    return (log_scale,)


@app.cell
def _(mo):
    def get_file_path(table: str, column: str) -> str:
        # comparison_tables_dir = Path("Data/Comparison_Tables")
        # file_path = comparison_tables_dir / f"{table}/{column}.csv"
        # file_path.parent.mkdir(exist_ok=True, parents=True)
        file_path = str(mo.notebook_location()) + f"/Data/Comparison_Tables/{table}/{column}.csv"
        return file_path
    return (get_file_path,)


@app.cell
def _():
    columns = {
        "annual_gross_expenditure_by_group": [
            "Gross_Expenditure_Total",
        ],
        "annual_gross_expenditure_by_group_normalized": [
            "Total",
            "Food_and_Beverages",
        ],
        "annual_gross_expenditure_by_group_normalized_share": [
            "Food_and_Beverages",
        ],
        "household_size": [
            "Household_Size_Average",
        ],
        "household_distribution_by_members": [
            "Household_Size_1",
            "Household_Size_2",
            "Household_Size_3",
            "Household_Size_4",
            "Household_Size_5",
            "Household_Size_6",
            "Household_Size_7",
            "Household_Size_8",
            "Household_Size_9",
            "Household_Size_10_plus",
        ],
        "employment_status_6_plus": [
            "Employed",
            "Unemployed",
            "Unemployed",
            "Income_without_Work",
            "Student",
            "Housekeeper",
            "Other",
        ],
        "household_distribution_by_number_of_employed": [
            "Employed_0",
            "Employed_1",
            "Employed_2",
            "Employed_3_plus",
        ],
        "housing_rooms": [
            "Dwelling_Room_1",
            "Dwelling_Room_2",
            "Dwelling_Room_3",
            "Dwelling_Room_4",
            "Dwelling_Room_5",
            "Dwelling_Room_6_Plus",
        ],
        "household_appliances_access": [
            "Car",
            "Motorcycle",
            "Bicycle",
            "Sewing_Machine",
            # "Radio_and_Cassette",
            # "Radio",
            "TV",
            "VCR",
            "PC",
            "Refrigerator",
            "Freezer_and_Freezer_Refrigerator",
            "Refrigerator_Freezer_and_Freezer_Refrigerator",
            "Fan",
            "Oven",
            "Vacuum_Cleaner",
            "Washing_Machine",
            "Cellphone",
            # "Camera",
        ],
        "household_facilities_access": [
            "Pipe_Water",
            "Electricity",
            "Natural_Gas",
            "Sewerage",
            "Phone",
            "Kitchen",
            "Bathroom",
            "AC",
            "Central_Heating",
            # "Storage_Room",
            "Internet",
            "Social_Media_Access",
        ],
    }
    return (columns,)


@app.cell
def _():
    title_mapping = {
        ("household_appliances_access", "Car"): "Car Ownership",
        ("household_appliances_access", "Motorcycle"): "Motorcycle Ownership",
        ("household_appliances_access", "Bicycle"): "Bicycle Ownership",
        ("household_appliances_access", "Radio"): "Radio Ownership",
        ("household_appliances_access", "Refrigerator"): "Refrigerator Ownership",
        ("household_appliances_access", "Freezer_and_Freezer_Refrigerator"): "Freezer and Freezer-Refrigerator Ownership",
        ("household_appliances_access", "Refrigerator_Freezer_and_Freezer_Refrigerator"): "Refrigerator and Freezer Ownership",
        ("household_appliances_access", "Vacuum_Cleaner"): "Vacuum Cleaner Ownership",
        ("household_appliances_access", "Washing_Machine"): "Washing Machine Ownership",
        ("household_appliances_access", "Cellphone"): "Cellphone Ownership",
        ("household_facilities_access", "Pipe_Water"): "Pipe_Water Access",
        ("household_facilities_access", "Electricity"): "Electricity Access",
        ("household_facilities_access", "Natural_Gas"): "Natural Gas Access",
        ("household_facilities_access", "Phone"): "Phone Access",
        ("household_facilities_access", "Kitchen"): "Kitchen Access",
        ("household_facilities_access", "Bathroom"): "Bathroom Access",
        ("household_facilities_access", "Sewerage"): "Sewerage Access",
    }
    return (title_mapping,)


@app.cell
def _(
    column,
    get_file_path,
    include_zero,
    log_scale,
    pd,
    pl,
    plot_table,
    title_mapping,
):
    import matplotlib.pyplot as plt
    import seaborn as sns

    def plot_comparision(): 
        title = title_mapping.get((plot_table.value, column.value), column.value)

        plot_data = (
            pd.DataFrame(pl.read_csv(get_file_path(plot_table.value, column.value)).to_dict())
            .set_index("Year")
            .rename_axis("Source", axis="columns")
            .stack(future_stack=True)
            .dropna()
            .rename("Value")
            .reset_index()
            .assign(Source=lambda df: df["Source"].str.replace("_", " "))
            .astype({
                "Source": pd.CategoricalDtype(
                    ["SCI All", "SCI Urban", "SCI CBI Sample", "CBI"],
                    ordered=True,
                )
            })
        )

        if plot_table.value == "household_size":
            ylabel = "Average Household Size"
        elif plot_table.value in [
            "housing_rooms",
            "household_distribution_by_members",
            "household_distribution_by_number_of_employed",
            "household_appliances_access",
            "household_facilities_access",
        ]:
            ylabel = "Percentage of Households (%)"
        elif "6_plus" in plot_table.value:
            ylabel = "Percentage of Individuals (%)"
        elif "share" in plot_table.value:
            ylabel = "Percentage of Expenditures (%)"
        else:
            ylabel = "Thousand Tomans"
            plot_data["Value"] = plot_data["Value"].div(1e4)

        sns.set_theme()
        fig, ax = plt.subplots(figsize=(10, 5))

        ax = sns.lineplot(
            plot_data,
            x="Year",
            y="Value",
            hue="Source",
            palette=["#94B4C1", "#547792", "#213448", "#FA6868"],
            marker="o",
        )

        ax.set_title(f"Comparative Analysis of {title} (SCI vs. CBI)", fontsize=18)
        ax.set_xlabel("")
        ax.set_ylabel(ylabel)

        if include_zero.value:
            ax.set_ylim(0, ax.get_ylim()[-1])

        if log_scale.value:
            ax.set_yscale("log")
        else:
            ax.set_yscale("linear")
        return fig
    return (plot_comparision,)


@app.cell
def _(column, include_zero, log_scale, mo, plot_comparision, plot_table):
    try:
        fig = plot_comparision()
    except Exception as e:
        raise e

    comparision = mo.vstack(
        [
            mo.hstack([plot_table, column], justify="start"),
            mo.hstack([include_zero, log_scale], justify="start"),
            fig,
        ]
    )
    return (comparision,)


@app.cell
def _(cleaned_tables, comparision, mo, raw_tables):
    mo.ui.tabs({
        "Raw Tables": raw_tables,
        "Clean Tables": cleaned_tables,
        "Comparision": comparision,
    })
    return


if __name__ == "__main__":
    app.run()
