# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "polars==1.35.2",
# ]
# ///

import marimo

__generated_with = "0.18.1"
app = marimo.App(width="full", app_title="CBI HBS Summary")


@app.cell
def _():
    import polars as pl

    import marimo as mo
    return mo, pl


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
        file_path = str(mo.notebook_location() / "Data/CSV_Files" / str(selected_row["Year"]) / file_name)
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
        "household_appliances_access"
    ]
    cleaned_table_name = mo.ui.dropdown(
        cleaned_tables_names,
        "household_appliances_access",
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
def _(cleaned_tables, mo, raw_tables):
    mo.ui.tabs({
        "Raw Tables": raw_tables,
        "Clean Tables": cleaned_tables,
    })
    return


if __name__ == "__main__":
    app.run()
