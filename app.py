# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "polars==1.35.2",
# ]
# ///

import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium", layout_file="layouts/app.grid.json")


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
def _(year):
    year
    return


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
def _(index_table):
    index_table
    return


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
    mo.ui.table(
        get_selected_table(),
        pagination=False,
        show_data_types=False,
        show_column_summaries=False,
    )
    return


if __name__ == "__main__":
    app.run()
