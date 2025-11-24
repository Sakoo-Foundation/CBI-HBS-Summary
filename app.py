# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "polars==1.35.2",
# ]
# ///

import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl

    import marimo as mo
    return mo, pl


@app.cell
def _(mo, pl):
    pl.read_csv(str(mo.notebook_location() / "Data/index.csv"))
    return


if __name__ == "__main__":
    app.run()
