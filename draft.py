import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path
    import re

    import fastexcel
    import polars as pl

    import cbi_hbs_summary
    return Path, cbi_hbs_summary, fastexcel, pl, re


@app.cell
def _(Path):
    DATA_DIR = Path("Data")
    excel_dir = Path("Data/Excel_Files")
    csv_dir = Path("Data/CSV_Files")

    cleaned_dir = DATA_DIR / "Cleaned_Tables"
    cleaned_dir.mkdir(exist_ok=True)
    return DATA_DIR, cleaned_dir, csv_dir, excel_dir


@app.cell
def _(pl):
    def extract_year_index(reader) -> pl.DataFrame:
        index_sheet = reader.load_sheet("فهرست جداول")
        return (
            pl.DataFrame(index_sheet)
            .select(pl.first().str.split(":"))
            .select(
                pl.first().list.get(0).str.strip_chars().alias("Table_Number"),
                pl.first().list.get(1).str.strip_chars().alias("Table_Name"),
            )
            .select(
                "Table_Number", "Table_Name",
                pl.col("Table_Number").is_in(reader.sheet_names).cast(pl.Int8).alias("Available")
            )
        )
    return (extract_year_index,)


@app.cell
def _(DATA_DIR, cbi_hbs_summary, excel_dir, extract_year_index, fastexcel, pl):
    df_list = []
    for file in excel_dir.iterdir():
        reader = fastexcel.read_excel(file)
        df_list.append(
            extract_year_index(reader)
            .select(
                pl.lit(file.stem).cast(pl.UInt16).alias("Year"),
                pl.all(),
            )
        )
    index = (
        pl.concat(df_list)
        .with_columns(
            pl.col("Table_Name").pipe(cbi_hbs_summary.utils.sanitize_farsi_text),
        )
    )
    index.write_csv(DATA_DIR / "index.csv")
    return (index,)


@app.cell
def _(DATA_DIR, index, pl):
    available_tables = (
        index
        .filter(pl.col("Available").gt(0))
        .pivot(index="Year", on="Table_Name", values="Table_Number")
    )
    available_tables.write_csv(DATA_DIR / "available_tables.csv")
    return (available_tables,)


@app.cell
def _(excel_dir, fastexcel):
    def get_year_file_reader(year: int) -> fastexcel.ExcelReader:
        file = excel_dir / f"{year}.xlsx"
        reader = fastexcel.read_excel(file)
        return reader
    return (get_year_file_reader,)


@app.cell
def _(available_tables):
    def get_table_number_dict(table_name: str):
        return dict(zip(
            available_tables.get_column("Year"),
            available_tables.get_column(table_name),
        ))
    return (get_table_number_dict,)


@app.cell
def _(
    cbi_hbs_summary,
    csv_dir,
    get_table_number_dict,
    get_year_file_reader,
    pl,
    re,
):
    def extract_table_across_years(table_name: str) -> None:
        table_number_dict = get_table_number_dict(table_name)
        for year, table_number in table_number_dict.items():
            if table_number is None:
                continue
            sheet = get_year_file_reader(year).load_sheet(table_number, header_row=None)
            df = pl.DataFrame(sheet).cast(pl.String).select(pl.all().pipe(cbi_hbs_summary.utils.sanitize_farsi_text))
            table_number = re.sub(r" (?P<number>\d)$", r" ۰\g<number>", table_number)
            file_name = f"{table_number}-{table_name}".replace(" ", "_")
            path = csv_dir/f"{year}"/f"{file_name}.csv"
            path.parent.mkdir(exist_ok=True, parents=True)
            df.write_csv(path, include_header=False)
    return (extract_table_across_years,)


@app.cell
def _(available_tables, extract_table_across_years):
    def extract_raw_tables():
        for table_name in available_tables.columns[1:]:
            extract_table_across_years(table_name)

    extract_raw_tables()
    return


@app.cell
def _(available_tables, cbi_hbs_summary, get_year_file_reader, pl):
    standard_tables = available_tables.rename(cbi_hbs_summary.metadata.load("table_names"))

    def get_table_numbers(table_name) -> dict:
        return dict(zip(
            standard_tables.get_column("Year"),
            standard_tables.get_column(table_name),
        ))

    def extract_standard_tables(name: str):
        rename_dict = {k.replace(" ", ""): v for k, v in cbi_hbs_summary.metadata.load("column_names")[name].items()}
        table_numbers = get_table_numbers(name)

        df_list = []
        for year, table_number in table_numbers.items():
            if table_number is None:
                continue
            sheet = get_year_file_reader(year).load_sheet(table_number, header_row=None)
            df = (
                pl.DataFrame(sheet).cast(pl.String)
                .select(
                    pl.all()
                    .pipe(cbi_hbs_summary.utils.sanitize_farsi_text)
                    .str.replace_all(r"[\s\(\)،]", "")
                )
            )
            columns = list(map(lambda x: rename_dict[x], df.row(0)))
            df.columns = columns
            df = (
                df[1:]
                .with_columns(pl.all().replace("", None))
                .select(
                    pl.lit(year).cast(pl.Int16).alias("Report_Year"),
                    pl.col("Year").cast(pl.Int16),
                    pl.all().exclude("Year").cast(pl.Float64)
                )
            )
            df_list.append(df)
        result = pl.concat(df_list, how="diagonal")
        return result
    return (extract_standard_tables,)


@app.cell
def _(cleaned_dir, extract_standard_tables):
    table_name = "household_appliances_access"
    extract_standard_tables(name=table_name).write_csv(cleaned_dir / f"{table_name}.csv")
    return


if __name__ == "__main__":
    app.run()
