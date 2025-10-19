import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    from pathlib import Path

    import fastexcel
    import polars as pl
    return Path, fastexcel, pl


@app.cell
def _(Path):
    excel_dir = Path("Data/Excel_Files")
    csv_dir = Path("Data/CSV_Files")
    return csv_dir, excel_dir


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
def _(pl):
    INVISIBLE_CHARS = [
        chr(8203),
        chr(173),
        chr(8207),
        chr(8235),
        chr(8236),
        chr(8234),
        chr(65279)
    ]


    UNWANTED_SYMBOLS = [
        "\n",
        "\r",
        "\t",
        "…",
        "ـ",
        "_",
        "•",
        "\\*",
        "`",
        "\"",
        "\'",
        "«",
        "»",
        ".",
        ",",
        ";",
        ":",
    ]


    def sanitize_farsi_text(column: pl.Expr) -> pl.Expr:
        """
        Clean Farsi text by replacing Arabic characters, removing invisible and unwanted characters,
        normalizing spaces, and stripping leading/trailing spaces.
        """

        return (
            column
            .pipe(replace_arabic_characters)

            # Replace Zero Width Non-Joiner ('\u200c') with a space
            .str.replace_all(chr(8204), " ")

            # Remove other invisible and unwanted characters
            .str.replace_all(
                "[" + "".join(INVISIBLE_CHARS + UNWANTED_SYMBOLS) + "]", ""
            )

            # Normalize spaces: replace all multi-space occurrences with a single space
            .str.replace_all("\\s+", " ")

            .str.strip_chars()
        )


    def replace_arabic_characters(column: pl.Expr) -> pl.Expr:
        """
        Replace Arabic characters with their Farsi equivalents in the Series.
        """
        character_mapping = {
            chr(1610): chr(1740), # ي -> ی
            chr(1574): chr(1740), # ئ -> ی
            chr(1609): chr(1740), # ى -> ی
            chr(1571): chr(1575), # أ -> ا
            chr(1573): chr(1575), # إ -> ا
            chr(1572): chr(1608), # ؤ -> و
            chr(1603): chr(1705), # ك -> ک
            chr(1728): chr(1607), # ۀ -> ه
            chr(1577): chr(1607), # ة -> ه
        }
        return column.str.replace_many(character_mapping)
    return (sanitize_farsi_text,)


@app.cell
def _(excel_dir, extract_year_index, fastexcel, pl, sanitize_farsi_text):
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
            pl.col("Table_Name").pipe(sanitize_farsi_text),
        )
    )
    index
    return (index,)


@app.cell
def _(index, pl):
    available_tables = (
        index
        .filter(pl.col("Available").gt(0))
        .pivot(index="Year", on="Table_Name", values="Table_Number")
    )
    available_tables
    return (available_tables,)


@app.cell
def _(excel_dir, fastexcel):
    def get_year_file_reader(year: int) -> fastexcel.ExcelReader:
        file = excel_dir/f"{year}.xlsx"
        reader = fastexcel.read_excel(file)
        return reader
    return (get_year_file_reader,)


@app.cell
def _(available_tables):
    def get_table_number_dict(table_name: str):
        return dict(zip(
            available_tables["Year"],
            available_tables.get_column(table_name),
        ))
    return (get_table_number_dict,)


@app.cell
def _(get_table_number_dict):
    get_table_number_dict("درصد توزیع افراد شش ساله و بیشتر خانوارها به تفکیک جنسیت بر حسب میزان سواد")
    return


@app.cell
def _(get_year_file_reader):
    get_year_file_reader(1400).load_sheet(12, header_row=None)
    return


@app.cell
def _(
    csv_dir,
    get_table_number_dict,
    get_year_file_reader,
    pl,
    sanitize_farsi_text,
):
    def extract_table_across_years(table_name: str) -> None:
        table_number_dict = get_table_number_dict(table_name)
        for year, table_number in table_number_dict.items():
            if table_number is None:
                continue
            sheet = get_year_file_reader(year).load_sheet(table_number, header_row=None)
            df = pl.DataFrame(sheet).cast(pl.String).select(pl.all().pipe(sanitize_farsi_text))

            file_name = f"{table_number}-{table_name}".replace(" ", "_")
            path = csv_dir/f"{year}"/f"{file_name}.csv"
            path.parent.mkdir(exist_ok=True, parents=True)
            df.write_csv(path, include_header=False)
    return (extract_table_across_years,)


@app.cell
def _(available_tables, extract_table_across_years):
    for table_name in available_tables.columns[1:]:
        extract_table_across_years(table_name)
    return


if __name__ == "__main__":
    app.run()
